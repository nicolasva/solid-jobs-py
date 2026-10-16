"""Warm IPC Pool supervisor.

Keeps a fixed set of persistent Python processes ("warm workers") and a broker socket that engines use to
run jobs in them. The supervisor only executes: reservation, acknowledgement, retries and crash recovery
remain with the engine that owns the claim (see engine.py / claim.py), so a result received here is never
an ACK. One job per worker at a time.

    engine (subinterpreter) --broker socket--> supervisor --socketpair--> warm worker (own process)
"""

import logging
import os
import queue
import select
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from ..errors import WarmPoolError
from . import protocol

DEFAULTS = {
    "enabled": False,
    "workers": 2,
    "startup_timeout": 15.0,
    "job_timeout": 300.0,
    "shutdown_timeout": 30.0,
    "max_jobs_per_worker": 10_000,
    "max_rss_mb": 512,
    "restart_backoff": 5.0,
    "serializer": "json",
    "max_message_bytes": protocol.MAX_MESSAGE_BYTES,
    "allowed_modules": [],
}
_LOGGER = logging.getLogger("solid_jobs.warm")
_PACKAGE_ROOT = str(Path(__file__).resolve().parents[2])


def normalize(options):
    unknown = set(options) - set(DEFAULTS) - {"min_workers"}
    if unknown:
        raise ValueError(f"unknown warm_pool options: {', '.join(sorted(unknown))}")
    merged = {**DEFAULTS, **options}
    if "min_workers" in options:
        merged["workers"] = merged.pop("min_workers")
    merged.pop("min_workers", None)
    if int(merged["workers"]) < 1:
        raise ValueError("warm_pool workers must be at least 1")
    protocol.serializer(merged["serializer"])
    return merged


class _Process:
    def __init__(self, popen, sock, pid):
        self.popen = popen
        self.sock = sock
        self.pid = pid
        self.jobs = 0
        self.rss_mb = 0.0

    def alive(self):
        return self.popen.poll() is None

    def kill(self, grace=0.0):
        try:
            if grace and self.alive():
                self.popen.terminate()
                try:
                    self.popen.wait(grace)
                except subprocess.TimeoutExpired:
                    pass
            if self.alive():
                self.popen.kill()
            self.popen.wait(5)
        except (OSError, subprocess.TimeoutExpired):
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class WarmPool:
    def __init__(self, options):
        self.options = normalize(options)
        self.codec = protocol.serializer(self.options["serializer"])
        self.max_bytes = int(self.options["max_message_bytes"])
        self.socket_path = None
        self._directory = None
        self._listener = None
        self._idle = queue.Queue()
        self._lock = threading.Lock()
        self._inflight = 0
        self._drained = threading.Condition(self._lock)
        self._stopping = threading.Event()
        self._processes = set()
        self._crashes = 0
        self._restarts = 0
        self._jobs = 0
        self._init_message = None
        self._threads = []

    def start(self, snapshot, paths):
        self._init_message = self.codec.dumps({
            "v": protocol.PROTOCOL_VERSION, "type": "init", "snapshot": snapshot, "paths": paths,
            "allowed_modules": list(self.options["allowed_modules"])})
        self._directory = tempfile.mkdtemp(prefix="solid_jobs_warm_")
        os.chmod(self._directory, 0o700)
        self.socket_path = os.path.join(self._directory, "warm.sock")
        previous = os.umask(0o177)
        try:
            self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._listener.bind(self.socket_path)
        finally:
            os.umask(previous)
        os.chmod(self.socket_path, 0o600)
        self._listener.listen(64)
        try:
            for process in self._spawn_many(int(self.options["workers"])):
                self._idle.put(process)
        except BaseException:
            self.stop()
            raise
        self._thread(self._accept_loop, "solid-jobs-warm-accept")
        return self

    def stats(self):
        with self._lock:
            return {"workers": len(self._processes), "idle": self._idle.qsize(), "inflight": self._inflight,
                    "restarts": self._restarts, "jobs": self._jobs}

    def stop(self):
        self._stopping.set()
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass
        deadline = time.monotonic() + float(self.options["shutdown_timeout"])
        with self._drained:
            while self._inflight and time.monotonic() < deadline:
                self._drained.wait(0.1)
        with self._lock:
            processes = list(self._processes)
            self._processes.clear()
        for process in processes:
            try:
                protocol.send_frame(process.sock, self.codec.dumps(
                    {"v": protocol.PROTOCOL_VERSION, "type": "shutdown"}))
            except OSError:
                pass
        for process in processes:
            process.kill(grace=2.0)
        if self._directory:
            shutil.rmtree(self._directory, ignore_errors=True)
            self._directory = None

    def _thread(self, target, name, *args):
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        thread.start()
        self._threads.append(thread)

    def _spawn_many(self, count):
        launched = [self._launch() for _ in range(count)]
        ready = []
        try:
            for process in launched:
                self._await_ready(process)
                ready.append(process)
        except BaseException:
            for process in launched:
                process.kill()
                with self._lock:
                    self._processes.discard(process)
            raise
        return ready

    def _launch(self):
        parent, child = socket.socketpair()
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(filter(None, [_PACKAGE_ROOT, env.get("PYTHONPATH")]))
        try:
            popen = subprocess.Popen(
                [sys.executable, "-m", "solid_jobs.warm.worker", "--fd", str(child.fileno()),
                 "--serializer", self.options["serializer"]],
                pass_fds=[child.fileno()], stdin=subprocess.DEVNULL, env=env)
        except OSError as error:
            parent.close()
            raise WarmPoolError(f"cannot start warm worker: {error}") from error
        finally:
            child.close()
        process = _Process(popen, parent, popen.pid)
        with self._lock:
            self._processes.add(process)
        try:
            protocol.send_frame(parent, self._init_message, self.max_bytes * 4)
        except OSError as error:
            process.kill()
            raise WarmPoolError(f"warm worker {popen.pid} died during startup: {error}") from error
        return process

    def _await_ready(self, process):
        timeout = float(self.options["startup_timeout"])
        process.sock.settimeout(timeout)
        try:
            payload = protocol.recv_frame(process.sock, self.max_bytes)
            message = protocol.decode(self.codec, payload) if payload else {}
        except (OSError, protocol.ProtocolError) as error:
            process.kill()
            raise WarmPoolError(f"warm worker {process.pid} did not become ready within {timeout}s: {error}") from error
        process.sock.settimeout(None)
        if message.get("type") != "ready":
            process.kill()
            raise WarmPoolError(f"warm worker {process.pid} failed to start: {message.get('error', 'unknown error')}")

    def _accept_loop(self):
        while not self._stopping.is_set():
            try:
                connection, _ = self._listener.accept()
            except OSError:
                return
            self._thread(self._serve, "solid-jobs-warm-client", connection)

    def _checkout(self, connection):
        while not self._stopping.is_set():
            try:
                return self._idle.get(timeout=0.2)
            except queue.Empty:
                if select.select([connection], [], [], 0)[0]:
                    return None
        return None

    def _serve(self, connection):
        try:
            while not self._stopping.is_set():
                try:
                    payload = protocol.recv_frame(connection, self.max_bytes)
                except protocol.ProtocolError as error:
                    self._reply(connection, {}, "ProtocolError", str(error), retryable=False)
                    return
                if payload is None:
                    return
                try:
                    request = protocol.decode(self.codec, payload)
                    if request.get("type") != "execute" or not request.get("request_id"):
                        raise protocol.ProtocolError("expected an execute request with a request_id")
                except protocol.ProtocolError as error:
                    self._reply(connection, {}, "ProtocolError", str(error), retryable=False)
                    continue
                if not self._run(connection, request, payload):
                    return
        except OSError:
            pass
        finally:
            connection.close()

    def _run(self, connection, request, payload):
        process = self._checkout(connection)
        if process is None:
            if self._stopping.is_set():
                self._reply(connection, request, "WarmPoolStopping", "warm pool is shutting down", True)
            return False
        with self._lock:
            self._inflight += 1
        try:
            return self._dispatch(connection, request, payload, process)
        finally:
            with self._drained:
                self._inflight -= 1
                self._drained.notify_all()

    def _dispatch(self, connection, request, payload, process):
        timeout = float(self.options["job_timeout"])
        if request.get("timeout_ms"):
            timeout = min(timeout, float(request["timeout_ms"]) / 1000)
        deadline = time.monotonic() + timeout
        try:
            protocol.send_frame(process.sock, payload, self.max_bytes)
        except OSError:
            self._retire(process, crashed=True)
            self._reply(connection, request, "WarmWorkerCrashed", "warm worker connection lost", True)
            return True
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._retire(process, crashed=False)
                self._reply(connection, request, "WarmTimeout",
                            f"job exceeded {timeout:g}s; worker {process.pid} was terminated and replaced", True)
                return True
            readable = select.select([process.sock, connection], [], [], remaining)[0]
            if connection in readable:
                # The engine went away (halt/shutdown): the job is abandoned, so the worker is not reusable.
                self._retire(process, crashed=False)
                return False
            if process.sock in readable:
                return self._finish(connection, request, process)

    def _finish(self, connection, request, process):
        try:
            raw = protocol.recv_frame(process.sock, self.max_bytes)
            result = protocol.decode(self.codec, raw) if raw is not None else None
        except (OSError, protocol.ProtocolError):
            result = None
        if result is None:
            self._retire(process, crashed=True)
            self._reply(connection, request, "WarmWorkerCrashed",
                        f"warm worker {process.pid} died while running the job", True)
            return True
        if result.get("request_id") != request.get("request_id"):
            self._retire(process, crashed=True)
            self._reply(connection, request, "ProtocolError", "mismatched request_id from warm worker", True)
            return True
        process.jobs += 1
        process.rss_mb = float(result.get("rss_mb") or 0)
        with self._lock:
            self._jobs += 1
            self._crashes = 0
        try:
            protocol.send_frame(connection, raw, self.max_bytes)
        finally:
            self._release(process)
        return True

    def _reply(self, connection, request, error_class, message, retryable):
        payload = self.codec.dumps({
            "v": protocol.PROTOCOL_VERSION, "type": "result", "request_id": request.get("request_id"),
            "job_id": request.get("job_id"), "status": "error",
            "error": {"class": error_class, "message": message, "retryable": retryable}})
        try:
            protocol.send_frame(connection, payload, self.max_bytes)
        except OSError:
            pass

    def _release(self, process):
        limit_jobs = int(self.options["max_jobs_per_worker"])
        limit_rss = float(self.options["max_rss_mb"])
        if (limit_jobs and process.jobs >= limit_jobs) or (limit_rss and process.rss_mb > limit_rss):
            _LOGGER.info("recycling warm worker %s (jobs=%s, rss=%.0fMiB)", process.pid, process.jobs, process.rss_mb)
            self._retire(process, crashed=False)
        elif process.alive():
            self._idle.put(process)
        else:
            self._retire(process, crashed=True)

    def _retire(self, process, crashed):
        process.kill()
        with self._lock:
            self._processes.discard(process)
            self._restarts += 1
            if crashed:
                self._crashes += 1
            delay = (min(0.1 * 2 ** (self._crashes - 1), float(self.options["restart_backoff"]))
                     if self._crashes else 0.0)
        if not self._stopping.is_set():
            self._thread(self._replace, "solid-jobs-warm-restart", delay)

    def _replace(self, delay):
        failures = 0
        while not self._stopping.is_set():
            if delay:
                self._stopping.wait(delay)
                if self._stopping.is_set():
                    return
            try:
                process = self._launch()
                self._await_ready(process)
            except WarmPoolError as error:
                failures += 1
                delay = min(0.1 * 2 ** failures, float(self.options["restart_backoff"]))
                _LOGGER.warning("warm worker restart failed (%s); retrying in %.1fs", error, delay)
                continue
            if self._stopping.is_set():
                process.kill()
                return
            self._idle.put(process)
            return

