"""Lifecycle of the Ruby sidecar that embeds the ``solid-redis`` gem."""

import atexit
import os
import shutil
import stat
import subprocess
import tempfile
import threading
from pathlib import Path

from .errors import GatewayError

GATEWAY_SCRIPT = Path(__file__).with_name("solid_redis_gateway.rb")


class Sidecar:
    def __init__(self, redis_url, ruby_command=("ruby",), startup_timeout=15.0):
        self.redis_url = redis_url
        self.ruby_command = tuple(ruby_command)
        self.startup_timeout = startup_timeout
        self.directory = None
        self.socket_path = None
        self.process = None

    def start(self):
        # mkdtemp creates the directory 0700, the gateway binds the socket 0600.
        self.directory = tempfile.mkdtemp(prefix="solid_jobs_")
        self.socket_path = os.path.join(self.directory, "ipc.sock")
        try:
            self.process = subprocess.Popen(
                [*self.ruby_command, str(GATEWAY_SCRIPT), "--socket", self.socket_path,
                 "--redis-url", self.redis_url],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
        except OSError as error:
            self._cleanup()
            raise GatewayError(f"cannot start Ruby sidecar ({' '.join(self.ruby_command)}): {error}") from error
        ready = {}
        reader = threading.Thread(target=lambda: ready.setdefault("line", self.process.stdout.readline()),
                                  daemon=True)
        reader.start()
        reader.join(self.startup_timeout)
        if ready.get("line", "").strip() != "READY":
            stderr = self._terminate()
            raise GatewayError(f"Ruby sidecar failed to start: {stderr.strip() or 'timeout'}")
        mode = stat.S_IMODE(os.stat(self.socket_path).st_mode)
        if mode & 0o077:
            self.stop()
            raise GatewayError(f"gateway socket permissions too open: {oct(mode)}")
        return self

    def alive(self):
        return self.process is not None and self.process.poll() is None

    def stop(self):
        self._terminate()

    def _terminate(self):
        stderr = ""
        process, self.process = self.process, None
        if process is not None:
            try:
                process.stdin.close()
                process.wait(timeout=3)
            except (OSError, subprocess.TimeoutExpired):
                process.kill()
                process.wait()
            try:
                stderr = process.stderr.read()
            except (OSError, ValueError):
                pass
            for stream in (process.stdout, process.stderr):
                try:
                    stream.close()
                except OSError:
                    pass
        self._cleanup()
        return stderr

    def _cleanup(self):
        if self.directory:
            shutil.rmtree(self.directory, ignore_errors=True)
            self.directory = None


_managed = {}
_managed_lock = threading.Lock()


def ensure(redis_url, ruby_command=("ruby",)):
    """Return the socket path of a process-wide sidecar for ``redis_url``."""
    key = (redis_url, tuple(ruby_command))
    with _managed_lock:
        sidecar = _managed.get(key)
        if sidecar is None or not sidecar.alive():
            if sidecar is not None:
                sidecar.stop()
            sidecar = _managed[key] = Sidecar(redis_url, ruby_command).start()
        return sidecar.socket_path


def shutdown():
    with _managed_lock:
        sidecars = list(_managed.values())
        _managed.clear()
    for sidecar in sidecars:
        sidecar.stop()


atexit.register(shutdown)
