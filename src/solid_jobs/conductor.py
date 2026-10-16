import json
import queue
import sys
import threading
import time
from concurrent import interpreters

from . import settings
from .blueprint import Blueprint
from .errors import Error, RedisError
from .heartbeat import INTERVAL, Heartbeat
from .recovery import call as recover
from .timer import Timer
from . import keyspace, worker

STOP_GRACE = 5.0
STOP_RESENDS = 3
STARTUP_TIMEOUT = 30.0


class Conductor:
    """Runs N executor subinterpreters + a scheduler and heartbeat thread for one node.

    Redis traffic of every component goes through the Ruby solid-redis sidecar
    over a 0600 Unix socket; the workers hold no Redis client of their own.
    """

    def __init__(self, config=None, stop_grace=STOP_GRACE):
        self.config = config or settings.config()
        self.stop_grace = stop_grace
        self.identity = self.config.identity
        self.started_at = time.time()
        self._reset()

    def _reset(self):
        self._started = False
        self._workers = []
        self._threads = []
        self._controls = []
        self._heartbeat_queue = None
        self._stop_background = threading.Event()
        self._quiet = threading.Event()
        self._stop_heartbeat = threading.Event()
        self._worker_failures = {}

    def running(self):
        return self._started

    def start(self):
        if self._started:
            raise Error("Conductor is already running")
        config = self.config
        snapshot = config.snapshot()
        paths = json.dumps([path for path in sys.path if path])
        try:
            recover(config)
            self._heartbeat_queue = interpreters.create_queue()
            ready = interpreters.create_queue()
            self._start_heartbeat(snapshot)
            for processor_id in range(config.concurrency):
                self._start_worker(snapshot, paths, processor_id, ready)
            self._start_scheduler(snapshot)
            self._await_ready(ready, config.concurrency)
        except BaseException:
            self._abort()
            raise
        self._started = True
        config.fire("startup", self)
        return self

    def quiet(self):
        if self._started:
            for control in self._controls:
                control.put("quiet")
            self._quiet.set()
            self.config.fire("quiet", self)
        return self

    def stop(self):
        if not self._started:
            return []
        for control in self._controls:
            control.put("stop")
        self._stop_background.set()
        deadline = self.config.shutdown_timeout + self.stop_grace
        results = [self._await_worker(index, deadline) for index in range(len(self._workers))]
        self._stop_heartbeat.set()
        for thread in self._threads:
            thread.join(deadline)
        self._teardown()
        self.config.fire("shutdown", self)
        return results

    def remote_signal(self):
        return self.config.redis_pool.call("RPOP", keyspace.node_signals(self.identity))

    def _start_worker(self, snapshot, paths, processor_id, ready):
        interpreter = interpreters.create()
        control = interpreters.create_queue()
        interpreter.prepare_main(snapshot=snapshot, processor_id=processor_id, control=control,
                                 heartbeat=self._heartbeat_queue, ready=ready, paths=paths)
        outcome = {}

        def run():
            try:
                interpreter.exec(worker.WORKER_CODE)
                outcome["status"] = "stopped"
            except BaseException as error:  # ExecutionFailed carries the worker traceback
                outcome["status"] = "crashed"
                outcome["error"] = error
                self.config.logger.error("SolidJobs worker %s crashed: %s", processor_id, error)

        thread = threading.Thread(target=run, name=f"solid-jobs-worker-{processor_id}", daemon=True)
        thread.start()
        self._workers.append((interpreter, thread, outcome))
        self._controls.append(control)

    def _start_heartbeat(self, snapshot):
        local = Blueprint.from_snapshot(snapshot)
        heartbeat = Heartbeat(local, self.identity, self.started_at, self.config.concurrency)
        # Publish liveness before workers/scheduler start, so recovery never treats this node as dead.
        heartbeat.beat(self._quiet.is_set())

        def loop():
            work, processed, failed, next_beat = {}, 0, 0, 0.0
            try:
                while not self._stop_heartbeat.is_set():
                    try:
                        message = self._heartbeat_queue.get_nowait()
                    except queue.Empty:
                        message = None
                        self._stop_heartbeat.wait(0.05)
                    while message is not None:
                        event, *arguments = message
                        if event == "work":
                            work[str(arguments[0])] = arguments[1]
                        elif event == "done":
                            work.pop(str(arguments[0]), None)
                        elif event == "stats":
                            processed += arguments[0]
                            failed += arguments[1]
                        try:
                            message = self._heartbeat_queue.get_nowait()
                        except queue.Empty:
                            message = None
                    if time.monotonic() >= next_beat:
                        try:
                            heartbeat.beat(self._quiet.is_set(), work, processed, failed)
                            processed = failed = 0
                        except RedisError as error:
                            local.logger.warning("Redis heartbeat failed, retrying: %s: %s",
                                                 type(error).__name__, error)
                        next_beat = time.monotonic() + INTERVAL
                try:
                    heartbeat.beat(self._quiet.is_set(), work, processed, failed)
                    heartbeat.cleanup()
                except RedisError as error:
                    local.logger.warning("Redis heartbeat cleanup failed: %s: %s", type(error).__name__, error)
            finally:
                local.close()

        self._spawn(loop, "solid-jobs-heartbeat")

    def _start_scheduler(self, snapshot):
        timer = Timer(Blueprint.from_snapshot(snapshot))
        self._spawn(lambda: timer.run(stop=self._stop_background.is_set), "solid-jobs-scheduler")

    def _spawn(self, target, name):
        thread = threading.Thread(target=target, name=name, daemon=True)
        thread.start()
        self._threads.append(thread)

    def _await_ready(self, ready, expected):
        deadline = time.monotonic() + STARTUP_TIMEOUT
        seen = 0
        while seen < expected:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise Error(f"SolidJobs workers did not start within {STARTUP_TIMEOUT}s")
            try:
                status, processor_id, detail = ready.get_nowait()
            except queue.Empty:
                time.sleep(0.02)
                continue
            if status == "failed":
                raise Error(f"SolidJobs worker {processor_id} failed to boot: {detail}")
            seen += 1

    def _await_worker(self, index, deadline):
        interpreter, thread, outcome = self._workers[index]
        resends = 0
        end = time.monotonic() + deadline
        while thread.is_alive():
            thread.join(0.1)
            if thread.is_alive() and time.monotonic() >= end:
                if resends >= STOP_RESENDS:
                    self.config.logger.error("SolidJobs worker %s did not stop; abandoning it", index)
                    self._fence_abandoned()
                    return {"status": "abandoned"}
                resends += 1
                self.config.logger.warning(
                    "SolidJobs worker %s did not stop within %ss, re-sending stop (%s/%s)",
                    index, deadline, resends, STOP_RESENDS)
                self._controls[index].put("stop")
                end = time.monotonic() + deadline
        return {"status": outcome.get("status", "stopped")}

    def _fence_abandoned(self):
        """Requeue claims of workers we could not stop; their late ack is rejected by the claim token."""
        try:
            recover(self.config, dead_identities=(self.identity,))
        except RedisError as error:
            self.config.logger.warning("Could not requeue abandoned claims: %s", error)

    def _teardown(self):
        for interpreter, thread, _ in self._workers:
            if not thread.is_alive():
                try:
                    interpreter.close()
                except Exception:
                    pass
        self.config.close()
        self._reset()

    def _abort(self):
        self._stop_background.set()
        self._stop_heartbeat.set()
        for control in self._controls:
            try:
                control.put("stop")
            except Exception:
                pass
        for _, thread, _ in self._workers:
            thread.join(2)
        self._teardown()
