import json
import queue
import threading
import time

from .errors import ExecutionHalt, RedisConnectionError
from .claim import Claim
from .executor import Executor
from .failure_policy import FailurePolicy
from .instrumentation import emit, worker_attributes


class Engine:
    """One executor loop: claim, execute, acknowledge. Runs in a worker (sub)interpreter thread."""

    def __init__(self, config, identity=None, processor_id=None, heartbeat=None):
        self.config = config
        self.claims = Claim(config, identity=identity, processor_id=processor_id)
        self.executor = Executor(config=config)
        self.identity = identity
        self.processor_id = processor_id
        self.heartbeat = heartbeat
        self._state = threading.Lock()
        self._busy = False
        self._runner = None
        self._quiet = False
        self._stopping = False

    def run(self, control):
        with self._state:
            self._runner = threading.get_ident()
        processed = failed = 0
        try:
            while True:
                self._receive_commands(control)
                if self._stopping:
                    break
                if self._quiet:
                    self._flush_ack()
                    time.sleep(0.05)
                    continue
                claim = self._retrieve()
                if claim is None:
                    continue
                self._receive_commands(control)
                if self._stopping or self._quiet:
                    self._requeue(claim)
                    if self._stopping:
                        break
                    continue
                outcome = self._process(claim)
                if outcome is None:
                    break
                processed += outcome[0]
                failed += outcome[1]
        finally:
            self._flush_ack()
            self._send("stats", processed, failed)
            self.config.close()
        return {"processed": processed, "failed": failed}

    def busy(self):
        with self._state:
            return self._busy

    def interrupt_current(self):
        """Raise ExecutionHalt inside the running task thread (same interpreter only)."""
        import ctypes

        with self._state:
            runner = self._runner if self._busy else None
        if runner is not None:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(runner), ctypes.py_object(ExecutionHalt))

    def _process(self, claim):
        """Returns (processed, failed) deltas, or None when the engine must halt."""
        try:
            with self._state:
                self._busy = True
            envelope = claim.envelope
            self._register_work(claim, envelope)
            attributes = self._telemetry_attributes(claim)
            emit(self.config, "started", envelope, **attributes)
            started = time.monotonic()
            try:
                self.executor.execute(envelope)
            except ExecutionHalt:
                raise
            except Exception as error:
                duration = time.monotonic() - started
                emit(self.config, "failed", envelope, **attributes, duration=duration,
                     error_class=type(error).__qualname__, error_message=str(error))
                FailurePolicy.call(config=self.config, payload=envelope, error=error,
                                   telemetry_attributes=attributes)
                self._complete(claim)
                return (0, 1)
            duration = time.monotonic() - started
            emit(self.config, "completed", envelope, **attributes, duration=duration)
            return (1 if self._complete(claim) else 0, 0)
        except ExecutionHalt:
            self._requeue(claim)
            return None
        finally:
            with self._state:
                self._busy = False
            self._clear_work()

    def _complete(self, claim):
        try:
            self.claims.defer_ack(claim, lambda acknowledged: self._acknowledged(claim, acknowledged))
        except RedisConnectionError as error:
            self._completion_failed(error)
            return False
        return True

    def _flush_ack(self):
        try:
            self.claims.flush()
        except RedisConnectionError as error:
            self._completion_failed(error)

    def _completion_failed(self, error):
        self.claims.connection_failed()
        self.config.logger.warning(
            "Redis completion failed; claimed task will be replayed: %s: %s", type(error).__name__, error)

    def _acknowledged(self, claim, acknowledged):
        if not acknowledged:
            self.config.logger.warning("Claim fencing rejected stale completion: %s", claim.claim_token)
            return
        emit(self.config, "acknowledged", claim.envelope, **self._telemetry_attributes(claim))

    def _requeue(self, claim):
        if not claim.requeue():
            self.config.logger.warning("Claim fencing rejected stale requeue: %s", claim.claim_token)

    def _retrieve(self):
        try:
            return self.claims.next()
        except RedisConnectionError as error:
            self.claims.connection_failed()
            self.config.logger.warning("Redis claim failed, retrying: %s: %s", type(error).__name__, error)
            time.sleep(0.1)
            return None

    def _register_work(self, claim, envelope):
        if self.identity is None:
            return
        work = json.dumps({
            "job_id": envelope.get("id"), "channel": claim.channel, "envelope": envelope,
            "started_at": time.time(), "claim_token": claim.claim_token, "attempt": claim.attempt,
        })
        self._send("work", self.processor_id, work, envelope.get("id"))

    def _clear_work(self):
        if self.identity is not None:
            self._send("done", self.processor_id)

    def _send(self, *message):
        if self.heartbeat is not None:
            try:
                self.heartbeat.put(message)
            except Exception as error:  # a dead heartbeat must never kill the executor
                self.config.logger.debug("heartbeat message dropped: %s", error)

    def _receive_commands(self, control):
        while True:
            try:
                command = control.get_nowait()
            except queue.Empty:
                return
            if command == "quiet":
                self._quiet = True
            elif command == "stop":
                self._stopping = True

    def _telemetry_attributes(self, claim):
        return {"reservation_id": claim.claim_token, "attempt": claim.attempt,
                **worker_attributes(self.processor_id)}
