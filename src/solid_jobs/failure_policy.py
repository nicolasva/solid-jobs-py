import random
import time

from . import keyspace
from .instrumentation import emit
from .utilities import constantize, dump_json, realtime_milliseconds


class FailurePolicy:
    """Decides retry / drop / archive for a failed envelope. Returns the next attempt time or None."""

    def __init__(self, config, payload, error, telemetry_attributes=None):
        self.config = config
        self.payload = payload
        self.error = error
        self.telemetry = telemetry_attributes or {}

    @classmethod
    def call(cls, **kwargs):
        return cls(**kwargs).run()

    def run(self):
        payload = dict(self.payload)
        now = time.time()
        now_ms = realtime_milliseconds()
        failure_count = int(payload.get("failure_count", 0)) + 1
        payload["exception_message"] = str(self.error)
        payload["exception_type"] = type(self.error).__qualname__
        payload.setdefault("first_failed_ms", now_ms)
        payload["last_failed_ms"] = now_ms
        payload["failure_count"] = failure_count
        if payload.get("retry_channel"):
            payload["channel"] = payload["retry_channel"]

        if self._retry_allowed(payload, failure_count, now):
            decision = self._retry_decision(payload, failure_count)
            if decision == "drop":
                self._emit_dead(payload)
                return None
            if decision == "archive":
                return self._archive(payload, now)
            next_attempt = now + (decision if decision is not None else self._retry_delay(failure_count))
            self.config.redis_pool.call("ZADD", keyspace.RETRIES, next_attempt, dump_json(payload))
            emit(self.config, "retry_scheduled", payload, **self.telemetry,
                 error_class=type(self.error).__qualname__, error_message=str(self.error))
            self.config.logger.info("retry scheduled for %s", payload.get("id"))
            return next_attempt

        self._invoke_final_failure(payload)
        self._archive(payload, now)
        self.config.logger.info("task discarded: %s", payload.get("id"))
        return None

    def _retry_allowed(self, payload, failure_count, now):
        if payload.get("retry_within") is not None:
            first_failure = float(payload["first_failed_ms"]) / 1_000
            return now - first_failure < float(payload["retry_within"])
        return failure_count <= int(payload.get("max_failures", 0))

    def _retry_delay(self, failure_count):
        ceiling = min(float(self.config.retry_base_delay) * (2 ** (failure_count - 1)),
                      float(self.config.retry_max_delay))
        return ceiling / 2 + random.random() * ceiling / 2

    def _retry_decision(self, payload, failure_count):
        task = self._resolve_task(payload)
        if task is None:
            return None
        value = task.retry_delay(failure_count, self.error, payload)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value
        return value if value in ("drop", "archive") else None

    def _invoke_final_failure(self, payload):
        task = self._resolve_task(payload)
        if task is not None:
            task.after_final_failure(payload, self.error)

    @staticmethod
    def _resolve_task(payload):
        try:
            return constantize(payload["task"])
        except (NameError, ImportError, AttributeError, KeyError):
            return None

    def _archive(self, payload, timestamp):
        with self.config.redis_pool.pipelined() as pipeline:
            pipeline.call("ZADD", keyspace.DISCARDED, timestamp, dump_json(payload))
            pipeline.call("ZREMRANGEBYSCORE", keyspace.DISCARDED, "-inf",
                          timestamp - self.config.discarded_retention)
            pipeline.call("ZREMRANGEBYRANK", keyspace.DISCARDED, 0, -(self.config.discarded_limit + 1))
        self._emit_dead(payload)
        return pipeline.results

    def _emit_dead(self, payload):
        emit(self.config, "dead", payload, **self.telemetry,
             error_class=type(self.error).__qualname__, error_message=str(self.error))

