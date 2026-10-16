import secrets
import uuid
from collections import namedtuple

from . import keyspace, lab, publication_barrier, settings
from .errors import InvalidArgumentError, InvalidJobError, RedisError
from .instrumentation import emit
from .utilities import class_path, dump_json, realtime_milliseconds, stringify_keys

DEFAULT_CHUNK_SIZE = 1_000
PLANNED_CHUNK_SIZE = 100
Publication = namedtuple("Publication", "task envelope redis_pool")


def _chunks(items, size):
    items = list(items)
    for start in range(0, len(items), size):
        yield start, items[start:start + size]


class Publisher:
    def __init__(self, config=None):
        self.config = config or settings.config()

    def publish(self, envelope):
        normalized = self._normalize(envelope)
        normalized = self._intercept(normalized)
        if not normalized:
            return None
        self._validate_arguments(normalized["arguments"])
        barriers = self.persist([normalized])
        self._emit_persisted([normalized], barriers)
        return normalized["id"]

    def publish_many(self, envelope):
        if not isinstance(envelope, dict):
            raise InvalidJobError("Task envelope must be a dict")
        source = stringify_keys(envelope)
        argument_sets = source.pop("arguments", None)
        if argument_sets is None or isinstance(argument_sets, (str, bytes, dict)):
            raise InvalidJobError("Task 'arguments' must be a list or iterable of argument lists")
        argument_sets = list(argument_sets)
        run_times = source.pop("run_at", None)
        if not (run_times is None or isinstance(run_times, (int, float, list, tuple))):
            raise InvalidJobError("Task 'run_at' must be a number or a list")
        if isinstance(run_times, (list, tuple)) and len(run_times) != len(argument_sets):
            raise InvalidJobError("Task 'run_at' list must match the number of argument sets")
        chunk_size = int(source.pop("chunk_size", None) or
                         (PLANNED_CHUNK_SIZE if run_times is not None else DEFAULT_CHUNK_SIZE))
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        explicit_id = source.pop("id", None)
        if explicit_id and len(argument_sets) > 1:
            raise InvalidJobError("An explicit id is only valid for a single task")

        template = self._normalize({**source, "arguments": []})
        template.pop("id", None)
        ids = []
        for offset, chunk in _chunks(argument_sets, chunk_size):
            envelopes = []
            for index, arguments in enumerate(chunk):
                if not isinstance(arguments, (list, tuple)):
                    raise InvalidJobError("Each task's arguments must be a list")
                item = {**template, "arguments": list(arguments), "id": explicit_id or str(uuid.uuid4())}
                if run_times is not None:
                    item["run_at"] = run_times[offset + index] if isinstance(run_times, (list, tuple)) \
                        else run_times
                intercepted = self._intercept(item)
                if intercepted:
                    self._validate_arguments(intercepted["arguments"])
                    envelopes.append(intercepted)
                    ids.append(intercepted["id"])
                else:
                    ids.append(None)
            barriers = self.persist(envelopes)
            self._emit_persisted(envelopes, barriers)
        return ids

    def persist(self, envelopes):
        if not envelopes:
            return []
        current = lab.mode()
        if current == "capture":
            for envelope in envelopes:
                lab.captured_for(envelope["task"]).append(envelope)
            return []
        if current == "execute":
            for envelope in envelopes:
                lab.execute(envelope)
            return []

        planned = [e for e in envelopes if e.get("run_at") is not None]
        ready = [e for e in envelopes if e.get("run_at") is None]
        barriers = []
        with self.config.redis_pool.pipelined() as pipeline:
            for envelope in planned:
                score = envelope.pop("run_at")
                pipeline.call("ZADD", keyspace.PLANNED, score, dump_json(envelope))
            barriers = self._persist_ready(pipeline, ready)
        return barriers

    def _emit_persisted(self, envelopes, barriers):
        for envelope in envelopes:
            emit(self.config, "enqueued", envelope)
            emit(self.config, "journaled", envelope)
        self._release_publications(barriers)

    def _normalize(self, envelope):
        if not isinstance(envelope, dict):
            raise InvalidJobError("Task envelope must be a dict")
        item = {**self.config.default_task_options, **stringify_keys(envelope)}
        task, arguments = item.get("task"), item.get("arguments")
        if not (isinstance(task, type) or (isinstance(task, str) and task)):
            raise InvalidJobError("Task envelope requires a non-empty task")
        if not isinstance(arguments, (list, tuple)):
            raise InvalidJobError("Task arguments must be a list")
        item["arguments"] = list(arguments)
        if isinstance(task, type):
            item["task"] = class_path(task)
        item["channel"] = str(item["channel"])
        item.setdefault("id", None)
        item["id"] = item["id"] or str(uuid.uuid4())
        item.setdefault("created_ms", realtime_milliseconds())
        return item

    def _intercept(self, envelope):
        context = Publication(envelope["task"], envelope, self.config.redis_pool)
        return self.config.publish_interceptors.call(context, lambda: envelope)

    def _persist_ready(self, pipeline, envelopes):
        barriers = []
        by_channel = {}
        for envelope in envelopes:
            by_channel.setdefault(envelope["channel"], []).append(envelope)
        for channel, group in by_channel.items():
            pipeline.call("SADD", keyspace.CHANNELS, channel)
            queued_ms = realtime_milliseconds()
            encoded = []
            for envelope in group:
                envelope["queued_ms"] = queued_ms
                envelope[publication_barrier.FIELD] = secrets.token_hex(16)
                encoded.append(dump_json(envelope))
                barrier = publication_barrier.prepare(publication=envelope[publication_barrier.FIELD])
                publication_barrier.mark(pipeline, barrier)
                barriers.append(barrier)
            pipeline.call("LPUSH", keyspace.channel(channel), *encoded)
        return barriers

    def _release_publications(self, barriers):
        try:
            publication_barrier.release(self.config.redis_pool, barriers)
        except RedisError as error:
            self.config.logger.warning(
                "SolidJobs publication barrier release failed; waiting for expiry: %s: %s",
                type(error).__name__, error)

    def _validate_arguments(self, arguments):
        invalid = self._invalid_json_value(arguments)
        if invalid is _VALID:
            return
        message = ("Task arguments must contain JSON-native values with string dict keys; "
                   f"found {invalid!r}")
        policy = self.config.on_complex_arguments
        if policy == "raise":
            raise InvalidArgumentError(message)
        if policy == "warn":
            self.config.logger.warning(message)

    def _invalid_json_value(self, value):
        if value is None or isinstance(value, (bool, str, int, float)):
            return _VALID
        if isinstance(value, (list, tuple)):
            for nested in value:
                invalid = self._invalid_json_value(nested)
                if invalid is not _VALID:
                    return invalid
            return _VALID
        if isinstance(value, dict):
            for key, nested in value.items():
                if not isinstance(key, str):
                    return key
                invalid = self._invalid_json_value(nested)
                if invalid is not _VALID:
                    return invalid
            return _VALID
        return value


_VALID = object()
