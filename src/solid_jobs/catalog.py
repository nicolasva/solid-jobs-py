"""Read/inspect API over the SolidJobs Redis keyspace (counters, channels, nodes, claims...)."""

import json
from datetime import datetime

from . import keyspace, settings
from .publisher import Publisher


def _cfg(config):
    return config or settings.config()


def _scan(pool, pattern):
    cursor, found = "0", []
    while True:
        cursor, batch = pool.call("SCAN", cursor, "MATCH", pattern, "COUNT", 100)
        found.extend(batch)
        if cursor == "0":
            return found


def _time(seconds):
    return datetime.fromtimestamp(float(seconds)) if seconds is not None else None


def _pairs(flat):
    flat = flat or []
    return list(zip(flat[0::2], flat[1::2]))


class Counters:
    def __init__(self, config=None):
        self.config = _cfg(config)

    @property
    def _pool(self):
        return self.config.redis_pool

    def completed(self):
        return int(self._pool.call("GET", keyspace.PROCESSED) or 0)

    def failed(self):
        return int(self._pool.call("GET", keyspace.FAILED) or 0)

    def ready(self):
        return sum(channel.size() for channel in Channel.catalog(self.config))

    def planned(self):
        return int(self._pool.call("ZCARD", keyspace.PLANNED))

    def retrying(self):
        return int(self._pool.call("ZCARD", keyspace.RETRIES))

    def discarded(self):
        return int(self._pool.call("ZCARD", keyspace.DISCARDED))

    def nodes(self):
        return int(self._pool.call("SCARD", keyspace.NODES))

    def executions(self):
        return sum(node.active_count() for node in Nodes(self.config))

    def reset(self):
        self._pool.call("DEL", keyspace.PROCESSED, keyspace.FAILED)
        return self


class StoredTask:
    def __init__(self, raw, score=None, config=None, owner=None):
        self.raw = raw
        self.envelope = json.loads(raw)
        self.score = float(score) if score is not None else None
        self.config = _cfg(config)
        self.owner = owner

    id = property(lambda self: self.envelope.get("id"))
    channel = property(lambda self: self.envelope.get("channel"))
    task = property(lambda self: self.envelope.get("task"))
    arguments = property(lambda self: self.envelope.get("arguments"))
    created_at = property(lambda self: self._millis("created_ms"))
    queued_at = property(lambda self: self._millis("queued_ms"))
    due_at = property(lambda self: _time(self.score))

    def remove(self):
        if isinstance(self.owner, TimedTasks):
            return self.config.redis_pool.call("ZREM", self.owner.key, self.raw)
        if isinstance(self.owner, Channel):
            return self.config.redis_pool.call("LREM", self.owner.storage_key, 1, self.raw)
        return False

    def requeue(self):
        self.remove()
        return Publisher(config=self.config).publish(self.envelope)

    def _millis(self, field):
        value = self.envelope.get(field)
        return _time(float(value) / 1000) if value else None


class Channel:
    def __init__(self, name, config=None):
        self.name = str(name)
        self.config = _cfg(config)

    @classmethod
    def catalog(cls, config=None):
        config = _cfg(config)
        return [cls(name, config) for name in sorted(config.redis_pool.call("SMEMBERS", keyspace.CHANNELS) or [])]

    @property
    def storage_key(self):
        return keyspace.channel(self.name)

    def __iter__(self):
        for raw in reversed(self.config.redis_pool.call("LRANGE", self.storage_key, 0, -1) or []):
            yield StoredTask(raw, config=self.config, owner=self)

    def size(self):
        return int(self.config.redis_pool.call("LLEN", self.storage_key))

    def waiting_time(self):
        import time

        raw = self.config.redis_pool.call("LINDEX", self.storage_key, -1)
        if not raw:
            return 0.0
        envelope = json.loads(raw)
        stamp = envelope.get("queued_ms") or envelope.get("created_ms")
        return max(time.time() - float(stamp) / 1000, 0.0) if stamp else 0.0

    def purge(self):
        with self.config.redis_pool.pipelined() as pipeline:
            pipeline.call("DEL", self.storage_key)
            pipeline.call("SREM", keyspace.CHANNELS, self.name)
        return self

    def paused(self):
        return int(self.config.redis_pool.call("SISMEMBER", keyspace.PAUSED_CHANNELS, self.name)) == 1

    def pause(self):
        self.config.redis_pool.call("SADD", keyspace.PAUSED_CHANNELS, self.name)
        return self

    def resume(self):
        self.config.redis_pool.call("SREM", keyspace.PAUSED_CHANNELS, self.name)
        return self


class TimedTasks:
    def __init__(self, key, config=None):
        self.key = key
        self.config = _cfg(config)

    def __iter__(self):
        values = self.config.redis_pool.call("ZRANGE", self.key, 0, -1, "WITHSCORES")
        for raw, score in _pairs(values):
            yield StoredTask(raw, score=score, config=self.config, owner=self)

    def size(self):
        return int(self.config.redis_pool.call("ZCARD", self.key))

    def purge(self):
        self.config.redis_pool.call("DEL", self.key)
        return self

    def locate(self, *ids):
        wanted = {i for group in ids for i in (group if isinstance(group, (list, tuple, set)) else [group])}
        return [task for task in self if task.id in wanted]

    def requeue_all(self):
        for task in list(self):
            task.requeue()


class PlannedTasks(TimedTasks):
    def __init__(self, config=None):
        super().__init__(keyspace.PLANNED, config)


class RetryingTasks(TimedTasks):
    def __init__(self, config=None):
        super().__init__(keyspace.RETRIES, config)


class DiscardedTasks(TimedTasks):
    def __init__(self, config=None):
        super().__init__(keyspace.DISCARDED, config)


class Execution:
    def __init__(self, node_id, executor_id, raw):
        self.node_id = node_id
        self.executor_id = executor_id
        self.attributes = json.loads(raw)

    channel = property(lambda self: self.attributes.get("channel"))
    envelope = property(lambda self: self.attributes.get("envelope"))
    claim_token = property(lambda self: self.attributes.get("claim_token"))
    started_at = property(lambda self: _time(self.attributes["started_at"]))
    attempt = property(lambda self: int(self.attributes.get("attempt") or 1))


class Node:
    CONTROL_SIGNALS = {"pause": "TSTP", "shutdown": "TERM", "backtraces": "TTIN"}

    def __init__(self, identity, config=None):
        self.identity = identity
        self.config = _cfg(config)
        self.attributes = dict(_pairs(self.config.redis_pool.call("HGETALL", keyspace.node(identity))))
        self.info = json.loads(self.attributes.get("info", "{}"))

    hostname = property(lambda self: self.info.get("hostname"))
    pid = property(lambda self: int(self.info.get("pid") or 0))
    channels = property(lambda self: self.info.get("channels") or [])

    def active_count(self):
        return int(self.attributes.get("busy") or 0)

    def concurrency(self):
        return int(self.attributes.get("concurrency") or 0)

    def quiet(self):
        return self.attributes.get("quiet") == "true"

    def last_seen_at(self):
        return _time(self.attributes["beat"]) if "beat" in self.attributes else None

    def request_control(self, action):
        signal = self.CONTROL_SIGNALS[action]
        self.config.redis_pool.call("LPUSH", keyspace.node_signals(self.identity), signal)
        return True

    def executions(self):
        fields = self.config.redis_pool.call("HGETALL", keyspace.node_work(self.identity))
        return [Execution(self.identity, executor, raw) for executor, raw in _pairs(fields)]


class Nodes:
    def __init__(self, config=None):
        self.config = _cfg(config)

    def __iter__(self):
        for identity in self.config.redis_pool.call("SMEMBERS", keyspace.NODES) or []:
            node = Node(identity, self.config)
            if node.last_seen_at() is not None:
                yield node
            else:
                self.config.redis_pool.call("SREM", keyspace.NODES, identity)


class ClaimInfo:
    def __init__(self, raw):
        self.attributes = json.loads(raw)

    task_id = property(lambda self: self.attributes.get("task_id"))
    claim_token = property(lambda self: self.attributes.get("claim_token"))
    node_id = property(lambda self: self.attributes.get("node_id"))
    executor_id = property(lambda self: self.attributes.get("executor_id"))
    channel = property(lambda self: self.attributes.get("channel"))
    attempt = property(lambda self: int(self.attributes["attempt"]))
    claimed_at = property(lambda self: _time(self.attributes["claimed_at"]))


class Claims:
    def __init__(self, config=None):
        self.config = _cfg(config)

    def __iter__(self):
        pool = self.config.redis_pool
        for key in _scan(pool, f"{keyspace.PREFIX}:node:*:claims"):
            for raw in pool.call("HVALS", key) or []:
                yield ClaimInfo(raw)

    def find_task(self, task_id):
        return next((claim for claim in self if claim.task_id == task_id), None)


class Executions:
    def __init__(self, config=None):
        self.nodes = Nodes(config)

    def __iter__(self):
        for node in self.nodes:
            yield from node.executions()
