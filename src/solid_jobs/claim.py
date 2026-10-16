import json
import random
import secrets
import time
from dataclasses import dataclass
from typing import Any

from . import keyspace, publication_barrier
from .instrumentation import emit, worker_attributes
from .errors import RedisCommandError
from .utilities import run_script, sha1

TIMEOUT = 0.25
_FIELD = publication_barrier.FIELD

RESERVE = """
local payload = redis.call("rpop", KEYS[1])
if not payload then
  return nil
end
local job = cjson.decode(payload)
local publication = job["__FIELD__"]
if publication and redis.call("exists", ARGV[5] .. publication) == 1 then
  redis.call("rpush", KEYS[1], payload)
  return nil
end

redis.call("lpush", KEYS[2], payload)
local attempt = redis.call("hincrby", KEYS[4], job["id"], 1)
local metadata = {
  task_id = job["id"],
  claim_token = ARGV[1],
  node_id = ARGV[2],
  executor_id = ARGV[3],
  channel = job["channel"],
  claimed_at = tonumber(ARGV[4]),
  attempt = attempt
}
redis.call("hset", KEYS[3], ARGV[3], cjson.encode(metadata))
return {payload, attempt}
""".replace("__FIELD__", _FIELD)

POP_READY = """
local payload = redis.call("rpop", KEYS[1])
if not payload then
  return nil
end
local job = cjson.decode(payload)
local publication = job["__FIELD__"]
if publication and redis.call("exists", ARGV[1] .. publication) == 1 then
  redis.call("rpush", KEYS[1], payload)
  return nil
end
return payload
""".replace("__FIELD__", _FIELD)

REGISTER = """
local attempt = redis.call("hincrby", KEYS[2], ARGV[1], 1)
local metadata = cjson.decode(ARGV[3])
metadata["attempt"] = attempt
redis.call("hset", KEYS[1], ARGV[2], cjson.encode(metadata))
return attempt
"""

ACK = """
local metadata = redis.call("hget", KEYS[2], ARGV[2])
if not metadata then
  return 0
end
if cjson.decode(metadata)["claim_token"] ~= ARGV[3] then
  return 0
end
local removed = redis.call("lrem", KEYS[1], 1, ARGV[1])
redis.call("hdel", KEYS[2], ARGV[2])
if removed > 0 then
  redis.call("hdel", KEYS[3], ARGV[4])
end
return removed
"""

REQUEUE = """
local metadata = redis.call("hget", KEYS[2], ARGV[2])
if not metadata then
  return 0
end
if cjson.decode(metadata)["claim_token"] ~= ARGV[3] then
  return 0
end
local removed = redis.call("lrem", KEYS[1], 1, ARGV[1])
if removed > 0 then
  redis.call("rpush", KEYS[3], ARGV[1])
end
redis.call("hdel", KEYS[2], ARGV[2])
return removed
"""

RESERVE_SHA, POP_READY_SHA, REGISTER_SHA, ACK_SHA, REQUEUE_SHA = (
    sha1(script) for script in (RESERVE, POP_READY, REGISTER, ACK, REQUEUE))


@dataclass
class ClaimRecord:
    channel: str
    payload: str
    envelope: dict
    redis_pool: Any
    claimed_key: str | None
    claims_key: str | None
    executor_field: str
    claim_token: str | None
    attempt: int | None

    def ack_arguments(self):
        return ([self.claimed_key, self.claims_key, keyspace.ATTEMPTS],
                [self.payload, self.executor_field, self.claim_token, self.envelope["id"]])

    def complete(self):
        if self.claimed_key:
            keys, arguments = self.ack_arguments()
            return int(run_script(self.redis_pool, ACK, ACK_SHA, keys, *arguments)) == 1
        return True

    def requeue(self):
        destination = keyspace.channel(self.channel)
        if self.claimed_key:
            removed = run_script(
                self.redis_pool, REQUEUE, REQUEUE_SHA,
                [self.claimed_key, self.claims_key, destination],
                self.payload, self.executor_field, self.claim_token)
            return int(removed) == 1
        self.redis_pool.call("RPUSH", destination, self.payload)
        return True


class Claim:
    def __init__(self, config, identity=None, processor_id=None):
        self.config = config
        self.redis_pool = config.redis_pool
        self.channel_order = config.channel_order
        reliable = config.reliable_fetch
        self.channels = [keyspace.channel(name) for name, weight in config.channel_entries
                         for _ in range(weight)]
        self.channel_names = {key: key[len(keyspace.CHANNEL_PREFIX):] for key in self.channels}
        self.available_channels = self.channels
        self.claimed_key = keyspace.claimed(identity, processor_id) if reliable and identity else None
        self.claims_key = keyspace.claims(identity) if self.claimed_key else None
        self.identity = identity
        self.processor_id = processor_id
        self.executor_field = str(processor_id)
        self.paused_refresh_at = 0.0
        self.channel_index = 0
        self.check_claimed = bool(self.claimed_key)
        self.pending = None
        self.scripts_loaded = False

    def defer_ack(self, record, on_result):
        """Hold the acknowledgement so the next reservation sends it in the same Redis round trip.

        The ack still goes out before anything else the worker does, so fencing is unchanged; only the
        extra round trip disappears. ``on_result(bool)`` receives the fenced outcome once it is known.
        """
        if not record.claimed_key:
            on_result(True)
            return
        self.flush()
        self.pending = (record, on_result)

    def flush(self):
        pending, self.pending = self.pending, None
        if pending is None:
            return
        record, on_result = pending
        try:
            acknowledged = record.complete()
        except Exception:
            self.connection_failed()
            raise
        on_result(acknowledged)

    def next(self):
        if self.check_claimed:
            self.flush()
            self.check_claimed = False
            payload = self.redis_pool.call("LINDEX", self.claimed_key, -1)
            if payload:
                return self._existing_claim(payload)

        self._refresh_paused()
        channels = self.available_channels
        if not channels:
            self.flush()
            time.sleep(0.05)
            return None
        return self._claim_reliable(channels) if self.claimed_key else self._claim_unreliable(channels)

    def connection_failed(self):
        self.check_claimed = bool(self.claimed_key)

    def _record(self, payload, channel=None, claim_token=None, attempt=None):
        envelope = publication_barrier.strip(json.loads(payload))
        channel = channel or envelope["channel"]
        if claim_token is None:
            claim_token, attempt = self._register(envelope, channel)
        claim = ClaimRecord(
            channel=channel, payload=payload, envelope=envelope, redis_pool=self.redis_pool,
            claimed_key=self.claimed_key, claims_key=self.claims_key,
            executor_field=self.executor_field, claim_token=claim_token, attempt=attempt)
        emit(self.config, "reserved", envelope, reservation_id=claim_token, attempt=attempt,
             **worker_attributes(self.processor_id))
        return claim

    def _existing_claim(self, payload):
        raw = self.redis_pool.call("HGET", self.claims_key, str(self.processor_id))
        if raw:
            metadata = json.loads(raw)
            envelope = publication_barrier.strip(json.loads(payload))
            if metadata.get("task_id") == envelope.get("id"):
                return ClaimRecord(
                    channel=envelope["channel"], payload=payload, envelope=envelope,
                    redis_pool=self.redis_pool, claimed_key=self.claimed_key,
                    claims_key=self.claims_key, executor_field=self.executor_field,
                    claim_token=metadata["claim_token"], attempt=int(metadata["attempt"]))
        return self._record(payload)

    def _register(self, envelope, channel):
        if not self.claimed_key:
            return None, None
        claim_token = secrets.token_hex(16)
        metadata = json.dumps({
            "task_id": envelope["id"], "claim_token": claim_token, "node_id": self.identity,
            "executor_id": self.processor_id, "channel": channel, "claimed_at": time.time(),
        })
        attempt = run_script(self.redis_pool, REGISTER, REGISTER_SHA,
                             [self.claims_key, keyspace.ATTEMPTS],
                             envelope["id"], self.processor_id, metadata)
        return claim_token, int(attempt)

    def _next_channel(self, channels):
        if self.channel_order == "shuffle":
            return random.choice(channels)
        channel = channels[self.channel_index % len(channels)]
        self.channel_index += 1
        return channel

    def _ordered_sources(self, channels):
        if self.channel_order == "priority":
            return list(dict.fromkeys(channels))
        if self.channel_order == "shuffle":
            shuffled = list(channels)
            random.shuffle(shuffled)
            return list(dict.fromkeys(shuffled))
        preferred = self._next_channel(channels)
        return list(dict.fromkeys([preferred, *channels]))

    def _claim_reliable(self, channels):
        for source in self._ordered_sources(channels):
            claim = self._claim_now(source)
            if claim:
                return claim
        time.sleep(TIMEOUT)
        return None

    def _claim_unreliable(self, channels):
        for source in self._ordered_sources(channels):
            payload = run_script(self.redis_pool, POP_READY, POP_READY_SHA,
                                 [source], keyspace.PUBLICATION_PREFIX)
            if payload:
                return self._record(payload, self.channel_names[source])
        time.sleep(TIMEOUT)
        return None

    def _claim_now(self, source):
        claim_token = secrets.token_hex(16)
        keys = [source, self.claimed_key, self.claims_key, keyspace.ATTEMPTS]
        arguments = (claim_token, self.identity, self.processor_id, time.time(), keyspace.PUBLICATION_PREFIX)
        if self.pending is not None:
            result = self._ack_and_reserve(keys, arguments)
        else:
            result = run_script(self.redis_pool, RESERVE, RESERVE_SHA, keys, *arguments)
        if not result:
            return None
        payload, attempt = result
        return self._record(payload, self.channel_names[source], claim_token=claim_token,
                            attempt=int(attempt))

    def _ack_and_reserve(self, keys, arguments):
        record, on_result = self.pending
        if not self.scripts_loaded:
            self.redis_pool.call("SCRIPT", "LOAD", ACK)
            self.redis_pool.call("SCRIPT", "LOAD", RESERVE)
            self.scripts_loaded = True
        ack_keys, ack_arguments = record.ack_arguments()
        try:
            with self.redis_pool.pipelined() as pipeline:
                pipeline.call("EVALSHA", ACK_SHA, len(ack_keys), *ack_keys, *ack_arguments)
                pipeline.call("EVALSHA", RESERVE_SHA, len(keys), *keys, *arguments)
        except RedisCommandError as error:
            if "NOSCRIPT" not in str(error):
                raise
            # Scripts were flushed: reload, then let the ack run alone. The reservation inside the failed
            # pipeline may have succeeded, so re-adopt whatever sits in the claimed list.
            self.scripts_loaded = False
            self.check_claimed = True
            self.flush()
            return None
        self.pending = None
        removed, result = pipeline.results
        on_result(int(removed) == 1)
        return result

    def _refresh_paused(self):
        now = time.monotonic()
        if now < self.paused_refresh_at:
            return
        paused = self.redis_pool.call("SMEMBERS", keyspace.PAUSED_CHANNELS) or []
        self.available_channels = (
            self.channels if not paused
            else [key for key in self.channels if self.channel_names[key] not in paused])
        self.paused_refresh_at = now + 5
