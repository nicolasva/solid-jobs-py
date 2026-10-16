import os
import secrets
import socket
import time

from . import keyspace, publication_barrier
from .errors import RedisError
from .instrumentation import emit

CLAIMED_PATTERN = f"{keyspace.PREFIX}:node:*:claimed:*"
RESTORE_ONE = """
local payload = redis.call("rpop", KEYS[1])
if payload then
  local envelope = cjson.decode(payload)
  local publication = envelope["__FIELD__"]
  if not publication then
    publication = ARGV[3]
    envelope["__FIELD__"] = publication
    payload = cjson.encode(envelope)
  end
  redis.call("set", ARGV[1] .. publication, ARGV[4], "PX", ARGV[2])
  redis.call("rpush", "__PREFIX__:channel:" .. envelope["channel"], payload)
  return {payload, publication, ARGV[4]}
end
""".replace("__FIELD__", publication_barrier.FIELD).replace("__PREFIX__", keyspace.PREFIX)

HEARTBEAT_TTL = 60


def call(config, dead_identities=()):
    """Requeue claims of dead nodes into their channels. ``dead_identities`` forces specific nodes."""
    recovered = 0
    pool = config.redis_pool
    for key in _scan_keys(pool):
        node_key = key.split(":claimed:", 1)[0]
        identity = node_key[len(f"{keyspace.PREFIX}:node:"):]
        if identity not in dead_identities and _alive(config, identity):
            continue
        while True:
            restored = pool.call(
                "EVAL", RESTORE_ONE, 1, key, keyspace.PUBLICATION_PREFIX,
                publication_barrier.TIMEOUT_MS, secrets.token_hex(16), secrets.token_hex(16))
            if not restored:
                break
            payload, publication, owner = restored
            recovered += 1
            emit(config, "recovered", payload)
            _release_publication(config, publication, owner)
        pool.call("DEL", key)
        pool.call("DEL", keyspace.claims(identity))
    return recovered


def _release_publication(config, publication, owner):
    entry = publication_barrier.prepare(publication=publication, owner=owner)
    try:
        publication_barrier.release(config.redis_pool, [entry])
    except RedisError as error:
        config.logger.warning("SolidJobs recovery barrier release failed; waiting for expiry: %s: %s",
                              type(error).__name__, error)


def _alive(config, identity):
    hostname, _, rest = identity.partition(":")
    pid = rest.partition(":")[0]
    if hostname == socket.gethostname() and pid.isdigit() and not _process_alive(int(pid)):
        return False
    beat = config.redis_pool.call("HGET", keyspace.node(identity), "beat")
    return bool(beat) and time.time() - float(beat) < HEARTBEAT_TTL


def _process_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _scan_keys(pool):
    cursor, keys = "0", []
    while True:
        cursor, found = pool.call("SCAN", cursor, "MATCH", CLAIMED_PATTERN, "COUNT", 100)
        keys.extend(found)
        if cursor == "0":
            return keys
