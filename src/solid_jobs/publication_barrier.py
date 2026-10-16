import secrets
from collections import namedtuple

from . import keyspace

TIMEOUT_MS = 30_000
FIELD = "_solid_jobs_publication"
Entry = namedtuple("Entry", "key publication owner")
RELEASE = """
local released = 0
for index, key in ipairs(KEYS) do
  if redis.call("get", key) == ARGV[index] then
    released = released + redis.call("del", key)
  end
end
return released
"""


def prepare(publication=None, owner=None):
    publication = publication or secrets.token_hex(16)
    return Entry(keyspace.publication(publication), publication, owner or secrets.token_hex(16))


def mark(target, entry):
    target.call("SET", entry.key, entry.owner, "PX", TIMEOUT_MS)


def release(redis_pool, entries):
    entries = list(entries)
    if not entries:
        return 0
    return redis_pool.call(
        "EVAL", RELEASE, len(entries),
        *[entry.key for entry in entries], *[entry.owner for entry in entries],
    )


def strip(envelope):
    envelope.pop(FIELD, None)
    return envelope
