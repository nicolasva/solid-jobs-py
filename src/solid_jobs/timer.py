import json
import random
import time

from . import keyspace
from .errors import RedisConnectionError
from .publisher import Publisher
from .recovery import call as recover
from .utilities import run_script, sha1

COLLECTIONS = (keyspace.RETRIES, keyspace.PLANNED)
POP_DUE = """
local jobs = redis.call("zrange", KEYS[1], "-inf", ARGV[1], "byscore", "limit", 0, 1)
if jobs[1] then
  redis.call("zrem", KEYS[1], jobs[1])
  return jobs[1]
end
"""
POP_DUE_SHA = sha1(POP_DUE)


class Timer:
    """Promotes due planned/retrying tasks and recovers claims of dead nodes."""

    def __init__(self, config):
        self.config = config
        self.publisher = Publisher(config=config)

    def enqueue_due(self, now=None):
        now = time.time() if now is None else now
        count = 0
        for collection in COLLECTIONS:
            while (raw := run_script(self.config.redis_pool, POP_DUE, POP_DUE_SHA, [collection], now)):
                self.publisher.publish(json.loads(raw))
                count += 1
        return count

    def run(self, stop):
        try:
            while not stop():
                try:
                    recover(self.config)
                    self.enqueue_due()
                except RedisConnectionError as error:
                    self.config.logger.warning("Redis scheduler poll failed, retrying: %s: %s",
                                               type(error).__name__, error)
                self._interruptible_sleep(self._randomized_interval(), stop)
        finally:
            self.config.close()

    def _randomized_interval(self):
        return float(self.config.poll_interval_average) * (0.5 + random.random())

    @staticmethod
    def _interruptible_sleep(duration, stop):
        deadline = time.monotonic() + duration
        while not stop() and time.monotonic() < deadline:
            time.sleep(0.05)
