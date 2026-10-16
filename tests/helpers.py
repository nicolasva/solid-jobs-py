import os
import sys
import unittest

from solid_jobs import Blueprint, settings

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6390/15")
SUPPORT = os.path.join(os.path.dirname(__file__), "..")
if SUPPORT not in sys.path:
    sys.path.insert(0, SUPPORT)


class RedisTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        settings.reset()
        cls.config = settings.use_config(Blueprint(redis_url=REDIS_URL, concurrency=2))
        cls.config.requires = ["tests.support.jobs"]
        cls.config.poll_interval_average = 0.2
        cls.config.shutdown_timeout = 3.0

    @classmethod
    def tearDownClass(cls):
        settings.reset()

    def setUp(self):
        self.redis = self.config.redis_pool
        self.redis.call("FLUSHDB")
