import time
import unittest

from solid_jobs import Conductor, IntegrityCheck
from tests.helpers import RedisTestCase
from tests.support.jobs import DONE_KEY, CountTask, FlakyTask, RecordTask


class ConductorTest(RedisTestCase):
    def wait_for(self, count, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.redis.call("SCARD", DONE_KEY) >= count:
                return True
            time.sleep(0.1)
        return False

    def run_conductor(self):
        conductor = Conductor(self.config, stop_grace=3.0)
        conductor.start()
        self.addCleanup(conductor.stop)
        return conductor

    def test_executes_jobs_in_subinterpreters(self):
        ids = list(RecordTask.enqueue_many([[i] for i in range(20)]))
        conductor = self.run_conductor()
        self.assertTrue(self.wait_for(20), self.redis.call("SCARD", DONE_KEY))
        conductor.stop()
        report = IntegrityCheck.call(expected_job_ids=ids, acked_job_ids=ids)
        self.assertTrue(report.ok(), report)

    def test_no_duplicate_execution_at_startup(self):
        CountTask.enqueue_many([[i] for i in range(200)])
        self.config.concurrency = 4
        self.addCleanup(setattr, self.config, "concurrency", 2)
        self.run_conductor()
        deadline = time.time() + 30
        while time.time() < deadline and int(self.redis.call("GET", "test:count") or 0) < 200:
            time.sleep(0.1)
        time.sleep(1.5)
        self.assertEqual(int(self.redis.call("GET", "test:count")), 200)

    def test_retry_then_success(self):
        FlakyTask.enqueue("f")
        self.run_conductor()
        self.assertTrue(self.wait_for(1, 40))

    def test_planned_job_is_promoted(self):
        RecordTask.enqueue_after(1, "later")
        self.run_conductor()
        self.assertTrue(self.wait_for(1, 40))


if __name__ == "__main__":
    unittest.main()
