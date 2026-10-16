import os
import stat
import unittest

from solid_jobs import IntegrityCheck, lab
from tests.helpers import RedisTestCase
from tests.support.jobs import RecordTask


class SidecarTest(RedisTestCase):
    def test_socket_is_0600_and_roundtrips(self):
        path = self.config.redis_pool.path if hasattr(self.config.redis_pool, "path") else None
        if path:
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
        self.assertEqual(self.redis.call("SET", "k", "v"), "OK")
        self.assertEqual(self.redis.call("GET", "k"), "v")

    def test_pipeline(self):
        with self.redis.pipelined() as p:
            p.call("SET", "a", 1)
            p.call("INCR", "a")
        self.assertEqual(p.results[-1], 2)


class PublishTest(RedisTestCase):
    def test_enqueue_stores_envelope(self):
        job_id = RecordTask.enqueue(1)
        report = IntegrityCheck.call(expected_job_ids=[job_id])
        self.assertTrue(report.ok(), report)
        self.assertEqual(len(report.states[job_id]), 1)

    def test_enqueue_many_and_schedule(self):
        ids = RecordTask.enqueue_many([[1], [2], [3]])
        later = RecordTask.enqueue_after(60, 4)
        report = IntegrityCheck.call(expected_job_ids=list(ids) + [later])
        self.assertTrue(report.ok(), report)

    def test_lab_capture_and_execute(self):
        with lab.testing("capture"):
            RecordTask.enqueue(9)
        self.assertEqual(len(RecordTask.captured()), 1)
        RecordTask.execute_next()
        self.assertIn("9", self.redis.call("SMEMBERS", "test:done"))
        RecordTask.clear_captured()
        self.assertEqual(lab.mode(), "disable")


if __name__ == "__main__":
    unittest.main()
