import time
import unittest

from solid_jobs import IntegrityCheck
from solid_jobs.claim import Claim
from solid_jobs.recovery import call as recover
from tests.helpers import RedisTestCase
from tests.support.crashy import AlwaysFails
from tests.support.jobs import RecordTask


class ClaimTest(RedisTestCase):
    def claim(self, identity="nodeA", processor=0):
        return Claim(self.config, identity=identity, processor_id=processor)

    def test_claim_ack(self):
        job_id = RecordTask.enqueue(1)
        record = self.claim().next()
        self.assertEqual(record.envelope["id"], job_id)
        self.assertTrue(record.complete())
        self.assertFalse(record.complete())
        self.assertIsNone(self.claim().next())

    def test_requeue_then_stale_ack_is_fenced(self):
        RecordTask.enqueue(1)
        first = self.claim().next()
        self.assertTrue(first.requeue())
        second = self.claim("nodeB").next()
        self.assertFalse(first.complete())
        self.assertTrue(second.complete())

    def test_dead_node_claims_are_recovered(self):
        job_id = RecordTask.enqueue(1)
        self.assertIsNotNone(self.claim("ghost:1:abc").next())
        self.assertEqual(recover(self.config, dead_identities=("ghost:1:abc",)), 1)
        record = self.claim("nodeB").next()
        self.assertEqual(record.envelope["id"], job_id)
        report = IntegrityCheck.call(expected_job_ids=[job_id])
        self.assertTrue(report.ok(), report)


class FailureTest(RedisTestCase):
    def test_final_failure_archives_and_calls_hook(self):
        from solid_jobs import Conductor
        job_id = AlwaysFails.enqueue(1)
        self.config.requires = ["tests.support.crashy"]
        conductor = Conductor(self.config, stop_grace=3.0).start()
        self.addCleanup(conductor.stop)
        deadline = time.time() + 30
        while time.time() < deadline and not self.redis.call("SCARD", "test:final"):
            time.sleep(0.1)
        self.assertEqual(self.redis.call("SMEMBERS", "test:final"), [job_id])
        conductor.stop()
        report = IntegrityCheck.call(expected_job_ids=[job_id])
        self.assertTrue(report.ok(), report)


class DropInterceptor:
    def around(self, context, proceed):
        return None


class TagInterceptor:
    def around(self, context, proceed):
        return proceed()


class InterceptorTest(RedisTestCase):
    def tearDown(self):
        self.config.publish_interceptors.discard(DropInterceptor)
        self.config.publish_interceptors.discard(TagInterceptor)

    def test_publish_interceptor_can_drop(self):
        self.config.publish_interceptors.use(DropInterceptor)
        self.assertIsNone(RecordTask.enqueue(1))
        self.assertEqual(self.redis.call("LLEN", "solid_jobs:channel:default"), 0)

    def test_passthrough_interceptor(self):
        self.config.publish_interceptors.use(TagInterceptor)
        self.assertIsNotNone(RecordTask.enqueue(1))


if __name__ == "__main__":
    unittest.main()
