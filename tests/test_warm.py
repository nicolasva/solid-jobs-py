import json
import os
import socket
import time
import unittest

from solid_jobs import Conductor, IntegrityCheck, errors
from solid_jobs.executor import Executor
from solid_jobs.warm import WarmClient, WarmPool, protocol
from tests.helpers import RedisTestCase
from tests.support.warm_jobs import (ATTEMPTS, WARM_KEY, CrashOnce, HangOnce, NativeRecord, Probe, RaiseWarm,
                                     WarmRecord)


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


class WarmTestCase(RedisTestCase):
    def setUp(self):
        super().setUp()
        self.config.warm_requires = ["tests.support.warm_jobs"]
        self.addCleanup(self.reset_execution)

    def reset_execution(self):
        self.config.warm_pool = {"enabled": False}
        self.config.warm_socket = None
        self.config.execution_registry = {}
        self.config.execution_default_mode = "native"

    def wait_for(self, count, timeout=40):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.redis.call("SCARD", WARM_KEY) >= count:
                return True
            time.sleep(0.1)
        return False

    def start_conductor(self, **warm):
        self.config.warm_pool = {"enabled": True, "workers": 2, "startup_timeout": 30, **warm}
        conductor = Conductor(self.config, stop_grace=3.0)
        conductor.start()
        self.addCleanup(conductor.stop)
        return conductor

    def members(self):
        return [m.split(":") for m in self.redis.call("SMEMBERS", WARM_KEY)]


class RoutingTest(WarmTestCase):
    def test_modes(self):
        from solid_jobs import routing

        self.assertEqual(routing.resolve({"task": "a.B"}, self.config), "native")
        self.assertEqual(routing.resolve({"task": "a.B", "execution_mode": "warm"}, self.config), "warm")
        self.config.configure_execution(registry={"a.B": "warm"})
        self.assertEqual(routing.resolve({"task": "a.B", "execution_mode": "auto"}, self.config), "warm")
        self.assertEqual(routing.resolve({"task": "a.C"}, self.config), "native")
        with self.assertRaises(errors.InvalidJobError):
            routing.resolve({"task": "a.B", "execution_mode": "bogus"}, self.config)

    def test_mode_is_published_with_the_job(self):
        WarmRecord.enqueue(1)
        raw = self.redis.call("RPOP", "solid_jobs:channel:default")
        self.assertEqual(json.loads(raw)["execution_mode"], "warm")

    def test_warm_job_without_pool_fails_loudly(self):
        with self.assertRaises(errors.ConfigurationError):
            Executor(self.config).execute(
                {"id": "1", "task": "tests.support.warm_jobs.WarmRecord", "arguments": [1], "channel": "default",
                 "execution_mode": "warm"})


class WarmExecutionTest(WarmTestCase):
    def test_jobs_run_in_persistent_processes_without_reloading_modules(self):
        ids = list(WarmRecord.enqueue_many([[i] for i in range(40)]))
        NativeRecord.enqueue_many([[100 + i] for i in range(10)])
        conductor = self.start_conductor()
        self.assertTrue(self.wait_for(50), self.redis.call("SCARD", WARM_KEY))
        stats = conductor.warm_pool_stats()
        members = self.members()
        conductor.stop()
        by_mode = {m[0]: m for m in members}
        warm = [by_mode[str(i)] for i in range(40)]
        native = [by_mode[str(100 + i)] for i in range(10)]
        self.assertNotIn(str(os.getpid()), {w[1] for w in warm})
        self.assertLessEqual(len({(w[1], w[2]) for w in warm}), 2)  # 2 workers, module imported once each
        self.assertEqual(stats["restarts"], 0)
        self.assertGreaterEqual(stats["jobs"], 40)
        self.assertEqual(len(native), 10)
        report = IntegrityCheck.call(expected_job_ids=ids, acked_job_ids=ids)
        self.assertTrue(report.ok(), report)

    def test_error_in_warm_job_follows_failure_policy(self):
        RaiseWarm.enqueue(1)
        self.start_conductor()
        deadline = time.time() + 30
        while time.time() < deadline and not self.redis.call("ZCARD", "solid_jobs:discarded"):
            time.sleep(0.1)
        self.assertEqual(self.redis.call("ZCARD", "solid_jobs:discarded"), 1)

    def test_worker_crash_is_replaced_and_job_retried(self):
        CrashOnce.enqueue_many([[i] for i in range(6)])
        WarmRecord.enqueue_many([[100 + i] for i in range(20)])
        conductor = self.start_conductor(restart_backoff=0.2)
        self.assertTrue(self.wait_for(26), self.redis.call("SCARD", WARM_KEY))
        self.assertGreaterEqual(conductor.warm_pool_stats()["restarts"], 6)

    def test_hung_worker_is_terminated_and_replaced(self):
        HangOnce.enqueue(1)
        conductor = self.start_conductor(job_timeout=1.5)
        self.assertTrue(self.wait_for(1, 40))
        self.assertGreaterEqual(conductor.warm_pool_stats()["restarts"], 1)

    def test_workers_are_recycled(self):
        WarmRecord.enqueue_many([[i] for i in range(30)])
        conductor = self.start_conductor(max_jobs_per_worker=5)
        self.assertTrue(self.wait_for(30))
        self.assertGreaterEqual(conductor.warm_pool_stats()["restarts"], 5)

    def test_crash_campaign_loses_no_job(self):
        ids = list(CrashOnce.enqueue_many([[i] for i in range(120)]))
        conductor = self.start_conductor(restart_backoff=0.2)
        self.assertTrue(self.wait_for(120, 90), self.redis.call("SCARD", WARM_KEY))
        conductor.stop()
        report = IntegrityCheck.call(expected_job_ids=ids, acked_job_ids=ids)
        self.assertTrue(report.ok(), report)

    def test_one_job_at_a_time_per_worker(self):
        Probe.enqueue_many([[i] for i in range(60)])
        self.config.concurrency = 6
        self.addCleanup(setattr, self.config, "concurrency", 2)
        self.start_conductor()
        self.assertTrue(self.wait_for(60))
        self.assertEqual({m[2] for m in self.members()}, {"1"})

    def test_stop_leaves_no_processes(self):
        conductor = self.start_conductor()
        pool = conductor._warm_pool
        pids = [p.pid for p in pool._processes]
        self.assertEqual(len(pids), 2)
        conductor.stop()
        deadline = time.time() + 10
        while time.time() < deadline and any(alive(pid) for pid in pids):
            time.sleep(0.1)
        self.assertFalse(any(alive(pid) for pid in pids))
        self.assertFalse(os.path.exists(pool.socket_path or "/nonexistent"))


class WarmSecurityTest(WarmTestCase):
    def client(self):
        conductor = self.start_conductor(max_message_bytes=64 * 1024)
        options = self.config.warm_pool
        client = WarmClient(self.config.warm_socket, options["serializer"], 10, options["max_message_bytes"])
        self.addCleanup(client.close)
        return conductor, client

    def envelope(self, task, arguments=(1,)):
        return {"id": "x", "task": task, "arguments": list(arguments), "channel": "default"}

    def test_non_task_classes_are_rejected(self):
        _, client = self.client()
        for name in ("os.system", "builtins.eval", "not a class", "tests.support.warm_jobs.IMPORT_ID"):
            with self.assertRaises(errors.InvalidJobError, msg=name):
                client.execute(self.envelope(name))

    def test_socket_is_private(self):
        self.client()
        self.assertEqual(os.stat(self.config.warm_socket).st_mode & 0o777, 0o600)

    def test_oversized_and_malformed_messages_are_rejected(self):
        _, client = self.client()
        with self.assertRaises(errors.WarmPoolError):
            client.execute(self.envelope("tests.support.warm_jobs.WarmRecord", ["x" * 200_000]))
        raw = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        raw.connect(self.config.warm_socket)
        self.addCleanup(raw.close)
        protocol.send_frame(raw, b"not json")
        reply = json.loads(protocol.recv_frame(raw))
        self.assertEqual(reply["status"], "error")
        self.assertFalse(reply["error"]["retryable"])

    def test_msgpack_serializer(self):
        try:
            import msgpack  # noqa: F401
        except ImportError:
            self.skipTest("msgpack not installed")
        WarmRecord.enqueue_many([[i] for i in range(5)])
        self.start_conductor(serializer="msgpack")
        self.assertTrue(self.wait_for(5))


if __name__ == "__main__":
    unittest.main()
