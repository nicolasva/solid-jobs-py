import os
import signal
import subprocess
import sys
import time
import unittest

from tests.helpers import REDIS_URL, RedisTestCase
from tests.support.jobs import DONE_KEY, RecordTask

ROOT = os.path.join(os.path.dirname(__file__), "..")


class CliTest(RedisTestCase):
    def test_cli_runs_jobs_and_stops_on_sigterm(self):
        RecordTask.enqueue_many([[i] for i in range(5)])
        env = {**os.environ, "PYTHONPATH": os.pathsep.join([os.path.join(ROOT, "src"), ROOT])}
        proc = subprocess.Popen(
            [sys.executable, "-m", "solid_jobs", "-r", "tests.support.jobs", "-c", "2",
             "-t", "3", "--redis-url", REDIS_URL], env=env, cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        try:
            deadline = time.time() + 30
            while time.time() < deadline and self.redis.call("SCARD", DONE_KEY) < 5:
                time.sleep(0.2)
            self.assertEqual(self.redis.call("SCARD", DONE_KEY), 5)
        finally:
            proc.send_signal(signal.SIGTERM)
            output = proc.communicate(timeout=30)[0].decode()
        self.assertEqual(proc.returncode, 0, output)


if __name__ == "__main__":
    unittest.main()
