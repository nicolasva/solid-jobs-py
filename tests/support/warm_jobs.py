import os
import time
import uuid

from solid_jobs import Task, settings

WARM_KEY = "test:warm"
ATTEMPTS = "test:warm:attempts"
IMPORT_ID = uuid.uuid4().hex  # one per process that imports this module


class WarmRecord(Task, channel="default", max_failures=3, execution_mode="warm"):
    def execute_task(self, value):
        settings.config().redis_pool.call("SADD", WARM_KEY, f"{value}:{os.getpid()}:{IMPORT_ID}")


class NativeRecord(Task, channel="default", max_failures=3, execution_mode="native"):
    def execute_task(self, value):
        settings.config().redis_pool.call("SADD", WARM_KEY, f"{value}:{os.getpid()}:{IMPORT_ID}")


class CrashOnce(Task, channel="default", max_failures=3, execution_mode="warm"):
    @classmethod
    def retry_delay(cls, failure_count, error, envelope):
        return 0.1

    def execute_task(self, value):
        pool = settings.config().redis_pool
        if pool.call("INCR", f"{ATTEMPTS}:{value}") == 1:
            os._exit(1)
        pool.call("SADD", WARM_KEY, f"{value}:{os.getpid()}:{IMPORT_ID}")


class HangOnce(Task, channel="default", max_failures=3, execution_mode="warm"):
    @classmethod
    def retry_delay(cls, failure_count, error, envelope):
        return 0.1

    def execute_task(self, value):
        pool = settings.config().redis_pool
        if pool.call("INCR", f"{ATTEMPTS}:{value}") == 1:
            time.sleep(60)
        pool.call("SADD", WARM_KEY, f"{value}:{os.getpid()}:{IMPORT_ID}")


class RaiseWarm(Task, channel="default", max_failures=0, execution_mode="warm"):
    def execute_task(self, value):
        raise RuntimeError(f"warm boom {value}")


class Probe(Task, channel="default", max_failures=3, execution_mode="warm"):
    """Records the highest number of simultaneous executions seen inside one worker process."""

    def execute_task(self, value):
        pool = settings.config().redis_pool
        key = f"test:warm:running:{os.getpid()}"
        running = pool.call("INCR", key)
        pool.call("SADD", WARM_KEY, f"{value}:{os.getpid()}:{running}")
        time.sleep(0.02)
        pool.call("DECR", key)
