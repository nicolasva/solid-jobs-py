from solid_jobs import Task, settings

DONE_KEY = "test:done"
FAILS_KEY = "test:fails"


class RecordTask(Task, channel="default", max_failures=3):
    def execute_task(self, value):
        settings.config().redis_pool.call("SADD", DONE_KEY, str(value))


class FlakyTask(Task, channel="default", max_failures=3):
    @classmethod
    def retry_delay(cls, failure_count, error, envelope):
        return 0.1

    def execute_task(self, value):
        pool = settings.config().redis_pool
        if pool.call("INCR", f"{FAILS_KEY}:{value}") < 2:
            raise RuntimeError("boom")
        pool.call("SADD", DONE_KEY, str(value))


class CountTask(Task, channel="default", max_failures=3):
    def execute_task(self, value):
        settings.config().redis_pool.call("INCR", "test:count")
