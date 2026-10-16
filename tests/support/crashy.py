from solid_jobs import Task


class AlwaysFails(Task, max_failures=1):
    @classmethod
    def after_final_failure(cls, envelope, error):
        from solid_jobs import settings
        settings.config().redis_pool.call("SADD", "test:final", envelope["id"])

    def execute_task(self, value):
        raise ValueError("nope")
