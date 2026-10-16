from . import lab, settings
from .errors import NoCapturedTask
from .publisher import DEFAULT_CHUNK_SIZE, Publisher
from .utilities import stringify_keys


class Task:
    """Base class for SolidJobs tasks.

    class RecalculateAccount(Task, channel="critical", max_failures=10):
        def execute_task(self, account_id): ...

    RecalculateAccount.enqueue(42)

    The task class must be importable by its ``module.QualName`` from every
    worker subinterpreter (so not defined in ``__main__``).
    """

    task_id = None

    def __init_subclass__(cls, **options):
        super().__init_subclass__()
        if options:
            cls._declared_options = stringify_keys(options)

    def execute_task(self, *arguments):
        raise NotImplementedError(f"{type(self).__name__} must implement execute_task")

    @property
    def logger(self):
        return settings.config().logger

    def interrupted(self):
        probe = getattr(self, "_interrupted", None)
        return bool(probe()) if probe else False

    @classmethod
    def retry_delay(cls, failure_count, error, envelope):
        """Return a delay in seconds, ``"drop"``, ``"archive"`` or ``None`` for the default backoff."""
        return None

    @classmethod
    def after_final_failure(cls, envelope, error):
        return None

    @classmethod
    def task_options(cls, **options):
        if options:
            cls._declared_options = {**cls.__dict__.get("_declared_options", {}), **stringify_keys(options)}
        return cls.current_task_options()

    @classmethod
    def current_task_options(cls):
        merged = settings.config().default_task_options
        for klass in reversed(cls.__mro__):
            merged.update(klass.__dict__.get("_declared_options", {}))
        return merged

    @classmethod
    def enqueue(cls, *arguments):
        return cls._publish({"task": cls, "arguments": arguments})

    @classmethod
    def enqueue_many(cls, argument_sets, chunk_size=DEFAULT_CHUNK_SIZE):
        return Publisher().publish_many(
            {**cls.current_task_options(), "task": cls, "arguments": argument_sets, "chunk_size": chunk_size})

    @classmethod
    def enqueue_after(cls, delay, *arguments):
        import time
        return cls.schedule_at(time.time() + float(delay), *arguments)

    @classmethod
    def schedule_at(cls, when, *arguments):
        timestamp = when.timestamp() if hasattr(when, "timestamp") else float(when)
        return cls._publish({"task": cls, "arguments": arguments, "run_at": timestamp})

    @classmethod
    def execute(cls, *arguments):
        return lab.execute({**cls.current_task_options(), "task": cls, "arguments": list(arguments)})

    @classmethod
    def with_options(cls, **options):
        return Submission(cls, options)

    @classmethod
    def captured(cls):
        return lab.captured_for(cls)

    @classmethod
    def clear_captured(cls):
        cls.captured().clear()

    @classmethod
    def execute_next(cls):
        captured = cls.captured()
        if not captured:
            raise NoCapturedTask(f"No captured task for {cls.__name__}")
        return lab.execute(captured.pop(0))

    @classmethod
    def _publish(cls, envelope):
        return Publisher().publish({**cls.current_task_options(), **envelope})


class Submission:
    def __init__(self, task, options):
        self.task = task
        self.options = stringify_keys(options)

    def enqueue(self, *arguments):
        options = dict(self.options)
        if options.pop("execute", False):
            return self.task.execute(*arguments)
        if (delay := options.pop("delay", None)) is not None:
            return self.enqueue_after(delay, *arguments)
        if (run_at := options.pop("run_at", None)) is not None:
            return self.schedule_at(run_at, *arguments)
        return self._publish(arguments, options)

    def enqueue_after(self, delay, *arguments):
        import time
        return self.schedule_at(time.time() + float(delay), *arguments)

    def schedule_at(self, when, *arguments):
        timestamp = when.timestamp() if hasattr(when, "timestamp") else float(when)
        options = {k: v for k, v in self.options.items() if k not in ("delay", "run_at", "execute")}
        return self._publish(arguments, options, {"run_at": timestamp})

    def _publish(self, arguments, options, extra=None):
        return Publisher().publish({
            **self.task.current_task_options(), **options, **(extra or {}),
            "task": self.task, "arguments": list(arguments),
        })
