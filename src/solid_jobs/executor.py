import json
from collections import namedtuple

from .errors import InvalidJobError
from .utilities import constantize

Execution = namedtuple("Execution", "task envelope")


class Executor:
    def __init__(self, config=None):
        from . import settings

        self.config = config or settings.config()

    def execute(self, envelope):
        if isinstance(envelope, (str, bytes)):
            envelope = json.loads(envelope)
        if not isinstance(envelope, dict):
            raise InvalidJobError("Task envelope must be a dict")
        name = envelope.get("task")
        if not isinstance(name, str) or not name:
            raise InvalidJobError("Task must be present")
        arguments = envelope.get("arguments")
        if not isinstance(arguments, list):
            raise InvalidJobError("Task arguments must be a list")
        channel = envelope.get("channel")
        if not isinstance(channel, str) or not channel:
            raise InvalidJobError("Task channel must be present")

        task = constantize(name)()
        task.task_id = envelope.get("id")
        interceptors = self.config.execute_interceptors
        if interceptors.empty():
            return task.execute_task(*arguments)
        return interceptors.call(Execution(task, envelope), lambda: task.execute_task(*arguments))
