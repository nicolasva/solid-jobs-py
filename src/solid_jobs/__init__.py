"""SolidJobs for Python: Redis task runner on PEP 734 subinterpreters.

Redis access goes through the Ruby ``solid-redis`` gem, run as a sidecar.
"""

from . import lab
from .blueprint import Blueprint
from .catalog import (Channel, Claims, Counters, DiscardedTasks, Execution, Executions, Node, Nodes,
                      PlannedTasks, RetryingTasks, StoredTask)
from .conductor import Conductor
from .errors import (ConfigurationError, Error, ExecutionHalt, GatewayError, InvalidArgumentError,
                     InvalidJobError, NoCapturedTask, RedisCommandError, RedisConnectionError, RedisError)
from .integrity_check import IntegrityCheck
from .publisher import Publisher
from .settings import config, configure, reset, use_config
from .task import Task
from .version import VERSION

__version__ = VERSION
testing = lab.testing

__all__ = [
    "Blueprint", "Channel", "Claims", "Conductor", "ConfigurationError", "Counters", "DiscardedTasks",
    "Error", "Execution", "ExecutionHalt", "Executions", "GatewayError", "IntegrityCheck",
    "InvalidArgumentError", "InvalidJobError", "NoCapturedTask", "Node", "Nodes", "PlannedTasks",
    "Publisher", "RedisCommandError", "RedisConnectionError", "RedisError", "RetryingTasks",
    "StoredTask", "Task", "VERSION", "config", "configure", "lab", "reset", "testing", "use_config",
]
