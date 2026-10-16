class Error(Exception):
    pass


class InvalidJobError(Error, ValueError):
    pass


class InvalidArgumentError(InvalidJobError):
    pass


class ConfigurationError(Error):
    pass


class ExecutionHalt(Error):
    pass


class NoCapturedTask(Error):
    pass


class RedisError(Error):
    pass


class RedisConnectionError(RedisError):
    pass


class RedisCommandError(RedisError):
    pass


class GatewayError(RedisConnectionError):
    """The solid-redis sidecar could not be started or has died."""


class WarmPoolError(Error):
    """The Warm IPC Pool is unavailable or misbehaved; the job counts as a failed attempt."""


class WarmExecutionError(Error):
    """A job raised inside a warm worker process."""

    def __init__(self, message, remote_class=None):
        super().__init__(message)
        self.remote_class = remote_class
