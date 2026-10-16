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
