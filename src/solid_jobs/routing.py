"""Execution routing: native subinterpreter or Warm IPC Pool.

The mode is explicit. It comes from the envelope (``execution_mode`` task option), then from the
compatibility registry, then from ``execution_default_mode``. ``auto`` means "use the registry or the
default"; it never inspects arguments or imports and never retries a job on the other engine.
"""

from .errors import ConfigurationError, InvalidJobError

MODES = ("native", "warm", "auto")


def resolve(envelope, config):
    mode = envelope.get("execution_mode") or "auto"
    if mode not in MODES:
        raise InvalidJobError(f"execution_mode must be one of {', '.join(MODES)}, got {mode!r}")
    if mode != "auto":
        return mode
    return config.execution_registry.get(envelope.get("task")) or config.execution_default_mode


def execute_warm(config, envelope):
    client = config.warm_client
    if client is None:
        raise ConfigurationError(
            f"task {envelope.get('task')!r} requires the warm pool, which is not enabled "
            "(set warm_pool['enabled'] or run with --warm-workers)")
    return client.execute(envelope)
