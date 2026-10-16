"""Per-interpreter global configuration (each subinterpreter owns its own copy)."""

from .blueprint import Blueprint

_config = None


def config():
    global _config
    if _config is None:
        _config = Blueprint()
    return _config


def use_config(blueprint):
    global _config
    _config = blueprint
    return blueprint


def configure(block=None):
    current = config()
    if block is not None:
        block(current)
    return current


def reset():
    global _config
    if _config is not None:
        _config.close()
    _config = None
