"""Test helpers: capture or inline-execute tasks instead of writing to Redis."""

import contextlib
import uuid

from .utilities import class_path, stringify_keys

MODES = ("disable", "capture", "execute")
_mode = "disable"
_captured = {}


def mode():
    return _mode


def testing(new_mode):
    """Context manager: ``with lab.testing("capture"): ...``; also callable to set a mode globally."""
    return _Testing(new_mode)


class _Testing(contextlib.AbstractContextManager):
    def __init__(self, new_mode):
        global _mode
        if new_mode not in MODES:
            raise ValueError(f"Unknown testing mode: {new_mode!r}")
        self.previous = _mode
        _mode = new_mode

    def __exit__(self, *exc):
        global _mode
        _mode = self.previous
        return False


def captured_for(task):
    return _captured.setdefault(class_path(task) if not isinstance(task, str) else task, [])


def clear_all():
    _captured.clear()


def execute(envelope):
    from . import settings
    from .executor import Executor

    normalized = stringify_keys(dict(envelope))
    if isinstance(normalized["task"], type):
        normalized["task"] = class_path(normalized["task"])
    normalized.setdefault("channel", "default")
    normalized.setdefault("id", str(uuid.uuid4()))
    return Executor(config=settings.config()).execute(normalized, local=True)
