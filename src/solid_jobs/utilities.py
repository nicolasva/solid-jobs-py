import hashlib
import importlib
import json
import time


def stringify_keys(value):
    if isinstance(value, dict):
        return {str(key): stringify_keys(nested) for key, nested in value.items()}
    if isinstance(value, (list, tuple)):
        return [stringify_keys(nested) for nested in value]
    return value


def dump_json(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def class_path(cls):
    return f"{cls.__module__}.{cls.__qualname__}"


def constantize(name):
    """Resolve ``package.module.Class`` (or ``module:Class.Nested``) to an object."""
    if ":" in name:
        module_name, _, attribute = name.partition(":")
        target = importlib.import_module(module_name)
        for part in attribute.split("."):
            target = getattr(target, part)
        return target
    parts = name.split(".")
    for split in range(len(parts) - 1, 0, -1):
        try:
            target = importlib.import_module(".".join(parts[:split]))
        except ModuleNotFoundError as error:
            if error.name and not ".".join(parts[:split]).startswith(error.name):
                raise
            continue
        try:
            for part in parts[split:]:
                target = getattr(target, part)
        except AttributeError:
            continue
        return target
    raise NameError(f"Cannot resolve {name!r}")


def realtime_milliseconds():
    return time.time_ns() // 1_000_000


def sha1(script):
    return hashlib.sha1(script.encode("utf-8")).hexdigest()


def run_script(pool, script, sha, keys, *arguments):
    """EVALSHA with an EVAL fallback when the server does not know the script yet."""
    from .errors import RedisCommandError

    try:
        return pool.call("EVALSHA", sha, len(keys), *keys, *arguments)
    except RedisCommandError as error:
        if "NOSCRIPT" not in str(error):
            raise
        return pool.call("EVAL", script, len(keys), *keys, *arguments)
