import importlib
import json
import logging
import os
import secrets
import socket

from . import gateway
from .errors import ConfigurationError
from .instrumentation import NullInstrumenter
from .interceptors import InterceptorRegistry
from .ipc import GatewayPool
from .warm import supervisor as warm_supervisor
from .warm.client import WarmClient
from .utilities import class_path, constantize, stringify_keys

DEFAULT_TASK_OPTIONS = {"channel": "default", "max_failures": 25}
CHANNEL_ORDERS = ("priority", "weighted", "shuffle")
EXECUTION_MODES = ("native", "warm", "auto")


def _normalize_channels(values):
    if isinstance(values, str):
        values = [values]
    entries = []
    for value in values:
        name, weight = value if isinstance(value, (list, tuple)) else (value, 1)
        name, weight = str(name), int(weight)
        if weight <= 0:
            raise ValueError("Channel weight must be positive")
        entries.append((name, weight))
    return entries


class Blueprint:
    """Runtime configuration. Redis is reached through the Ruby solid-redis sidecar."""

    def __init__(self, redis_url=None, concurrency=5, channels=("default",), gateway_socket=None):
        self.redis_url = redis_url or os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
        self.gateway_socket = gateway_socket or os.environ.get("SOLID_JOBS_GATEWAY_SOCKET")
        self.ruby_command = tuple(os.environ.get("SOLID_JOBS_RUBY", "ruby").split())
        self.concurrency = int(concurrency)
        self._channels = _normalize_channels(channels)
        self._default_task_options = dict(DEFAULT_TASK_OPTIONS)
        self.publish_interceptors = InterceptorRegistry()
        self.execute_interceptors = InterceptorRegistry()
        self.on_complex_arguments = "raise"
        self.poll_interval_average = 5.0
        self.shutdown_timeout = 25.0
        self._channel_order = "weighted"
        self.reliable_fetch = True
        self.retry_base_delay = 15.0
        self.retry_max_delay = 3_600.0
        self.discarded_limit = 10_000
        self.discarded_retention = 180 * 24 * 60 * 60
        self.logger = logging.getLogger("solid_jobs")
        self.identity = f"{socket.gethostname()}:{os.getpid()}:{secrets.token_hex(6)}"
        self.instrumenter = NullInstrumenter()
        self.requires = []
        self.warm_requires = []
        self._execution_default_mode = "native"
        self.execution_registry = {}
        self._warm_pool = dict(warm_supervisor.DEFAULTS)
        self.warm_socket = None
        self._warm_client = None
        self._callbacks = {}
        self._redis_pool = None

    @property
    def redis_pool(self):
        if self._redis_pool is None:
            path = self.gateway_socket or gateway.ensure(self.redis_url, self.ruby_command)
            self._redis_pool = GatewayPool(path)
        return self._redis_pool

    @property
    def warm_pool(self):
        return dict(self._warm_pool)

    @warm_pool.setter
    def warm_pool(self, options):
        self._warm_pool = warm_supervisor.normalize(dict(options))

    @property
    def execution_default_mode(self):
        return self._execution_default_mode

    @execution_default_mode.setter
    def execution_default_mode(self, mode):
        if mode not in ("native", "warm"):
            raise ValueError("execution_default_mode must be 'native' or 'warm'")
        self._execution_default_mode = mode

    def configure_execution(self, default_mode=None, registry=None, warm_pool=None):
        if default_mode is not None:
            self.execution_default_mode = default_mode
        for name, mode in (registry or {}).items():
            if mode not in ("native", "warm"):
                raise ValueError(f"registry mode for {name!r} must be 'native' or 'warm'")
            self.execution_registry[str(name)] = mode
        if warm_pool is not None:
            self.warm_pool = {**self._warm_pool, **warm_pool}
        return self

    @property
    def warm_client(self):
        if self._warm_client is None:
            if not self.warm_socket:
                return None
            options = self._warm_pool
            self._warm_client = WarmClient(self.warm_socket, options["serializer"], options["job_timeout"],
                                           int(options["max_message_bytes"]))
        return self._warm_client

    def close(self):
        if self._warm_client is not None:
            self._warm_client.close()
            self._warm_client = None
        if self._redis_pool is not None:
            self._redis_pool.close()
            self._redis_pool = None
        return self

    @property
    def channels(self):
        return [name for name, _ in self._channels]

    @channels.setter
    def channels(self, values):
        self._channels = _normalize_channels(values)

    @property
    def channel_entries(self):
        return list(self._channels)

    @property
    def channel_order(self):
        return self._channel_order

    @channel_order.setter
    def channel_order(self, mode):
        if mode not in CHANNEL_ORDERS:
            raise ValueError("channel_order must be 'priority', 'weighted', or 'shuffle'")
        self._channel_order = mode

    @property
    def default_task_options(self):
        return dict(self._default_task_options)

    @default_task_options.setter
    def default_task_options(self, options):
        self._default_task_options = {**DEFAULT_TASK_OPTIONS, **stringify_keys(options)}

    def on(self, event, callback):
        self._callbacks.setdefault(event, []).append(callback)
        return self

    def fire(self, event, *arguments):
        for callback in self._callbacks.get(event, ()):
            callback(*arguments)

    def __repr__(self):
        return f"<Blueprint concurrency={self.concurrency} channels={self.channels!r}>"

    def snapshot(self, for_warm_worker=False):
        """JSON string a subinterpreter (or warm worker process) uses to rebuild an equivalent configuration.

        A warm worker runs jobs locally and imports only ``warm_requires``; engines import ``requires``.
        """
        if self.gateway_socket is None:
            self.gateway_socket = gateway.ensure(self.redis_url, self.ruby_command)
        return json.dumps({
            "redis_url": self.redis_url,
            "gateway_socket": self.gateway_socket,
            "concurrency": self.concurrency,
            "channels": self._channels,
            "channel_order": self._channel_order,
            "reliable_fetch": self.reliable_fetch,
            "default_task_options": self._default_task_options,
            "on_complex_arguments": self.on_complex_arguments,
            "poll_interval_average": self.poll_interval_average,
            "shutdown_timeout": self.shutdown_timeout,
            "discarded_limit": self.discarded_limit,
            "discarded_retention": self.discarded_retention,
            "retry_base_delay": self.retry_base_delay,
            "retry_max_delay": self.retry_max_delay,
            "publish_interceptors": self.publish_interceptors.export(),
            "execute_interceptors": self.execute_interceptors.export(),
            "identity": self.identity,
            "instrumenter": class_path(type(self.instrumenter)),
            "requires": list(self.warm_requires if for_warm_worker else self.requires),
            "execution_default_mode": self._execution_default_mode,
            "execution_registry": self.execution_registry,
            "warm_pool": self._warm_pool,
            "warm_socket": None if for_warm_worker else self.warm_socket,
            "log_level": self.logger.getEffectiveLevel(),
        })

    @classmethod
    def from_snapshot(cls, raw):
        data = json.loads(raw)
        config = cls(
            redis_url=data["redis_url"], concurrency=data["concurrency"],
            channels=data["channels"], gateway_socket=data["gateway_socket"],
        )
        config.channel_order = data["channel_order"]
        for name in ("reliable_fetch", "on_complex_arguments", "poll_interval_average", "shutdown_timeout",
                     "discarded_limit", "discarded_retention", "retry_base_delay", "retry_max_delay"):
            setattr(config, name, data[name])
        config.default_task_options = data["default_task_options"]
        config.identity = data["identity"]
        config.requires = data["requires"]
        config.execution_default_mode = data["execution_default_mode"]
        config.execution_registry = data["execution_registry"]
        config.warm_pool = data["warm_pool"]
        config.warm_socket = data["warm_socket"]
        config.logger.setLevel(data["log_level"])
        for module in config.requires:
            importlib.import_module(module)
        config.publish_interceptors.import_(data["publish_interceptors"])
        config.execute_interceptors.import_(data["execute_interceptors"])
        try:
            config.instrumenter = constantize(data["instrumenter"])()
        except Exception as error:
            raise ConfigurationError(f"instrumenter must be a no-argument class: {error}") from error
        return config
