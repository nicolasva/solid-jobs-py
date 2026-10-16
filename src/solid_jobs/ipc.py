"""Client side of the Unix-socket protocol spoken by solid_redis_gateway.rb.

Python never speaks RESP: commands go out as JSON lines to the Ruby sidecar,
which executes them through the ``solid-redis`` gem. Each thread owns one
socket (and therefore one Redis connection inside the sidecar).
"""

import json
import socket
import threading

from .errors import RedisCommandError, RedisConnectionError

_ERRORS = {"CommandError": RedisCommandError, "AuthenticationError": RedisCommandError}


def _argument(value):
    if isinstance(value, bool):
        raise TypeError("Redis arguments cannot be booleans")
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    return str(repr(value)) if isinstance(value, float) else str(value)


def _command(command):
    if not command:
        raise ValueError("empty Redis command")
    return [_argument(part) for part in command]


class _Connection:
    def __init__(self, path, timeout):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.settimeout(timeout)
        self.socket.connect(path)
        self.stream = self.socket.makefile("rwb")

    def request(self, payload):
        self.stream.write(json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n")
        self.stream.flush()
        line = self.stream.readline()
        if not line:
            raise ConnectionError("gateway closed the connection")
        return json.loads(line)

    def close(self):
        for closable in (self.stream, self.socket):
            try:
                closable.close()
            except OSError:
                pass


class Pipeline:
    def __init__(self):
        self.commands = []
        self.results = []

    def call(self, *command):
        self.commands.append(_command(command))


class _PipelineContext:
    def __init__(self, pool):
        self.pool = pool
        self.pipeline = Pipeline()

    def __enter__(self):
        return self.pipeline

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None and self.pipeline.commands:
            self.pipeline.results = self.pool._send({"pipeline": self.pipeline.commands})
        return False


class GatewayPool:
    """Thread-safe: one gateway connection per calling thread."""

    def __init__(self, socket_path, timeout=30.0):
        self.socket_path = socket_path
        self.timeout = timeout
        self._local = threading.local()
        self._lock = threading.Lock()
        self._connections = set()
        self._closed = False

    def call(self, *command):
        parts = _command(command)
        return self._send({"command": parts[0], "args": parts[1:]})

    def pipelined(self):
        return _PipelineContext(self)

    def close(self):
        with self._lock:
            self._closed = True
            connections, self._connections = self._connections, set()
        for connection in connections:
            connection.close()
        self._local = threading.local()

    def _connection(self):
        connection = getattr(self._local, "connection", None)
        if connection is None:
            if self._closed:
                raise RedisConnectionError("Redis pool is closed")
            try:
                connection = _Connection(self.socket_path, self.timeout)
            except OSError as error:
                raise RedisConnectionError(f"cannot reach gateway at {self.socket_path}: {error}") from error
            self._local.connection = connection
            with self._lock:
                self._connections.add(connection)
        return connection

    def _discard(self, connection):
        self._local.connection = None
        with self._lock:
            self._connections.discard(connection)
        connection.close()

    def _send(self, payload):
        connection = self._connection()
        try:
            reply = connection.request(payload)
        except (OSError, ValueError, ConnectionError) as error:
            self._discard(connection)
            raise RedisConnectionError(f"gateway request failed: {error}") from error
        if reply.get("status") == "OK":
            return reply.get("data")
        error = reply.get("error") or {}
        message = error.get("message", "unknown gateway error")
        if error.get("class") in ("ConnectionError", "TimeoutError", "FailoverError", "CheckoutTimeoutError"):
            raise RedisConnectionError(message)
        raise _ERRORS.get(error.get("class"), RedisCommandError)(message)
