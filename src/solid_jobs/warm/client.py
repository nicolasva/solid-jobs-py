"""Engine-side client of the Warm IPC Pool broker (one connection per calling thread)."""

import select
import socket
import threading
import time
import uuid

from ..errors import InvalidJobError, WarmExecutionError, WarmPoolError
from . import protocol

POLL = 0.25  # short socket timeout so an asynchronous ExecutionHalt can interrupt the wait


class WarmClient:
    def __init__(self, socket_path, serializer="json", job_timeout=300.0, max_bytes=protocol.MAX_MESSAGE_BYTES):
        self.socket_path = socket_path
        self.codec = protocol.serializer(serializer)
        self.job_timeout = float(job_timeout)
        self.max_bytes = max_bytes
        self._local = threading.local()

    def execute(self, envelope):
        connection = self._connection()
        request_id = uuid.uuid4().hex
        request = {"v": protocol.PROTOCOL_VERSION, "type": "execute", "request_id": request_id,
                   "job_id": envelope.get("id"), "job_class": envelope["task"], "args": envelope["arguments"],
                   "channel": envelope["channel"], "timeout_ms": int(self.job_timeout * 1000)}
        try:
            protocol.send_frame(connection, self.codec.dumps(request), self.max_bytes)
            raw = self._receive(connection, time.monotonic() + self.job_timeout + 15.0)
        except BaseException as error:
            # Includes ExecutionHalt: closing the connection makes the supervisor drop the abandoned job.
            self._discard(connection)
            if isinstance(error, (OSError, protocol.ProtocolError, TypeError, ValueError)):
                raise WarmPoolError(f"warm pool request failed: {error}") from error
            raise
        reply = protocol.decode(self.codec, raw)
        if reply.get("request_id") != request_id:
            self._discard(connection)
            raise WarmPoolError("mismatched request_id in warm pool reply")
        if reply.get("status") == "ok":
            return reply
        details = reply.get("error") or {}
        message = f"{details.get('class', 'Error')}: {details.get('message', 'warm execution failed')}"
        if details.get("retryable") is False:
            raise InvalidJobError(message)
        raise WarmExecutionError(message, remote_class=details.get("class"))

    def close(self):
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            self._discard(connection)

    def _receive(self, connection, deadline):
        # Wait for the first byte in short slices (so ExecutionHalt can interrupt), then read the frame blocking.
        while not select.select([connection], [], [], POLL)[0]:
            if time.monotonic() >= deadline:
                raise WarmPoolError("warm pool did not answer in time")
        connection.settimeout(30.0)
        raw = protocol.recv_frame(connection, self.max_bytes)
        if raw is None:
            raise WarmPoolError("warm pool closed the connection")
        return raw

    def _connection(self):
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                connection.connect(self.socket_path)
            except OSError as error:
                connection.close()
                raise WarmPoolError(f"cannot reach warm pool at {self.socket_path}: {error}") from error
            self._local.connection = connection
        connection.settimeout(30.0)
        return connection

    def _discard(self, connection):
        self._local.connection = None
        try:
            connection.close()
        except OSError:
            pass
