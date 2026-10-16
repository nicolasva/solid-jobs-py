"""Framing and serialization shared by the Warm IPC Pool supervisor, client and worker.

Wire format: a 4-byte big-endian payload size followed by the serialized message. The protocol is
independent of Redis: it only carries job execution requests and their results.
"""

import json
import struct

from ..errors import Error

PROTOCOL_VERSION = 1
MAX_MESSAGE_BYTES = 16 * 1024 * 1024
_HEADER = struct.Struct(">I")


class ProtocolError(Error):
    pass


class JsonSerializer:
    name = "json"

    @staticmethod
    def dumps(message):
        return json.dumps(message, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def loads(raw):
        return json.loads(raw)


class MsgpackSerializer:
    name = "msgpack"

    def __init__(self):
        try:
            import msgpack
        except ImportError as error:
            raise ProtocolError("serializer 'msgpack' requires the 'msgpack' package") from error
        self._msgpack = msgpack

    def dumps(self, message):
        return self._msgpack.packb(message, use_bin_type=True)

    def loads(self, raw):
        return self._msgpack.unpackb(raw, raw=False, strict_map_key=False)


def serializer(name):
    if name == "json":
        return JsonSerializer()
    if name == "msgpack":
        return MsgpackSerializer()
    raise ProtocolError(f"unknown serializer {name!r}; expected 'json' or 'msgpack'")


def _recv_exact(sock, size):
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(min(remaining, 1 << 20))
        if not chunk:
            return None if remaining == size else _truncated()
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _truncated():
    raise ProtocolError("connection closed in the middle of a message")


def send_frame(sock, payload, max_bytes=MAX_MESSAGE_BYTES):
    if len(payload) > max_bytes:
        raise ProtocolError(f"message of {len(payload)} bytes exceeds the {max_bytes} bytes limit")
    sock.sendall(_HEADER.pack(len(payload)) + payload)


def recv_frame(sock, max_bytes=MAX_MESSAGE_BYTES):
    """Returns the next payload, or ``None`` when the peer closed the connection cleanly."""
    header = _recv_exact(sock, _HEADER.size)
    if header is None:
        return None
    (size,) = _HEADER.unpack(header)
    if size > max_bytes:
        raise ProtocolError(f"incoming message of {size} bytes exceeds the {max_bytes} bytes limit")
    payload = _recv_exact(sock, size) if size else b""
    if payload is None:
        _truncated()
    return payload


def decode(codec, payload):
    try:
        message = codec.loads(payload)
    except Exception as error:
        raise ProtocolError(f"invalid message: {error}") from error
    if not isinstance(message, dict) or message.get("v") != PROTOCOL_VERSION:
        raise ProtocolError("unsupported or malformed message")
    return message
