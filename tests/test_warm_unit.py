"""Unit tests for the Warm IPC Pool that need neither Redis nor worker processes."""
import socket
import threading
import types
import unittest

from solid_jobs import errors, routing
from solid_jobs.warm import normalize, protocol
from solid_jobs.warm.worker import Worker


def config(registry=None, default="native", client=None):
    return types.SimpleNamespace(execution_registry=registry or {}, execution_default_mode=default,
                                 warm_client=client)


class ProtocolTest(unittest.TestCase):
    def setUp(self):
        self.a, self.b = socket.socketpair()
        self.addCleanup(self.a.close)
        self.addCleanup(self.b.close)

    def test_frame_roundtrip(self):
        for payload in (b"", b"x", b"y" * 200_000):
            sender = threading.Thread(target=protocol.send_frame, args=(self.a, payload))
            sender.start()
            self.assertEqual(protocol.recv_frame(self.b), payload)
            sender.join()

    def test_clean_close_returns_none(self):
        self.a.close()
        self.assertIsNone(protocol.recv_frame(self.b))

    def test_truncated_frame_is_an_error(self):
        self.a.sendall(b"\x00\x00\x00\x0aabc")
        self.a.close()
        with self.assertRaises(protocol.ProtocolError):
            protocol.recv_frame(self.b)

    def test_size_limits_both_directions(self):
        with self.assertRaises(protocol.ProtocolError):
            protocol.send_frame(self.a, b"x" * 11, max_bytes=10)
        self.a.sendall(b"\xff\xff\xff\xff")
        with self.assertRaises(protocol.ProtocolError):
            protocol.recv_frame(self.b, max_bytes=1024)

    def test_json_codec_and_decode(self):
        codec = protocol.serializer("json")
        message = {"v": 1, "type": "ping", "list": [1, "é", None]}
        self.assertEqual(protocol.decode(codec, codec.dumps(message)), message)

    def test_decode_rejects_bad_messages(self):
        codec = protocol.serializer("json")
        for raw in (b"not json", b"[1]", b'{"v": 2}', b"{}"):
            with self.assertRaises(protocol.ProtocolError):
                protocol.decode(codec, raw)

    def test_unknown_serializer(self):
        with self.assertRaises(protocol.ProtocolError):
            protocol.serializer("pickle")

    def test_msgpack_codec_if_available(self):
        try:
            import msgpack  # noqa: F401
        except ImportError:
            self.skipTest("msgpack not installed")
        codec = protocol.serializer("msgpack")
        message = {"v": 1, "args": [1, 2.5, "x", b"raw"]}
        self.assertEqual(protocol.decode(codec, codec.dumps(message)), message)


class NormalizeTest(unittest.TestCase):
    def test_defaults_and_alias(self):
        self.assertEqual(normalize({"min_workers": 3})["workers"], 3)
        self.assertNotIn("min_workers", normalize({"min_workers": 3}))
        self.assertGreaterEqual(normalize({})["workers"], 1)

    def test_rejects_invalid_options(self):
        for options in ({"bogus": 1}, {"workers": 0}, {"serializer": "pickle"}):
            with self.assertRaises((ValueError, protocol.ProtocolError)):
                normalize(options)


class ResolveTest(unittest.TestCase):
    def test_envelope_mode_wins(self):
        env = {"task": "a.B", "execution_mode": "warm"}
        self.assertEqual(routing.resolve(env, config({"a.B": "native"})), "warm")

    def test_auto_uses_registry_then_default(self):
        env = {"task": "a.B"}
        self.assertEqual(routing.resolve(env, config({"a.B": "warm"})), "warm")
        self.assertEqual(routing.resolve(env, config(default="warm")), "warm")
        self.assertEqual(routing.resolve(env, config()), "native")

    def test_invalid_mode(self):
        with self.assertRaises(errors.InvalidJobError):
            routing.resolve({"task": "a.B", "execution_mode": "fast"}, config())

    def test_execute_warm_requires_a_pool(self):
        with self.assertRaises(errors.ConfigurationError):
            routing.execute_warm(config(), {"task": "a.B"})

    def test_execute_warm_delegates_to_client(self):
        client = types.SimpleNamespace(execute=lambda envelope: ("ran", envelope["task"]))
        self.assertEqual(routing.execute_warm(config(client=client), {"task": "a.B"}), ("ran", "a.B"))


class AuthorizeTest(unittest.TestCase):
    def worker(self, allowed=()):
        w = Worker.__new__(Worker)
        w.allowed_modules = tuple(allowed)
        return w

    def test_rejects_malformed_class_paths(self):
        for bad in (None, 3, "", "os.system; rm", "../x", "a b"):
            with self.assertRaises(errors.InvalidJobError):
                self.worker().authorize(bad)

    def test_rejects_modules_outside_allowlist(self):
        with self.assertRaises(errors.InvalidJobError):
            self.worker(["myapp"]).authorize("os.path")
        with self.assertRaises(errors.InvalidJobError):
            self.worker(["myapp"]).authorize("myapplication.Job")

    def test_rejects_non_task_classes(self):
        with self.assertRaises(errors.InvalidJobError):
            self.worker().authorize("collections.OrderedDict")


if __name__ == "__main__":
    unittest.main()
