"""Persistent Python process of the Warm IPC Pool.

Started by the supervisor with one end of a socketpair (``--fd``). It loads the configuration and the
task modules once, then executes jobs one at a time until told to shut down or the supervisor goes away.
Redis reservation and acknowledgement stay with the engine that owns the claim; this process never
touches the queues.
"""

import argparse
import os
import re
import resource
import signal
import socket
import sys
import time

from ..errors import InvalidJobError
from . import protocol

_CLASS_PATH = re.compile(r"^[A-Za-z_][\w]*(\.[A-Za-z_][\w]*)*(:[A-Za-z_][\w.]*)?$")
_MAX_ERROR_CHARS = 4096


def _rss_mb():
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


def _error(request, error, retryable):
    message = str(error)[:_MAX_ERROR_CHARS]
    return {"v": protocol.PROTOCOL_VERSION, "type": "result", "request_id": request.get("request_id"),
            "job_id": request.get("job_id"), "status": "error",
            "error": {"class": type(error).__qualname__, "message": message, "retryable": retryable}}


class Worker:
    def __init__(self, sock, codec):
        self.sock = sock
        self.codec = codec
        self.executor = None
        self.allowed_modules = ()

    def send(self, message):
        protocol.send_frame(self.sock, self.codec.dumps(message))

    def initialize(self, init):
        from .. import settings
        from ..blueprint import Blueprint
        from ..executor import Executor

        for path in reversed(init.get("paths", [])):
            if path not in sys.path:
                sys.path.insert(0, path)
        config = Blueprint.from_snapshot(init["snapshot"])
        config.warm_socket = None
        settings.use_config(config)
        self.allowed_modules = tuple(init.get("allowed_modules") or ())
        self.executor = Executor(config=config)

    def authorize(self, job_class):
        from ..task import Task
        from ..utilities import constantize

        if not isinstance(job_class, str) or not _CLASS_PATH.match(job_class):
            raise InvalidJobError(f"invalid job class {job_class!r}")
        if self.allowed_modules and not any(
                job_class == module or job_class.startswith(f"{module}.") or job_class.startswith(f"{module}:")
                for module in self.allowed_modules):
            raise InvalidJobError(f"job class {job_class!r} is not allowed in the warm pool")
        target = constantize(job_class)
        if not (isinstance(target, type) and issubclass(target, Task)):
            raise InvalidJobError(f"{job_class!r} is not a SolidJobs task")

    def execute(self, request):
        started = time.monotonic()
        try:
            self.authorize(request.get("job_class"))
        except (InvalidJobError, NameError, ImportError, AttributeError) as error:
            return _error(request, error, retryable=False)
        envelope = {"id": request.get("job_id"), "task": request["job_class"],
                    "arguments": request.get("args"), "channel": request.get("channel")}
        try:
            self.executor.execute(envelope, local=True)
        except InvalidJobError as error:
            return _error(request, error, retryable=False)
        except Exception as error:
            return _error(request, error, retryable=True)
        return {"v": protocol.PROTOCOL_VERSION, "type": "result", "request_id": request.get("request_id"),
                "job_id": request.get("job_id"), "status": "ok",
                "duration_ms": int((time.monotonic() - started) * 1000), "rss_mb": round(_rss_mb(), 1)}

    def serve(self):
        while True:
            try:
                payload = protocol.recv_frame(self.sock)
            except (OSError, protocol.ProtocolError):
                return
            if payload is None:
                return
            try:
                request = protocol.decode(self.codec, payload)
            except protocol.ProtocolError as error:
                self.send(_error({}, error, retryable=False))
                continue
            kind = request.get("type")
            if kind == "execute":
                self.send(self.execute(request))
            elif kind == "ping":
                self.send({"v": protocol.PROTOCOL_VERSION, "type": "pong", "pid": os.getpid()})
            elif kind == "shutdown":
                return
            else:
                self.send(_error(request, protocol.ProtocolError(f"unknown message type {kind!r}"), False))


def main(argv=None):
    parser = argparse.ArgumentParser(prog="solid_jobs.warm.worker")
    parser.add_argument("--fd", type=int, required=True)
    parser.add_argument("--serializer", default="json")
    options = parser.parse_args(argv)

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    sock = socket.socket(fileno=options.fd)
    sock.setblocking(True)
    worker = Worker(sock, protocol.serializer(options.serializer))
    try:
        payload = protocol.recv_frame(sock)
        if payload is None:
            return 1
        init = protocol.decode(worker.codec, payload)
        worker.initialize(init)
    except BaseException as error:
        try:
            worker.send({"v": protocol.PROTOCOL_VERSION, "type": "failed",
                         "error": f"{type(error).__name__}: {error}"})
        except OSError:
            pass
        return 2
    worker.send({"v": protocol.PROTOCOL_VERSION, "type": "ready", "pid": os.getpid()})
    worker.serve()
    return 0


if __name__ == "__main__":
    sys.exit(main())
