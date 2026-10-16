import argparse
import importlib
import json
import logging
import signal
import sys
import threading
import tomllib
from pathlib import Path

from . import settings
from .conductor import Conductor
from .version import VERSION

SIGNALS = ("SIGINT", "SIGTERM", "SIGTSTP", "SIGTTIN", "SIGINFO")


def _channel(value):
    name, _, weight = str(value).partition(",")
    return (name, int(weight)) if weight else name


def _parser():
    parser = argparse.ArgumentParser(prog="solid-jobs", description="Run SolidJobs executors")
    parser.add_argument("-r", "--require", action="append", default=[],
                        help="module name or .py file defining your tasks (imported in every worker)")
    parser.add_argument("-C", "--config", help="TOML or JSON configuration file")
    parser.add_argument("-c", "--concurrency", type=int, help="worker subinterpreter count")
    parser.add_argument("--channel", action="append", default=[], help="channel name or name,weight")
    parser.add_argument("-t", "--timeout", type=float, help="graceful shutdown timeout in seconds")
    parser.add_argument("--redis-url", help="Redis URL used by the solid-redis sidecar")
    parser.add_argument("-v", "--version", action="version", version=f"solid-jobs {VERSION}")
    return parser


def _load_file(path):
    raw = Path(path).read_bytes()
    return json.loads(raw) if str(path).endswith(".json") else tomllib.loads(raw.decode("utf-8"))


def _import_requirement(requirement):
    path = Path(requirement)
    if requirement.endswith(".py") and path.exists():
        directory = str(path.resolve().parent)
        if directory not in sys.path:
            sys.path.insert(0, directory)
        requirement = path.stem
    importlib.import_module(requirement)
    return requirement


def main(argv=None):
    options = _parser().parse_args(argv)
    config = settings.config()
    values = _load_file(options.config) if options.config else {}

    url = options.redis_url or values.get("redis_url") or values.get("redis", {}).get("url")
    if url:
        config.redis_url = url
    concurrency = options.concurrency or values.get("concurrency")
    if concurrency:
        config.concurrency = concurrency
    channels = [_channel(c) for c in options.channel] or [_channel(c) for c in values.get("channels", [])]
    if channels:
        config.channels = channels
    timeout = options.timeout or values.get("timeout")
    if timeout:
        config.shutdown_timeout = timeout
    config.requires = [_import_requirement(r) for r in [*values.get("require", []), *options.require]]

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    server = Conductor(config=config)
    wake = threading.Event()
    state = {"signals": []}

    def handler(signum, _frame):
        state["signals"].append(signal.Signals(signum).name)
        wake.set()

    previous = {}
    for name in SIGNALS:
        if hasattr(signal, name):
            previous[name] = signal.signal(getattr(signal, name), handler)
    try:
        server.start()
        _wait_for_shutdown(server, wake, state)
    finally:
        for name, old in previous.items():
            signal.signal(getattr(signal, name), old)
        server.stop()
    return 0


def _wait_for_shutdown(server, wake, state):
    while True:
        wake.wait(0.5)
        wake.clear()
        while state["signals"]:
            if _handle(server, state["signals"].pop(0)):
                return
        remote = server.remote_signal()
        if remote and _handle(server, f"SIG{remote}"):
            return


def _handle(server, name):
    if name in ("SIGINT", "SIGTERM"):
        return True
    if name == "SIGTSTP":
        server.quiet()
    elif name in ("SIGTTIN", "SIGINFO"):
        for thread in threading.enumerate():
            logging.getLogger("solid_jobs").info("Thread %s alive=%s", thread.name, thread.is_alive())
    return False


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
