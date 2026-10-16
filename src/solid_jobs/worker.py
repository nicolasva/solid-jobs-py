"""Entry point executed inside each worker subinterpreter (PEP 734, own GIL)."""

import logging
import queue
import sys
import threading

WORKER_CODE = (
    "from solid_jobs import worker\n"
    "worker.main(snapshot, processor_id, control, heartbeat, ready, paths)\n"
)


def main(snapshot, processor_id, control, heartbeat, ready, paths):
    import json

    for path in reversed(json.loads(paths)):
        if path not in sys.path:
            sys.path.insert(0, path)

    from . import settings
    from .blueprint import Blueprint
    from .engine import Engine

    config = engine = None
    try:
        config = Blueprint.from_snapshot(snapshot)
        if not logging.getLogger().handlers:
            logging.basicConfig(level=config.logger.getEffectiveLevel(),
                                format=f"%(asctime)s %(levelname)s [worker-{processor_id}] %(message)s")
        settings.use_config(config)
        engine = Engine(config, identity=config.identity, processor_id=processor_id, heartbeat=heartbeat)
        config.redis_pool.call("PING")
    except BaseException as error:
        ready.put(("failed", processor_id, f"{type(error).__name__}: {error}"))
        if config is not None:
            config.close()
        return
    ready.put(("ready", processor_id, ""))

    commands = queue.Queue()
    finished = threading.Event()

    def listen():
        while not finished.is_set():
            try:
                command = control.get_nowait()
            except queue.Empty:
                # Queue.get(timeout=...) busy-spins on 3.14 and starves the executor thread.
                finished.wait(0.05)
                continue
            commands.put(command)
            if command == "stop":
                if engine.busy() and not finished.wait(config.shutdown_timeout):
                    engine.interrupt_current()
                return

    listener = threading.Thread(target=listen)
    listener.start()
    try:
        engine.run(commands)
    finally:
        finished.set()
        listener.join(1)
