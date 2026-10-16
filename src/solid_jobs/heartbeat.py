import json
import os
import socket
import time

from . import keyspace

INTERVAL = 5
TTL = 60


class Heartbeat:
    """Publishes node liveness, busy executors and processed/failed counters."""

    def __init__(self, config, identity, started_at, concurrency):
        self.config = config
        self.identity = identity
        self.started_at = started_at
        self.concurrency = concurrency

    def beat(self, quiet=False, work=None, processed=0, failed=0):
        work = work or {}
        now = time.time()
        info = {
            "hostname": socket.gethostname(), "pid": os.getpid(), "started_at": self.started_at,
            "channels": self.config.channels, "labels": [], "identity": self.identity,
        }
        node_key = keyspace.node(self.identity)
        work_key = keyspace.node_work(self.identity)
        busy = self.config.redis_pool.call("HLEN", work_key)
        with self.config.redis_pool.pipelined() as pipeline:
            if processed > 0:
                pipeline.call("INCRBY", keyspace.PROCESSED, processed)
            if failed > 0:
                pipeline.call("INCRBY", keyspace.FAILED, failed)
            pipeline.call("SADD", keyspace.NODES, self.identity)
            pipeline.call("HSET", node_key, "info", json.dumps(info), "beat", now, "busy", busy,
                          "quiet", "true" if quiet else "false", "concurrency", self.concurrency)
            pipeline.call("EXPIRE", node_key, TTL)
            pipeline.call("DEL", work_key)
            if work:
                pipeline.call("HSET", work_key, *[part for item in work.items() for part in item])
            pipeline.call("EXPIRE", work_key, TTL)
        return now

    def cleanup(self):
        try:
            with self.config.redis_pool.pipelined() as pipeline:
                pipeline.call("SREM", keyspace.NODES, self.identity)
                pipeline.call("DEL", keyspace.node(self.identity))
                pipeline.call("DEL", keyspace.node_work(self.identity))
        finally:
            self.config.close()
