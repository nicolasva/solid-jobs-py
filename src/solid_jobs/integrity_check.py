"""Consistency audit: classifies every task id found in Redis and reports anomalies."""

import json
from collections import defaultdict
from dataclasses import dataclass, field

from . import keyspace, settings

ACTIVE_COLLECTIONS = {keyspace.PLANNED: "PLANNED", keyspace.RETRIES: "RETRYING",
                      keyspace.DISCARDED: "DISCARDED"}


@dataclass
class Report:
    lost: list = field(default_factory=list)
    orphaned: list = field(default_factory=list)
    invalid_reservations: list = field(default_factory=list)
    dangling_indexes: list = field(default_factory=list)
    duplicates: list = field(default_factory=list)
    malformed_payloads: list = field(default_factory=list)
    states: dict = field(default_factory=dict)

    def ok(self):
        return not (self.lost or self.orphaned or self.invalid_reservations or self.dangling_indexes
                    or self.duplicates or self.malformed_payloads)


class IntegrityCheck:
    def __init__(self, expected_job_ids, acked_job_ids=(), acked_key=None, config=None):
        self.config = config or settings.config()
        self.redis = self.config.redis_pool
        self.expected = {str(i) for i in expected_job_ids}
        self.acked = {str(i) for i in acked_job_ids}
        if acked_key:
            self.acked.update(self.redis.call("SMEMBERS", acked_key) or [])
        self.states = defaultdict(list)
        self.malformed = []
        self.invalid = []

    @classmethod
    def call(cls, **kwargs):
        return cls(**kwargs).run()

    def run(self):
        self._read_channels()
        self._read_timed_collections()
        claimed = self._read_claimed()
        self._validate_claim_metadata(claimed)
        for job_id in self.acked:
            self._add_state(job_id, "ACKED", "acknowledged")
        observed = set(self.states)
        return Report(
            lost=sorted(self.expected - observed), orphaned=sorted(observed - self.expected),
            invalid_reservations=self.invalid, dangling_indexes=self._dangling_attempts(),
            duplicates=[{"job_id": j, "locations": l} for j, l in self.states.items() if len(l) > 1],
            malformed_payloads=self.malformed, states=dict(self.states))

    def _read_channels(self):
        for key in self._scan(f"{keyspace.PREFIX}:channel:*"):
            for index, raw in enumerate(self.redis.call("LRANGE", key, 0, -1) or []):
                self._add_payload(raw, "READY", f"{key}[{index}]")

    def _read_timed_collections(self):
        for key, state in ACTIVE_COLLECTIONS.items():
            for index, raw in enumerate(self.redis.call("ZRANGE", key, 0, -1) or []):
                self._add_payload(raw, state, f"{key}[{index}]")

    def _read_claimed(self):
        claimed = {}
        for key in self._scan(f"{keyspace.PREFIX}:node:*:claimed:*"):
            records = []
            for index, raw in enumerate(self.redis.call("LRANGE", key, 0, -1) or []):
                payload = self._parse_payload(raw, f"{key}[{index}]")
                if payload:
                    self._add_state(payload["id"], "CLAIMED", f"{key}[{index}]")
                    records.append(payload)
            claimed[key] = records
        return claimed

    def _validate_claim_metadata(self, claimed):
        metadata = self._claim_metadata()
        for key, payloads in claimed.items():
            node_key, executor_id = key.split(":claimed:", 1)
            identity = node_key[len(f"{keyspace.PREFIX}:node:"):]
            entry = metadata.pop((identity, executor_id), None)
            if not payloads:
                self.invalid.append(self._issue(key, "empty_claimed_list"))
            elif len(payloads) > 1:
                self.invalid.append(self._issue(key, "multiple_tasks_for_executor"))
            payload = payloads[0] if payloads else None
            if entry is None:
                self.invalid.append(self._issue(key, "missing_metadata",
                                                job_id=payload.get("id") if payload else None))
                continue
            if payload is None:
                continue
            expected = {"task_id": payload["id"], "node_id": identity, "executor_id": executor_id,
                        "channel": payload.get("channel")}
            for name, value in expected.items():
                actual = entry["attributes"].get(name)
                if str(actual) != str(value):
                    self.invalid.append(self._issue(key, "metadata_mismatch", field=name,
                                                    expected=value, actual=actual))
        for entry in metadata.values():
            self.invalid.append(self._issue(entry["key"], "metadata_without_payload", field=entry["field"],
                                            job_id=entry["attributes"].get("task_id")))

    def _claim_metadata(self):
        entries = {}
        for key in self._scan(f"{keyspace.PREFIX}:node:*:claims"):
            identity = key[len(f"{keyspace.PREFIX}:node:"):-len(":claims")]
            for name, raw in self._hash(key).items():
                attributes = self._parse_json(raw, f"{key}[{name}]")
                if attributes:
                    entries[(identity, name)] = {"key": key, "field": name, "attributes": attributes}
        return entries

    def _dangling_attempts(self):
        dangling = []
        for job_id, attempt in self._hash(keyspace.ATTEMPTS).items():
            if any(location["state"] != "ACKED" for location in self.states.get(job_id, ())):
                continue
            dangling.append({"job_id": job_id, "attempt": int(attempt), "index": keyspace.ATTEMPTS})
        return dangling

    def _add_payload(self, raw, state, location):
        payload = self._parse_payload(raw, location)
        if payload:
            self._add_state(payload["id"], state, location)

    def _parse_payload(self, raw, location):
        payload = self._parse_json(raw, location)
        if payload is None:
            return None
        if isinstance(payload, dict) and payload.get("id"):
            return payload
        self.malformed.append({"location": location, "error": "missing id", "raw": raw})
        return None

    def _parse_json(self, raw, location):
        try:
            return json.loads(raw)
        except ValueError as error:
            self.malformed.append({"location": location, "error": str(error), "raw": raw})
            return None

    def _add_state(self, job_id, state, location):
        self.states[str(job_id)].append({"state": state, "location": location})

    def _hash(self, key):
        flat = self.redis.call("HGETALL", key) or []
        return dict(zip(flat[0::2], flat[1::2]))

    def _scan(self, pattern):
        cursor, keys = "0", []
        while True:
            cursor, batch = self.redis.call("SCAN", cursor, "MATCH", pattern, "COUNT", 100)
            keys.extend(batch)
            if cursor == "0":
                return keys

    @staticmethod
    def _issue(key, reason, **details):
        return {"key": key, "reason": reason, **details}
