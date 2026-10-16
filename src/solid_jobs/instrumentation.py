import json

from .errors import ExecutionHalt

EVENTS = {
    "enqueued": "job.enqueued",
    "reserved": "job.reserved",
    "journaled": "job.journaled",
    "started": "job.started",
    "completed": "job.completed",
    "failed": "job.failed",
    "retry_scheduled": "job.retry_scheduled",
    "acknowledged": "job.acknowledged",
    "recovered": "job.recovered",
    "dead": "job.dead",
}


class NullInstrumenter:
    def instrument(self, name, payload=None):
        return None


def emit(config, event, envelope, **attributes):
    """Publish a lifecycle event to ``config.instrumenter``; never raises (except ExecutionHalt)."""
    try:
        if isinstance(envelope, (str, bytes)):
            envelope = json.loads(envelope)
        payload = {
            "job_id": envelope.get("id"),
            "queue": envelope.get("channel"),
            "job_class": envelope.get("task"),
            "node_id": config.identity,
            **attributes,
        }
        config.instrumenter.instrument(EVENTS[event], {k: v for k, v in payload.items() if v is not None})
        return True
    except ExecutionHalt:
        raise
    except Exception as error:
        config.logger.warning("SolidJobs telemetry failed: %s: %s", type(error).__name__, error)
        return False


def worker_attributes(processor_id):
    if processor_id is None:
        return {}
    return {"worker_id": processor_id, "interpreter_id": processor_id}
