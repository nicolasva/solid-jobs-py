PREFIX = "solid_jobs"
CHANNELS = f"{PREFIX}:channels"
PAUSED_CHANNELS = f"{PREFIX}:paused_channels"
PLANNED = f"{PREFIX}:planned"
RETRIES = f"{PREFIX}:retries"
DISCARDED = f"{PREFIX}:discarded"
NODES = f"{PREFIX}:nodes"
ATTEMPTS = f"{PREFIX}:attempts"
PUBLICATION_PREFIX = f"{PREFIX}:publication:"
PROCESSED = f"{PREFIX}:metrics:processed"
FAILED = f"{PREFIX}:metrics:failed"
CHANNEL_PREFIX = f"{PREFIX}:channel:"


def channel(name):
    return f"{CHANNEL_PREFIX}{name}"


def publication(digest):
    return f"{PUBLICATION_PREFIX}{digest}"


def node(identity):
    return f"{PREFIX}:node:{identity}"


def node_work(identity):
    return f"{node(identity)}:work"


def node_signals(identity):
    return f"{node(identity)}:signals"


def claims(identity):
    return f"{node(identity)}:claims"


def claimed(identity, executor_id):
    return f"{node(identity)}:claimed:{executor_id}"
