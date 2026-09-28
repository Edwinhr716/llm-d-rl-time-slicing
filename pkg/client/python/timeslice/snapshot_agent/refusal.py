"""Read the ErrorReason of a refused snapshot-agent Suspend, Resume or Kill call.

The agent puts the reason's enum name at the start of the gRPC status message,
followed by ": " (for example "STALE_EPOCH: epoch 3 < 5"). A refusal without a
reason has no prefix.

This module imports nothing from the timeslice package, so it can be loaded on
its own.
"""

import grpc

# The ErrorReason names the agent can put in the prefix. They mirror the
# ErrorReason enum of the snapshot-agent API, without ERROR_REASON_UNSPECIFIED,
# which the agent never writes.
_REASONS = (
    "DEADLINE_INFEASIBLE",
    "DEADLINE_EXCEEDED",
    "STALE_EPOCH",
    "PRECONDITION_READINESS",
    "PRECONDITION_PROBES",
    "PRECONDITION_MEMORY",
    "PRECONDITION_NODE",
    "BACKEND_ERROR",
    "VERIFY_FAILED",
    "KILL_UNCONFIRMED",
)


def reason_of(err: grpc.RpcError) -> str:
    """Return the ErrorReason name carried by err (for example "STALE_EPOCH").

    Returns "" when err carries none.
    """
    details = getattr(err, "details", None)
    if not callable(details):
        return ""
    message = details() or ""
    for name in _REASONS:
        if message.startswith(name + ": "):
            return name
    return ""
