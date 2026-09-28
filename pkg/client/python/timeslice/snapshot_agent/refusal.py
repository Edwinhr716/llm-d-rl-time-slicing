"""Read the ErrorReason of a refused snapshot-agent Suspend, Resume or Kill call.

The agent puts the reason in the gRPC status details, as one google.rpc.ErrorInfo
whose reason is the ErrorReason enum name (for example "STALE_EPOCH") and whose
domain is DOMAIN. A refusal without a reason has no ErrorInfo. The status message
is free text.

The details are read straight from the "grpc-status-details-bin" trailer rather
than with grpc_status.rpc_status.from_call, which raises when the trailer's
message differs from the call's.

This module imports nothing from the timeslice package, so it can be loaded on
its own.
"""

import grpc
from google.protobuf.message import DecodeError
from google.rpc import error_details_pb2, status_pb2

DOMAIN = "snapshot-agent.llm-d-rl-time-slicing"
_DETAILS_KEY = "grpc-status-details-bin"


def reason_of(err: grpc.RpcError) -> str:
    """Return the ErrorReason name carried by err (for example "STALE_EPOCH").

    Returns "" when err carries none.
    """
    trailing_metadata = getattr(err, "trailing_metadata", None)
    if not callable(trailing_metadata):
        return ""
    for key, value in trailing_metadata() or ():
        if key != _DETAILS_KEY:
            continue
        status = status_pb2.Status()
        try:
            status.ParseFromString(value)
        except DecodeError:
            return ""
        for detail in status.details:
            info = error_details_pb2.ErrorInfo()
            if detail.Unpack(info) and info.domain == DOMAIN:
                return info.reason
    return ""
