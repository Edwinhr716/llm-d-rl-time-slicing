"""Helpers for building BackendConfig protos."""

from typing import Sequence, Tuple, Union

from . import snapshot_agent_pb2
from .types import MemoryRegion

RegionLike = Union[MemoryRegion, Tuple[int, int, int], str]


def memory_regions_config(
    regions: Sequence[RegionLike],
    snapshot_name: str = "",
) -> snapshot_agent_pb2.BackendConfig:
    """Builds a BackendConfig selecting the memory-regions backend.

    Accepts MemoryRegion dataclasses, (pid, address, size_bytes) tuples, or
    legacy 'pid:0xADDR:size' spec strings. Hex or decimal addresses are
    accepted; validation mirrors the agent's (non-empty regions, positive
    pid and size).

    snapshot_name names the agent-side snapshot slot (defaults to the
    request's job_id server-side when empty). Use it — not the request's
    `group` — for slot naming: group identifies a set of related jobs for
    the orchestrator and does not name agent-side storage.
    """
    if not regions:
        raise ValueError("at least one memory region is required")

    pb_regions = []
    for region in regions:
        if isinstance(region, MemoryRegion):
            r = region
        elif isinstance(region, str):
            r = MemoryRegion.from_spec(region)
        elif isinstance(region, tuple):
            if len(region) != 3:
                raise ValueError(
                    f"memory region tuple must be (pid, address, size_bytes), got {region!r}"
                )
            r = MemoryRegion(pid=region[0], address=region[1], size_bytes=region[2])
        else:
            raise TypeError(
                "memory region must be a MemoryRegion, (pid, address, size_bytes) "
                f"tuple, or 'pid:0xADDR:size' string, got {type(region).__name__}"
            )
        pb_regions.append(
            snapshot_agent_pb2.MemoryRegion(
                pid=r.pid, address=r.address, size_bytes=r.size_bytes
            )
        )

    return snapshot_agent_pb2.BackendConfig(
        memory_regions=snapshot_agent_pb2.MemoryRegionsBackendConfig(
            regions=pb_regions,
            snapshot_name=snapshot_name,
        )
    )
