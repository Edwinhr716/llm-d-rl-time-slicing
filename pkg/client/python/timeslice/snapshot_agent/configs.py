"""Helpers for building BackendConfig protos."""

from typing import Sequence, Tuple, Union

from . import snapshot_agent_pb2
from .types import MemoryRegion

RegionLike = Union[MemoryRegion, Tuple[int, int, int], str]


def _process_target(pids: Sequence[int]) -> snapshot_agent_pb2.ProcessTarget:
    if not pids:
        raise ValueError("at least one PID is required")
    validated = []
    for pid in pids:
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            raise ValueError(f"PID must be a positive integer, got {pid!r}")
        validated.append(pid)
    return snapshot_agent_pb2.ProcessTarget(pids=validated)


def cuda_config(pids: Sequence[int]) -> snapshot_agent_pb2.BackendConfig:
    """Builds a BackendConfig selecting the cuda (cuda-checkpoint) backend
    with an explicit process target.

    In k8s mode the agent can discover PIDs itself from the
    ``timeslice.io/job-id`` pod label; pass explicit PIDs for standalone
    mode or to override discovery.
    """
    return snapshot_agent_pb2.BackendConfig(
        cuda=snapshot_agent_pb2.CudaBackendConfig(explicit_target=_process_target(pids))
    )


def direct_memory_config(pids: Sequence[int]) -> snapshot_agent_pb2.BackendConfig:
    """Builds a BackendConfig selecting the direct_memory (GPU-CR
    full-process) backend with an explicit process target.

    Experimental: the agent rejects this config with FAILED_PRECONDITION
    unless it runs with --feature-gates=DirectMemoryBackend=true (or the
    FEATURE_GATES env var). The target workload must run under the GPU-CR
    vGPU preloader.

    In k8s mode the agent can discover PIDs itself from the
    ``timeslice.io/job-id`` pod label; pass explicit PIDs for standalone
    mode or to override discovery.
    """
    return snapshot_agent_pb2.BackendConfig(
        direct_memory=snapshot_agent_pb2.DirectMemoryBackendConfig(
            explicit_target=_process_target(pids)
        )
    )


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
