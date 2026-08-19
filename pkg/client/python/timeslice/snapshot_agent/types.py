from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class MemoryRegion:
    """One device-memory range owned by a process.

    Addresses are plain integers; helpers accept hex strings ("0x7f...")
    and convert. On the wire the address travels as a uint64 (grpcurl JSON
    renders it as a decimal string).
    """

    pid: int
    address: int
    size_bytes: int

    def __post_init__(self):
        if self.pid <= 0:
            raise ValueError(f"memory region pid must be positive, got {self.pid}")
        if self.address < 0:
            raise ValueError(
                f"memory region address must be non-negative, got {self.address}"
            )
        if self.size_bytes <= 0:
            raise ValueError(
                f"memory region size_bytes must be positive, got {self.size_bytes}"
            )

    @classmethod
    def from_spec(cls, spec: str) -> "MemoryRegion":
        """Parses a 'pid:0xADDR:size' spec string.

        Address and size accept hex ("0x...") or decimal literals.
        """
        parts = spec.split(":")
        if len(parts) != 3:
            raise ValueError(
                f"invalid memory region spec {spec!r}, expected 'pid:address:size'"
            )
        try:
            pid = int(parts[0], 0)
            address = int(parts[1], 0)
            size_bytes = int(parts[2], 0)
        except ValueError as e:
            raise ValueError(f"invalid memory region spec {spec!r}: {e}") from e
        return cls(pid=pid, address=address, size_bytes=size_bytes)


@dataclass(frozen=True)
class SnapshotResponse:
    """Response message for Snapshot RPC."""

    operation_id: str


@dataclass(frozen=True)
class RestoreResponse:
    """Response message for Restore RPC."""

    operation_id: str


@dataclass(frozen=True)
class HealthResponse:
    """Response message for Health Check RPC."""

    status: str


@dataclass(frozen=True)
class GetOperationResponse:
    """Response message for GetOperation RPC."""

    status: str
    elapsed_ms: int
    storage_bytes: Optional[int] = None
    snapshot_device_bytes: Optional[int] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class JobStatus:
    """Status information for a specific job."""

    job_id: str
    state: str


@dataclass(frozen=True)
class AcceleratorStatus:
    """Status information for an accelerator."""

    id: str
    memory_used_bytes: int
    memory_total_bytes: int


@dataclass(frozen=True)
class StatusResponse:
    """Response message for Status RPC."""

    job_statuses: List[JobStatus]
    accelerator_statuses: List[AcceleratorStatus]
