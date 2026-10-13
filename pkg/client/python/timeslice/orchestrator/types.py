from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional


class GroupLockState(str, Enum):
    UNSPECIFIED = "UNSPECIFIED"
    UNKNOWN = "UNKNOWN"
    IDLE = "IDLE"
    IDLE_YIELDED = "IDLE_YIELDED"
    LOCKED = "LOCKED"
    SWITCHING = "SWITCHING"
    BACKGROUND = "BACKGROUND"
    VACATING = "VACATING"


class AgentJobState(str, Enum):
    UNSPECIFIED = "UNSPECIFIED"
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    TRANSITIONING = "TRANSITIONING"
    SAVED = "SAVED"
    FAULTED = "FAULTED"
    SUSPENDED = "SUSPENDED"


@dataclass(frozen=True)
class AcquireResult:
    success: bool
    waited_ms: int
    context_restored: bool
    # True only when the accelerator was handed back after a background guest
    # kill that did not confirm, so device memory may still be held.
    vram_unconfirmed: bool = False


@dataclass(frozen=True)
class YieldResult:
    success: bool
    pending_waiters: int
    snapshot_deferred: bool


@dataclass(frozen=True)
class SnapshotAgentJobState:
    agent: str
    job_id: str
    job_state: AgentJobState


@dataclass(frozen=True)
class GroupStatus:
    group_id: str
    group_state: GroupLockState
    state_timestamp: datetime
    locking_job: str
    active_job: str
    waiter_queue_depth: int
    loaded_job: str
    # 1 when the server speaks the background participant protocol.
    background_protocol: int = 0
    # Time left until background guests must vacate; set only while a notice runs.
    vacate_within: Optional[timedelta] = None


@dataclass(frozen=True)
class OrchestratorGroupStatus:
    group: GroupStatus
    agent_job_states: List[SnapshotAgentJobState]
