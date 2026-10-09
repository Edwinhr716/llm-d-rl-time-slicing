"""Application-aware parking of the trainer (opt-in; cuda-checkpoint is the default).

By default, while the trainer GPU is lent, the snapshot agent checkpoints the
trainer's whole CUDA state with cuda-checkpoint. With
``TIMESLICE_DONOR_BACKEND=app_channel`` (set by the admission webhook when the
trainer worker group is annotated ``timeslice.io/backend: app_channel``) the
trainer parks itself instead:

  park     the actor worker group moves its optimizer state, then its model
           (and gradients) to host memory and frees its cached GPU memory
           (verl's ``to("cpu", ...)``, the manual offload control every verl
           engine implements). The CUDA context and a small allocator residue
           stay, which the agent bounds at verify.
  restore  the worker group moves everything back (``to("cuda")``).

The agent drives both through a workload channel: the trainer driver registers
the park/restore workload with the snapshot agent of every node that runs an
actor worker (the agent is on the host network, so its address is the worker's
node IP and ``TIMESLICE_AGENT_PORT``). The agent selects the channel from the
annotation on the trainer pods.

Safety:
  * a park is only accepted at a safe point: when the trainer does not hold the
    group lock (after a yield, before the next acquire). The orchestrator only
    parks a trainer that yielded, so a refusal means something is wrong, and
    the agent fails closed (the GPU is not lent).
  * after every granted acquire the trainer makes sure it is resident again
    (``ensure_resident``), so a restore the agent lost (an agent restart while
    the trainer was parked) is redone by the trainer itself.

Environment contract (on top of hooks.py's):

  TIMESLICE_DONOR_BACKEND=app_channel   activation gate of this module
  TIMESLICE_AGENT_PORT                  snapshot agent port (default 9001)
  TIMESLICE_AGENT_ADDRS                 optional comma-separated host:port list
                                        that replaces the node discovery
  TIMESLICE_DEVICE                      device name of the restore (default:
                                        verl's get_device_name(), else "cuda")

Nothing here imports verl, ray or grpc at module level, so the logic is
testable without them.
"""

import os
import threading
from collections.abc import Callable, Iterable
from time import monotonic as _now

from timeslice_verl.locks import _log

ENV_DONOR_BACKEND = "TIMESLICE_DONOR_BACKEND"
ENV_AGENT_PORT = "TIMESLICE_AGENT_PORT"
ENV_AGENT_ADDRS = "TIMESLICE_AGENT_ADDRS"
ENV_DEVICE = "TIMESLICE_DEVICE"
BACKEND_APP_CHANNEL = "app_channel"
DEFAULT_AGENT_PORT = 9001
MODE_OFFLOAD = "offload"


def enabled() -> bool:
    return os.environ.get(ENV_DONOR_BACKEND, "").strip().lower() == BACKEND_APP_CHANNEL


class NotAtSafePointError(RuntimeError):
    """The trainer holds the GPU lock: parking it now would pull its state
    from under a running step."""


def _device_name() -> str:
    explicit = os.environ.get(ENV_DEVICE, "").strip()
    if explicit:
        return explicit
    try:
        from verl.utils.device import get_device_name  # noqa: PLC0415 - optional dependency

        return get_device_name()
    except Exception:  # noqa: BLE001 - verl layout without the helper
        return "cuda"


class TrainerPark:
    """The park/restore workload of the trainer's actor worker group.

    ``worker_group`` needs verl's manual offload control:
    ``to(device, model=..., optimizer=..., grad=...)``, dispatched to every
    rank. ``is_safe()`` tells whether the trainer is at a safe point (does not
    hold the group lock). Thread-safe: the agent's commands arrive on the
    workload-channel threads, ``ensure_resident`` on the driver's.
    """

    def __init__(self, worker_group, is_safe: Callable[[], bool], device: str | None = None, clock=_now):
        self._wg = worker_group
        self._is_safe = is_safe
        self._device = device
        self._clock = clock
        self._lock = threading.Lock()
        self._parked = False
        self.parks = 0
        self.restores = 0
        self.self_restores = 0
        self.last_park_s: float | None = None
        self.last_restore_s: float | None = None

    @property
    def parked(self) -> bool:
        return self._parked

    # The SnapshottableAdapter interface of the timeslice client.
    def snapshot(self, mode: str, tags: list[str]) -> None:
        if mode != MODE_OFFLOAD:
            raise ValueError(f"the trainer only parks in mode {MODE_OFFLOAD!r} (got {mode!r}): it must restore")
        with self._lock:
            if self._parked:
                _log("app-park: already parked (repeated park, nothing to do)")
                return
            if not self._is_safe():
                raise NotAtSafePointError("park refused: the trainer holds the GPU lock (not at a safe point)")
            t0 = self._clock()
            # Optimizer first, then the model: the model offload empties the
            # allocator cache, which then also returns the optimizer's blocks.
            self._wg.to("cpu", model=False, optimizer=True, grad=False)
            self._wg.to("cpu", model=True, optimizer=False, grad=True)
            self._parked = True
            self.parks += 1
            self.last_park_s = self._clock() - t0
            _log(f"app-park took={self.last_park_s:.3f}s parks={self.parks}")

    def restore(self, tags: list[str]) -> None:
        self._restore(source="agent")

    def ensure_resident(self) -> bool:
        """Restore if still parked (the agent's restore was lost). Returns
        True when it had to restore."""
        return self._restore(source="self")

    def _restore(self, source: str) -> bool:
        with self._lock:
            if not self._parked:
                return False
            t0 = self._clock()
            self._wg.to(self._device or _device_name(), model=True, optimizer=True, grad=True)
            self._parked = False
            self.restores += 1
            if source == "self":
                self.self_restores += 1
            self.last_restore_s = self._clock() - t0
            note = ""
            if source == "self":
                note = " (WARNING: the agent did not restore the trainer; restored by the trainer)"
            _log(f"app-restore source={source} took={self.last_restore_s:.3f}s restores={self.restores}{note}")
            return True


def _host_port(host: str, port: int) -> str:
    host = host.strip().strip("[]")
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def agent_addresses(node_ips: Iterable[str], port: int | None = None) -> list[str]:
    """The agent address of every distinct node (in first-seen order), or the
    explicit TIMESLICE_AGENT_ADDRS list."""
    explicit = [a.strip() for a in os.environ.get(ENV_AGENT_ADDRS, "").split(",") if a.strip()]
    if explicit:
        return explicit
    if port is None:
        raw = os.environ.get(ENV_AGENT_PORT, "").strip()
        port = int(raw) if raw else DEFAULT_AGENT_PORT
    seen: list[str] = []
    for ip in node_ips:
        if ip and ip not in seen:
            seen.append(ip)
    return [_host_port(ip, port) for ip in seen]


def _node_ip(_worker) -> str:
    import ray  # noqa: PLC0415 - only inside Ray workers

    return ray.util.get_node_ip_address()


def worker_node_ips(worker_group) -> list[str]:
    """Node IPs of the worker group's Ray actors (``__ray_call__`` runs a
    function inside each actor)."""
    import ray  # noqa: PLC0415 - only in the Ray driver

    workers = list(getattr(worker_group, "workers", []) or [])
    return list(ray.get([w.__ray_call__.remote(_node_ip) for w in workers]))


def start(
    worker_group,
    job_id: str,
    group: str,
    is_safe: Callable[[], bool],
    register=None,
    node_ips: Callable[[object], list[str]] = worker_node_ips,
):
    """Register the trainer's park/restore workload with the snapshot agent of
    every node of the worker group. Returns (TrainerPark, handles)."""
    if register is None:
        from timeslice.snapshot_agent.workload import register_workload as register  # noqa: PLC0415, N813

    park = TrainerPark(worker_group, is_safe)
    addrs = agent_addresses(node_ips(worker_group))
    if not addrs:
        raise RuntimeError("app_channel donor: no node found for the actor workers; set TIMESLICE_AGENT_ADDRS")
    handles = [
        register(agent=a, job_id=job_id, group=group, workload=park, supported_modes=[MODE_OFFLOAD]) for a in addrs
    ]
    _log(f"app-park: workload of job={job_id} group={group} registered with agents {addrs}")
    return park, handles
