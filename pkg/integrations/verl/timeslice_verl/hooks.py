"""TimesliceHooksMixin: verl fully-async lifecycle hooks for GPU time-slicing.

Maps the phase transitions of verl's fully_async_policy trainer onto a single
orchestrator group lock (the trainers pool), so multiple RL jobs can share one
trainer GPU via checkpoint/restore. The mixin overrides the empty ``on_*``
template methods that verl's FullyAsyncTrainer exposes (same convention as
the v1 trainers); ``timeslice_verl.trainer.TimesliceFullyAsyncTrainer``
combines it with the real trainer and registers itself under the name
``timeslice`` via verl's fully-async trainer registry. Select it with the
hydra override ``async_training.trainer_name=timeslice``. Requires a verl
build with fully-async lifecycle hooks + trainer registry and per-pool
placement-group bundle resources (``ray_pg_extra_resources``).

The mixin itself imports nothing from verl, so the lock protocol is testable
without verl/ray/grpc/GPU (see tests/).

Lock protocol (single trainers-pool group lock):

  on_init_workers_begin        ACQUIRE (worker groups + model load touch the GPU)
  on_init_workers_end          YIELD (trainer is checkpointed off until resumed)
  on_sample_end                ACQUIRE (per-step resume point: a full batch was
                               dequeued and assembled; the queue wait itself is
                               CPU-only and runs unlocked)
  on_update_weights_begin      ensure-held ACQUIRE (idempotent; a real acquire
                               only for the pre-fit initial param sync, which
                               fully_async_main runs after init_workers)
  on_update_weights_end        YIELD iff synced (update_actor + NCCL param sync
                               + reset_staleness complete)
  on_save_checkpoint           veto (return False) when trainer.save_freq > 0:
                               saving RPCs the FSDP worker, which may be
                               checkpointed off; run with trainer.save_freq=-1
  on_exception                 crash-release: never die holding the group lock

All lock RPCs run in the CPU-only trainer driver actor (never checkpointed)
via ``asyncio.to_thread``, so a minutes-long acquire never stalls the actor's
event loop. Acquire failures propagate (a job must not run unlocked); release
failures are swallowed.

Environment contract:

  TIMESLICE_FULLY_ASYNC=1      activation gate; the hooks are fully inert
                               otherwise (safe to bake into images and to keep
                               async_training.trainer_name=timeslice in shared
                               manifests)
  TIMESLICE_JOB_ID             unique job identifier (e.g. "job-a")
  TIMESLICE_ORCH_ADDR          orchestrator gRPC address (e.g. "127.0.0.1:50051")
  TIMESLICE_GROUP              trainers-pool group id (e.g. "trainers")
                               (gate on + these missing => warn-once no-op locks)
  TIMESLICE_EMPTY_CACHE_BEFORE_YIELD=1
                               experimental: torch.cuda.empty_cache() right
                               before each yield's release (best-effort probe
                               for smaller cuda-checkpoint snapshots)
  TIMESLICE_DONOR_BACKEND=app_channel
                               opt-in application-aware parking: the trainer
                               parks itself (model and optimizer to host
                               memory) when the snapshot agent asks, instead
                               of being cuda-checkpointed (timeslice_verl.
                               app_aware; registered at on_init_workers_end,
                               and every granted acquire makes sure the
                               trainer is resident again)
  TIMESLICE_EXPECTED_IDLE      expected_idle hint sent with each yield
                               (timeslice_verl.estimator): "off" (default,
                               no hint), "auto" (per yield point, EWMA of the
                               measured gaps between that point's yield and
                               the next acquire; no hint on a point's first
                               yield) or a fixed number of seconds. The
                               orchestrator lends the GPU to guests on a hint
                               >= its --min-bubble (--lend-policy=hint).

Config this module depends on directly: async_training.trainer_name=timeslice
selects the subclass (see trainer.py); trainer.save_freq=-1 is enforced by the
on_save_checkpoint veto; ray_pg_extra_resources.* feeds placement-group
pinning. The full required-overrides list for time-sliced runs lives in the
verl integration guide (guides/rl-frameworks/verl/).
"""

import asyncio
import os
import threading
from time import monotonic as _now

from timeslice_verl import app_aware, estimator
from timeslice_verl.locks import PhaseLocks, _log

ENV_ENABLE = "TIMESLICE_FULLY_ASYNC"
# EXPERIMENTAL: when "1", call torch.cuda.empty_cache() immediately before each
# yield's drop_all, to probe whether releasing cached allocator blocks shrinks
# the cuda-checkpoint snapshot. Inert by default; never breaks training.
ENV_EMPTY_CACHE = "TIMESLICE_EMPTY_CACHE_BEFORE_YIELD"


def _flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def enabled() -> bool:
    return _flag(ENV_ENABLE)


class TimesliceHooksMixin:
    """Lifecycle-hook overrides implementing the lock protocol above.

    Mixed into verl's FullyAsyncTrainer by
    ``timeslice_verl.trainer.TimesliceFullyAsyncTrainer`` (mixin first in the
    MRO, so these overrides win over the trainer's empty template methods).
    Every hook runs inside the trainer driver actor. `client_factory(target,
    job_id, group_id)` is injectable for grpc-free tests.
    """

    def __init__(self, client_factory=None):
        self._client_factory = client_factory
        self._state_lock = threading.Lock()
        self._locks: PhaseLocks | None = None
        self._warned: set = set()
        # expected_idle estimator state, and the (point, monotonic time) of
        # the last yield whose gap to the next acquire is not measured yet.
        self._idle_state = estimator.new()
        self._pending_gap: tuple[str, float] | None = None
        # Application-aware parking (TIMESLICE_DONOR_BACKEND=app_channel).
        self._park: app_aware.TrainerPark | None = None
        self._park_handles: list = []
        if enabled():
            _log("fully_async: timeslice lifecycle hooks active (TIMESLICE_FULLY_ASYNC=1)")

    # ------------------------------------------------------------ lifecycle hooks

    async def on_init_workers_begin(self) -> None:
        if not enabled():
            return
        await self._ensure_lock(point="init_workers")

    async def on_init_workers_end(self) -> None:
        if not enabled():
            return
        if app_aware.enabled():
            try:
                await asyncio.to_thread(self._start_app_park)
            except BaseException:
                # Never leave holding the group lock.
                locks = self._locks
                if locks is not None and locks.enabled:
                    await asyncio.to_thread(locks.drop_all)
                raise
        await self._yield_lock(point="init_workers")

    async def on_sample_end(self) -> None:
        if not enabled():
            return
        await self._ensure_lock(point="samples_ready")

    async def on_update_weights_begin(self) -> None:
        if not enabled():
            return
        # Idempotent ensure-held: a real acquire only happens for the pre-fit
        # initial param sync (fully_async_main calls _fit_update_weights after
        # init_workers, where we yielded). In steady state the lock is already
        # held from on_sample_end, so this is a no-op.
        await self._ensure_lock(point="update_weights")

    async def on_update_weights_end(self, synced: bool) -> None:
        if not enabled():
            return
        if synced:
            await self._yield_lock(point="update_weights")
        else:
            self._warn_once(
                "trigger_gt_1",
                "_fit_update_weights did not sync (trigger_parameter_sync_step > 1?): "
                "no yield this step — the trainer lock stays held across the next "
                "queue wait. Time-slicing is designed for trigger_parameter_sync_step=1.",
            )

    def on_save_checkpoint(self, force: bool) -> bool:
        if not enabled():
            return True
        save_freq = None
        try:
            save_freq = self.config.trainer.save_freq
        except Exception:  # noqa: BLE001
            save_freq = None
        if save_freq is not None and save_freq > 0:
            self._warn_once(
                "save_checkpoint",
                f"vetoing checkpoint save (save_freq={save_freq}, force={force}): "
                "checkpoint saving is disabled under fully-async time-slicing "
                "(the trainer worker may be checkpointed off). Set trainer.save_freq=-1.",
            )
            return False
        return True

    async def on_exception(self, point: str, exc: BaseException) -> None:
        """Best-effort lock release on a crash path: a job must never die
        holding the group lock (the orchestrator would stay pinned to it).
        Never raises — crash hygiene must not mask the real error (the
        trainer additionally logs and suppresses on_exception hook errors)."""
        if not enabled():
            return
        try:
            locks = self._locks
            if locks is None or not locks.enabled:
                return
            _log(f"crash-release at point={point}: {exc!r:.500}")
            await asyncio.to_thread(locks.drop_all)  # idempotent; RPC errors swallowed
        except Exception as release_err:  # noqa: BLE001
            # Crash hygiene must never mask the original error.
            _log(f"crash-release failed (ignored): {release_err!r}")

    # ------------------------------------------------------------ lock plumbing

    def _get_locks(self) -> PhaseLocks:
        """Lazily build the per-process lock singleton (may open a gRPC
        channel: only call off the event loop)."""
        if self._locks is None:
            with self._state_lock:
                if self._locks is None:
                    self._locks = PhaseLocks.from_env(client_factory=self._client_factory)
        return self._locks

    async def _ensure_lock(self, point: str) -> None:
        """Idempotent blocking acquire, run in a worker thread so the actor's
        event loop stays free (the wait can be minutes while the other job
        holds the group). Acquire errors propagate."""
        locks = await asyncio.to_thread(self._get_locks)
        self._observe_gap(locks)
        await asyncio.to_thread(locks.ensure)
        park = self._park
        if park is not None:
            # The orchestrator grants only once the agent restored the
            # trainer; this redoes a restore the agent lost (agent restart).
            await asyncio.to_thread(park.ensure_resident)

    async def _yield_lock(self, point: str) -> None:
        """Idempotent release (PhaseLocks.drop_all: errors logged, never
        raised), run in a worker thread. Sends the expected_idle hint for
        `point` (TIMESLICE_EXPECTED_IDLE) and starts timing the gap to the
        next acquire."""
        locks = self._locks
        if locks is None or not locks.enabled or not locks.held:
            return
        await asyncio.to_thread(self._maybe_empty_cache, point)  # experimental, inert by default
        await asyncio.to_thread(locks.drop_all, self._expected_idle(point))
        self._pending_gap = (point, _now())

    # ------------------------------------------------------------ app-aware parking

    def _at_safe_point(self) -> bool:
        """A park is safe only while the trainer does not hold the lock."""
        locks = self._locks
        return locks is None or not locks.enabled or not locks.held

    def _start_app_park(self) -> None:
        """Register the park/restore workload of the actor worker group with
        the node agents (once, after the worker groups exist)."""
        if self._park is not None:
            return
        worker_group = getattr(self, "actor_wg", None)
        if worker_group is None:
            raise RuntimeError("app_channel donor: the trainer has no actor worker group to park")
        locks = self._get_locks()
        if not locks.enabled:
            self._warn_once(
                "app_park_no_locks",
                "TIMESLICE_DONOR_BACKEND=app_channel without lock settings: not parking",
            )
            return
        self._park, self._park_handles = app_aware.start(
            worker_group, job_id=locks.job_id, group=locks.group, is_safe=self._at_safe_point
        )

    # ------------------------------------------------------------ expected_idle

    def _expected_idle(self, point: str) -> float | None:
        """The hint for a yield at `point` in seconds, or None for no hint."""
        try:
            mode, seconds = estimator.parse_mode(os.environ.get(estimator.ENV_MODE))
        except ValueError as e:
            self._warn_once("expected_idle_mode", f"{e}; sending no expected_idle hint")
            return None
        if mode == estimator.MODE_FIXED:
            return seconds
        if mode == estimator.MODE_AUTO:
            return estimator.next(self._idle_state, point)
        return None

    def _observe_gap(self, locks: PhaseLocks) -> None:
        """Feed the estimator the gap from the last yield to this acquire
        (only when the acquire is real, i.e. the lock is not held)."""
        pending, self._pending_gap = self._pending_gap, None
        if pending is None or not locks.enabled or locks.held:
            return
        point, yielded_at = pending
        estimator.observe(self._idle_state, point, _now() - yielded_at)

    # ------------------------------------------------------------ misc

    def _warn_once(self, key: str, msg: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            _log(f"WARNING: {msg}")

    def _maybe_empty_cache(self, point: str) -> None:
        """EXPERIMENTAL, env-gated: best-effort torch.cuda.empty_cache() right
        before a yield's drop_all. Never raises; no-op when disabled, when
        torch is not importable, or when empty_cache itself fails (e.g. no
        CUDA context — the hooks run in the CPU-only driver actor)."""
        if not _flag(ENV_EMPTY_CACHE):
            return
        try:
            import torch  # noqa: PLC0415 - only touch torch when the experiment is on
        except Exception:  # noqa: BLE001
            self._warn_once("empty_cache_no_torch", f"{ENV_EMPTY_CACHE}=1 but torch is not importable; skipping")
            return
        try:
            torch.cuda.empty_cache()
            _log(f"empty_cache before yield point={point} (experimental {ENV_EMPTY_CACHE}=1)")
        except Exception as e:  # noqa: BLE001
            self._warn_once("empty_cache_failed", f"torch.cuda.empty_cache() failed ({e!r}); continuing")
