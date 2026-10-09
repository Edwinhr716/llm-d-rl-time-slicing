"""Setting defaults that time-slicing needs, applied when the hooks are enabled.

While the trainer GPU is lent, the trainer's CUDA state is checkpointed and the
GPU serves another workload. The NCCL communicator verl keeps between the
trainer and the rollout workers for weight sync does not survive that, so the
weight-sync group must be rebuilt at every sync. verl has the setting
(``actor_rollout_ref.rollout.checkpoint_engine.engine_kwargs.nccl.rebuild_group``)
but defaults it to False. With TIMESLICE_FULLY_ASYNC=1 this module makes True
the default, so the RL team does not have to add the override; an explicit
``rebuild_group=False`` from the user is still honoured.

It also times every (re)build of that group and logs it
(``[timeslice] nccl-group init ... took=...``), which is the cost the rebuild
adds to each weight sync.

Inert without the env gate, and a no-op when verl has no NCCL checkpoint
engine (older or newer layouts).
"""

import functools
import time

from timeslice_verl.hooks import enabled
from timeslice_verl.locks import _log

_MARK = "_timeslice_defaults"


def apply() -> bool:
    """Install the defaults. Returns True when the NCCL default is in place."""
    if not enabled():
        return False
    try:
        from verl.checkpoint_engine.nccl_checkpoint_engine import NCCLCheckpointEngine
    except Exception:  # noqa: BLE001 - verl layout without an NCCL engine
        return False
    patch_group_timing(NCCLCheckpointEngine)
    return patch_rebuild_group(NCCLCheckpointEngine)


def patch_rebuild_group(cls) -> bool:
    """Make rebuild_group default to True on cls.__init__ (idempotent)."""
    init = cls.__init__
    if getattr(init, _MARK, False):
        return True

    @functools.wraps(init)
    def patched_init(self, *args, **kwargs):
        kwargs.setdefault("rebuild_group", True)
        init(self, *args, **kwargs)

    setattr(patched_init, _MARK, True)
    cls.__init__ = patched_init
    _log("defaults: NCCL checkpoint engine rebuild_group defaults to True")
    return True


_TIMING_MARK = "_timeslice_group_timing"


def patch_group_timing(cls) -> bool:
    """Log the duration of cls.init_process_group for ranks that join the
    group (idempotent; False when cls has no such method)."""
    init_pg = getattr(cls, "init_process_group", None)
    if init_pg is None:
        return False
    if getattr(init_pg, _TIMING_MARK, False):
        return True

    @functools.wraps(init_pg)
    def timed(self, rank, world_size, *args, **kwargs):
        t0 = time.monotonic()
        out = init_pg(self, rank, world_size, *args, **kwargs)
        if rank is not None and rank >= 0:
            _log(
                f"nccl-group init rank={rank} world={world_size} "
                f"rebuild={getattr(self, 'rebuild_group', '?')} took={time.monotonic() - t0:.3f}s"
            )
        return out

    setattr(timed, _TIMING_MARK, True)
    cls.init_process_group = timed
    return True
