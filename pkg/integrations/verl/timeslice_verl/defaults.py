"""Setting defaults that time-slicing needs, applied when the hooks are enabled.

While the trainer GPU is lent, the trainer's CUDA state is checkpointed and the
GPU serves another workload. The NCCL communicator verl keeps between the
trainer and the rollout workers for weight sync does not survive that, so the
weight-sync group must be rebuilt at every sync. verl has the setting
(``actor_rollout_ref.rollout.checkpoint_engine.engine_kwargs.nccl.rebuild_group``)
but defaults it to False. With TIMESLICE_FULLY_ASYNC=1 this module makes True
the default, so the RL team does not have to add the override; an explicit
``rebuild_group=False`` from the user is still honoured.

Inert without the env gate, and a no-op when verl has no NCCL checkpoint
engine (older or newer layouts).
"""

import functools

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
    return patch_rebuild_group(NCCLCheckpointEngine)


def patch_rebuild_group(cls) -> bool:
    """Make rebuild_group default to True on cls.__init__ (idempotent)."""
    init = cls.__init__
    if getattr(init, _MARK, False):
        return True

    @functools.wraps(init)
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("rebuild_group", True)
        init(self, *args, **kwargs)

    setattr(__init__, _MARK, True)
    cls.__init__ = __init__
    _log("defaults: NCCL checkpoint engine rebuild_group defaults to True")
    return True
