"""Unit tests for timeslice_verl.defaults (pure python, no verl)."""

import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from timeslice_verl import defaults  # noqa: E402


class _Engine:
    def __init__(self, bucket_size, rebuild_group=False, **kw):
        self.bucket_size = bucket_size
        self.rebuild_group = rebuild_group


def _fresh():
    return type("Engine", (_Engine,), {"__init__": _Engine.__init__})


def test_rebuild_group_defaults_true_and_explicit_kept():
    cls = _fresh()
    assert defaults.patch_rebuild_group(cls)
    assert cls(bucket_size=1).rebuild_group is True
    assert cls(bucket_size=1, rebuild_group=False).rebuild_group is False


def test_patch_is_idempotent():
    cls = _fresh()
    defaults.patch_rebuild_group(cls)
    first = cls.__init__
    defaults.patch_rebuild_group(cls)
    assert cls.__init__ is first


def test_apply_inert_without_env(monkeypatch):
    monkeypatch.delenv("TIMESLICE_FULLY_ASYNC", raising=False)
    assert defaults.apply() is False


def test_apply_with_env_patches_verl_engine(monkeypatch):
    monkeypatch.setenv("TIMESLICE_FULLY_ASYNC", "1")
    cls = _fresh()
    mod = types.ModuleType("verl.checkpoint_engine.nccl_checkpoint_engine")
    mod.NCCLCheckpointEngine = cls
    for name in ("verl", "verl.checkpoint_engine"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, mod.__name__, mod)
    assert defaults.apply() is True
    assert cls(bucket_size=1).rebuild_group is True


def test_apply_without_engine_is_noop(monkeypatch):
    monkeypatch.setenv("TIMESLICE_FULLY_ASYNC", "1")
    monkeypatch.setitem(sys.modules, "verl", None)
    assert defaults.apply() is False


class _PG:
    rebuild_group = True

    def __init__(self):
        self.calls = []

    def init_process_group(self, rank, world_size, master_metadata=None):
        self.calls.append((rank, world_size))
        return "ok"


def test_group_timing_logs_joining_ranks_only(capsys):
    cls = type("PG", (_PG,), {"init_process_group": _PG.init_process_group})
    assert defaults.patch_group_timing(cls)
    first = cls.init_process_group
    assert defaults.patch_group_timing(cls)  # idempotent
    assert cls.init_process_group is first
    pg = cls()
    assert pg.init_process_group(0, 2, master_metadata="m") == "ok"
    assert pg.init_process_group(rank=-1, world_size=2) == "ok"
    assert pg.calls == [(0, 2), (-1, 2)]
    out = capsys.readouterr().out
    assert out.count("nccl-group init") == 1
    assert "rank=0 world=2 rebuild=True took=" in out


def test_group_timing_without_method():
    assert defaults.patch_group_timing(type("X", (), {})) is False
