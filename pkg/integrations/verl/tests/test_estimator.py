"""Tests for the expected_idle estimator and the hint the verl hooks send.

Run:  python3 -m pytest tests/ -v

What is covered:
  * timeslice_verl.estimator replays tests/testdata/expected_idle_parity.json,
    the fixture its Go mirror (pkg/integrations/verl/expectedidle) replays too
  * parse_mode for TIMESLICE_EXPECTED_IDLE (off / auto / seconds / invalid)
  * the hooks: default off sends no hint (release() is called without the
    argument, as before); auto sends no hint on a point's first yield, then the
    EWMA of the measured yield->acquire gaps; a fixed value is sent as is; an
    invalid value sends no hint
  * PhaseLocks.drop_all passes the hint through
"""

import asyncio
import json
import math
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import timeslice_verl.hooks as hooks_mod  # noqa: E402
from timeslice_verl import estimator  # noqa: E402
from timeslice_verl.hooks import TimesliceHooksMixin  # noqa: E402
from timeslice_verl.locks import PhaseLocks  # noqa: E402

JOB = "job-a"
ADDR = "orch:50051"
GROUP = "trainers"
FIXTURE = os.path.join(os.path.dirname(__file__), "testdata", "expected_idle_parity.json")


def load_fixture() -> dict:
    with open(FIXTURE, encoding="utf-8") as f:
        return json.load(f)


class HintClient:
    """Records each release() and the expected_idle it got (absent = no kwarg)."""

    def __init__(self, releases):
        self.releases = releases

    def acquire(self, group_id, timeout_sec=None):
        return SimpleNamespace(success=True, waited_ms=1, context_restored=True)

    def release(self, group_id, **kwargs):
        self.releases.append(kwargs)
        return SimpleNamespace(success=True, pending_waiters=0, snapshot_deferred=False)

    def close(self):
        pass


class Trainer(TimesliceHooksMixin):
    def __init__(self, releases):
        super().__init__(client_factory=lambda target, job_id, group_id: HintClient(releases))
        self.config = SimpleNamespace(trainer=SimpleNamespace(save_freq=-1))


class Clock:
    """Stand-in for the hooks module's monotonic clock (_now)."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(hooks_mod, "_now", c)
    return c


@pytest.fixture
def ts_env(monkeypatch):
    monkeypatch.setenv("TIMESLICE_FULLY_ASYNC", "1")
    monkeypatch.setenv("TIMESLICE_JOB_ID", JOB)
    monkeypatch.setenv("TIMESLICE_ORCH_ADDR", ADDR)
    monkeypatch.setenv("TIMESLICE_GROUP", GROUP)
    monkeypatch.delenv(hooks_mod.ENV_EMPTY_CACHE, raising=False)
    monkeypatch.delenv(estimator.ENV_MODE, raising=False)


def run_steps(t, clock, gaps):
    """init_workers, then one training step per gap: the trainer yields at
    update_weights and the next batch arrives `gap` seconds later."""

    async def seq():
        await t.on_init_workers_begin()
        await t.on_init_workers_end()
        clock.now += 5.0
        await t.on_update_weights_begin()  # pre-fit initial param sync: a real acquire
        await t.on_update_weights_end(synced=True)
        for gap in gaps:
            clock.now += gap
            await t.on_sample_end()
            clock.now += 2.0  # the step itself
            await t.on_update_weights_begin()  # held: no-op
            await t.on_update_weights_end(synced=True)

    asyncio.run(seq())


# ======================================================================
# estimator
# ======================================================================


def test_expected_idle_parity_fixture():
    fx = load_fixture()
    assert fx["alpha"] == estimator.ALPHA
    state = estimator.new()
    for i, step in enumerate(fx["steps"]):
        if step["op"] == "observe":
            estimator.observe(state, step["point"], step["gap"])
        elif step["op"] == "next":
            assert estimator.next(state, step["point"]) == step["want"], f"step {i}"
        else:
            pytest.fail(f"step {i}: unknown op {step['op']!r}")


def test_expected_idle_parse_mode_fixture():
    for case in load_fixture()["modes"]:
        if case["mode"] == "invalid":
            with pytest.raises(ValueError):
                estimator.parse_mode(case["raw"])
            continue
        assert estimator.parse_mode(case["raw"]) == (case["mode"], case.get("seconds", 0)), case


def test_expected_idle_parse_mode_unset_is_off():
    assert estimator.parse_mode(None) == (estimator.MODE_OFF, 0.0)


def test_expected_idle_ignores_nan():
    state = estimator.new()
    estimator.observe(state, "p", math.nan)
    assert estimator.next(state, "p") is None
    estimator.observe(state, "p", 8)
    estimator.observe(state, "p", math.nan)
    assert estimator.next(state, "p") == 8


# ======================================================================
# hooks
# ======================================================================


def test_expected_idle_default_off_sends_no_hint(ts_env, clock):
    releases = []
    run_steps(Trainer(releases), clock, [10, 20])
    assert releases == [{}, {}, {}, {}]


def test_expected_idle_auto_no_hint_on_first_yield_then_ewma(ts_env, clock, monkeypatch):
    monkeypatch.setenv(estimator.ENV_MODE, "auto")
    releases = []
    t = Trainer(releases)
    run_steps(t, clock, [10, 20, 30])
    # init_workers yields once (no gap measured there yet). update_weights:
    # the first yield has no hint; then 10, 0.5*20 + 0.5*10 = 15, 0.5*30 + 0.5*15 = 22.5.
    assert releases == [{}, {}, {"expected_idle": 10.0}, {"expected_idle": 15.0}, {"expected_idle": 22.5}]
    # the init_workers -> first acquire gap (5 s) was measured
    assert estimator.next(t._idle_state, "init_workers") == 5.0


def test_expected_idle_fixed_value(ts_env, clock, monkeypatch):
    monkeypatch.setenv(estimator.ENV_MODE, "45")
    releases = []
    run_steps(Trainer(releases), clock, [10])
    assert releases == [{"expected_idle": 45.0}] * 3


def test_expected_idle_invalid_env_sends_no_hint(ts_env, clock, monkeypatch, capsys):
    monkeypatch.setenv(estimator.ENV_MODE, "soon")
    releases = []
    run_steps(Trainer(releases), clock, [10, 20])
    assert releases == [{}, {}, {}, {}]
    out = capsys.readouterr().out
    assert out.count("invalid TIMESLICE_EXPECTED_IDLE") == 1


def test_expected_idle_drop_all_passes_hint():
    releases = []
    locks = PhaseLocks(JOB, ADDR, GROUP, client_factory=lambda target, job_id, group_id: HintClient(releases))
    locks.ensure()
    locks.drop_all(12.5)
    locks.ensure()
    locks.drop_all()
    assert releases == [{"expected_idle": 12.5}, {}]
