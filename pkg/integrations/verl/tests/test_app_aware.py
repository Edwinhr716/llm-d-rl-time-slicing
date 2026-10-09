"""Unit tests for timeslice_verl.app_aware and its hooks wiring (pure python:
no GPU, no verl, no ray, no grpc)."""

import asyncio
import os
import sys
import threading
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import timeslice_verl.hooks as hooks_mod  # noqa: E402
from timeslice_verl import app_aware  # noqa: E402
from timeslice_verl.hooks import TimesliceHooksMixin  # noqa: E402

JOB, ADDR, GROUP = "job-a", "orch:50051", "ns.job-a.trainer"


class FakeWG:
    """Stands in for verl's actor worker group (manual offload control)."""

    def __init__(self, events):
        self.events = events
        self.workers = ["w0", "w1"]

    def to(self, device, model=True, optimizer=True, grad=True):
        self.events.append(("to", device, model, optimizer, grad))


class FakeClient:
    def __init__(self, events):
        self.events = events

    def acquire(self, group_id, timeout_sec=None):
        self.events.append(("acquire", group_id))
        return SimpleNamespace(success=True, waited_ms=1, context_restored=True)

    def release(self, group_id, **_kw):
        self.events.append(("release", group_id))
        return SimpleNamespace(success=True, pending_waiters=0, snapshot_deferred=False)

    def close(self):
        pass


class FakeTrainer(TimesliceHooksMixin):
    def __init__(self, events):
        super().__init__(client_factory=lambda target, job_id, group_id: FakeClient(events))
        self.config = SimpleNamespace(trainer=SimpleNamespace(save_freq=-1))
        self.actor_wg = FakeWG(events)


@pytest.fixture
def events():
    return []


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("TIMESLICE_FULLY_ASYNC", "1")
    monkeypatch.setenv("TIMESLICE_JOB_ID", JOB)
    monkeypatch.setenv("TIMESLICE_ORCH_ADDR", ADDR)
    monkeypatch.setenv("TIMESLICE_GROUP", GROUP)
    monkeypatch.setenv("TIMESLICE_DEVICE", "cuda")
    monkeypatch.delenv(app_aware.ENV_AGENT_ADDRS, raising=False)
    monkeypatch.delenv(app_aware.ENV_AGENT_PORT, raising=False)
    monkeypatch.delenv(app_aware.ENV_DONOR_BACKEND, raising=False)


# ----------------------------------------------------------------- TrainerPark


def test_park_offloads_optimizer_then_model_and_restores(env, events):
    park = app_aware.TrainerPark(FakeWG(events), is_safe=lambda: True)
    park.snapshot("offload", [])
    assert park.parked
    assert events == [("to", "cpu", False, True, False), ("to", "cpu", True, False, True)]
    park.snapshot("offload", [])  # repeated park: no-op
    assert len(events) == 2
    park.restore([])
    assert not park.parked
    assert events[-1] == ("to", "cuda", True, True, True)
    park.restore([])  # repeated restore: no-op
    assert len(events) == 3
    assert (park.parks, park.restores, park.self_restores) == (1, 1, 0)


def test_park_refused_outside_safe_point(env, events):
    park = app_aware.TrainerPark(FakeWG(events), is_safe=lambda: False)
    with pytest.raises(app_aware.NotAtSafePointError):
        park.snapshot("offload", [])
    assert events == [] and not park.parked


def test_park_refuses_discard(env, events):
    park = app_aware.TrainerPark(FakeWG(events), is_safe=lambda: True)
    with pytest.raises(ValueError):
        park.snapshot("discard", [])
    assert events == []


def test_ensure_resident_self_restores(env, events):
    park = app_aware.TrainerPark(FakeWG(events), is_safe=lambda: True)
    assert park.ensure_resident() is False
    park.snapshot("offload", [])
    assert park.ensure_resident() is True
    assert park.self_restores == 1 and not park.parked


def test_park_and_restore_serialize(env, events):
    """A restore arriving during a park waits for the park to finish."""
    entered, gate = threading.Event(), threading.Event()

    class SlowWG(FakeWG):
        def to(self, device, **kw):
            if device == "cpu" and kw.get("model"):
                entered.set()
                gate.wait(2)
            super().to(device, **kw)

    park = app_aware.TrainerPark(SlowWG(events), is_safe=lambda: True)
    t = threading.Thread(target=park.snapshot, args=("offload", []))
    t.start()
    assert entered.wait(2)
    r = threading.Thread(target=park.restore, args=([],))
    r.start()
    gate.set()
    t.join(2)
    r.join(2)
    assert [e[1] for e in events] == ["cpu", "cpu", "cuda"]
    assert not park.parked


# -------------------------------------------------------------- addresses


def test_agent_addresses_dedup_port_and_ipv6(env, monkeypatch):
    assert app_aware.agent_addresses(["10.0.0.1", "10.0.0.1", "10.0.0.2"]) == ["10.0.0.1:9001", "10.0.0.2:9001"]
    monkeypatch.setenv(app_aware.ENV_AGENT_PORT, "9100")
    assert app_aware.agent_addresses(["fd00::1"]) == ["[fd00::1]:9100"]
    monkeypatch.setenv(app_aware.ENV_AGENT_ADDRS, "a:1, b:2")
    assert app_aware.agent_addresses(["10.0.0.1"]) == ["a:1", "b:2"]


def test_start_registers_once_per_node(env):
    calls = []

    def register(**kw):
        calls.append(kw)
        return object()

    park, handles = app_aware.start(
        FakeWG([]), JOB, GROUP, is_safe=lambda: True, register=register, node_ips=lambda wg: ["n1", "n1", "n2"]
    )
    assert [c["agent"] for c in calls] == ["n1:9001", "n2:9001"]
    assert all(c["job_id"] == JOB and c["group"] == GROUP and c["workload"] is park for c in calls)
    assert all(c["supported_modes"] == ["offload"] for c in calls)
    assert len(handles) == 2


def test_enabled_gate(monkeypatch):
    monkeypatch.delenv(app_aware.ENV_DONOR_BACKEND, raising=False)
    assert not app_aware.enabled()
    monkeypatch.setenv(app_aware.ENV_DONOR_BACKEND, "app_channel")
    assert app_aware.enabled()
    monkeypatch.setenv(app_aware.ENV_DONOR_BACKEND, "cuda")
    assert not app_aware.enabled()


# ------------------------------------------------------------------- hooks


@pytest.fixture
def registered(monkeypatch):
    """Replace the agent registration and node discovery with fakes."""
    calls = []
    real_start = app_aware.start

    def fake_start(wg, job_id, group, is_safe):
        return real_start(
            wg, job_id, group, is_safe, register=lambda **kw: calls.append(kw), node_ips=lambda _wg: ["10.0.0.1"]
        )

    monkeypatch.setattr(hooks_mod.app_aware, "start", fake_start)
    return calls


def test_hooks_cuda_default_registers_nothing(env, events, registered):
    t = FakeTrainer(events)

    async def seq():
        await t.on_init_workers_begin()
        await t.on_init_workers_end()

    asyncio.run(seq())
    assert registered == [] and t._park is None


def test_hooks_app_channel_park_only_while_yielded(env, events, registered, monkeypatch):
    monkeypatch.setenv(app_aware.ENV_DONOR_BACKEND, "app_channel")
    t = FakeTrainer(events)

    async def init():
        await t.on_init_workers_begin()
        await t.on_init_workers_end()

    asyncio.run(init())
    assert len(registered) == 1 and registered[0]["agent"] == "10.0.0.1:9001"
    assert registered[0]["job_id"] == JOB and registered[0]["group"] == GROUP
    park = t._park
    # Yielded after init: the agent may park the trainer.
    park.snapshot("offload", [])
    assert park.parked

    # The agent loses the restore (e.g. restarts): the next granted acquire
    # restores the trainer before any GPU work.
    asyncio.run(t.on_sample_end())
    assert not park.parked and park.self_restores == 1
    assert events.index(("acquire", GROUP), 1) < events.index(("to", "cuda", True, True, True))

    # Holding the lock: a park is refused.
    with pytest.raises(app_aware.NotAtSafePointError):
        park.snapshot("offload", [])

    async def update():
        await t.on_update_weights_begin()
        await t.on_update_weights_end(synced=True)

    asyncio.run(update())
    park.snapshot("offload", [])  # yielded again
    assert park.parked


def test_hooks_app_channel_registration_failure_releases_lock(env, events, monkeypatch):
    monkeypatch.setenv(app_aware.ENV_DONOR_BACKEND, "app_channel")

    def broken_start(*a, **kw):
        raise RuntimeError("no agent")

    monkeypatch.setattr(hooks_mod.app_aware, "start", broken_start)
    t = FakeTrainer(events)

    async def init():
        await t.on_init_workers_begin()
        await t.on_init_workers_end()

    with pytest.raises(RuntimeError):
        asyncio.run(init())
    assert events[-1] == ("release", GROUP)
