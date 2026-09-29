"""D-NS-16 client wiring in the verl plugin: keep vs ns-downward.

Needs the timeslice client (pip install pkg/client/python); no verl, ray or GPU.
Run:  python3 -m pytest tests/test_client_wiring.py -v
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import timeslice_verl.hooks as hooks_mod  # noqa: E402
import timeslice_verl.ns_downward as nsd  # noqa: E402
from timeslice import wiring  # noqa: E402
from timeslice_verl.hooks import TimesliceHooksMixin  # noqa: E402
from timeslice_verl.locks import PhaseLocks  # noqa: E402

JOB = "rl-c-4hd"
GROUP = "ns-c.rl-c-4hd.trainers"
LABELS = f'timeslice.io/donor="true"\ntimeslice.io/group="{GROUP}"\ntimeslice.io/job-id="{JOB}"'


class FakeClient:
    def __init__(self, target, job_id, group_id):
        self.target, self.job_id, self.group_id = target, job_id, group_id

    def acquire(self, group_id, timeout_sec=None):
        raise AssertionError("not reached")

    def release(self, group_id):
        pass

    def close(self):
        pass


class FakeRay:
    """Just enough of ray for ns_downward.ray_donor_podinfo."""

    def __init__(self, resources, result=None, exc=None):
        self.resources, self.result, self.exc = resources, result, exc
        self.remote_opts = self.task_opts = self.timeout = None

    def is_initialized(self):
        return True

    def cluster_resources(self):
        return self.resources

    def remote(self, **opts):
        self.remote_opts = opts
        ray = self

        class Task:
            def options(self, **o):
                ray.task_opts = o
                return self

            def remote(self):
                return "ref"

        return lambda fn: Task()

    def get(self, ref, timeout=None):
        self.timeout = timeout
        if self.exc is not None:
            raise self.exc
        return self.result


@pytest.fixture
def env(monkeypatch, tmp_path):
    for k in list(os.environ):
        if k.startswith("TIMESLICE_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("HOSTNAME", "rl-c-pod-0")
    monkeypatch.setattr(wiring, "_logged", False)
    monkeypatch.setattr(nsd, "_ray", lambda: None)
    empty = tmp_path / "empty"
    empty.mkdir()
    labelled = tmp_path / "labelled"
    labelled.mkdir()
    (labelled / "labels").write_text(LABELS)
    return {"mp": monkeypatch, "empty": str(empty), "labelled": str(labelled)}


def factory(target, job_id, group_id):
    return FakeClient(target, job_id, group_id)


class TestClientWiringKeep:
    def test_default_env_wiring(self, env, capsys):
        env["mp"].setenv("TIMESLICE_JOB_ID", "j")
        env["mp"].setenv("TIMESLICE_GROUP", "g")
        env["mp"].setenv("TIMESLICE_ORCH_ADDR", "o:1")
        env["mp"].setenv("TIMESLICE_PODINFO_DIR", env["labelled"])
        pl = PhaseLocks.from_env(client_factory=factory)
        assert pl.enabled
        assert (pl._client.target, pl._client.job_id, pl._client.group_id) == ("o:1", "j", "g")
        out = capsys.readouterr().out
        assert (
            "[timeslice] wiring mode=keep job=j group=g orch=o:1 source=job:env,group:env,orch:env "
            "pod=rl-c-pod-0 enabled=true" in out
        )
        PhaseLocks.from_env(client_factory=factory)
        assert "wiring mode=" not in capsys.readouterr().out  # once per process

    def test_noop_without_env_even_when_labelled(self, env):
        env["mp"].setenv("TIMESLICE_PODINFO_DIR", env["labelled"])
        assert not PhaseLocks.from_env(client_factory=factory).enabled

    def test_hooks_need_the_gate(self, env):
        env["mp"].setenv("TIMESLICE_PODINFO_DIR", env["labelled"])
        assert not hooks_mod.enabled()
        env["mp"].setenv("TIMESLICE_FULLY_ASYNC", "1")
        assert hooks_mod.enabled()

    def test_hook_init_does_not_resolve(self, env):
        env["mp"].setenv("TIMESLICE_FULLY_ASYNC", "1")
        mixin = TimesliceHooksMixin(client_factory=factory)
        assert mixin._wiring is None

    def test_without_client_installed_reads_env(self, env):
        env["mp"].setitem(sys.modules, "timeslice", None)  # import fails
        env["mp"].setenv("TIMESLICE_JOB_ID", "j")
        env["mp"].setenv("TIMESLICE_GROUP", "g")
        env["mp"].setenv("TIMESLICE_ORCH_ADDR", "o:1")
        pl = PhaseLocks.from_env(client_factory=factory)
        assert (pl._client.target, pl._client.job_id, pl._client.group_id) == ("o:1", "j", "g")


class TestClientWiringNsDownward:
    def select(self, env, podinfo):
        env["mp"].setenv("TIMESLICE_CLIENT_WIRING", "ns-downward")
        env["mp"].setenv("TIMESLICE_PODINFO_DIR", env[podinfo])

    def test_activates_without_gate(self, env):
        self.select(env, "labelled")
        assert hooks_mod.enabled()

    @pytest.mark.parametrize("off", ["0", "false", "no"])
    def test_gate_off_escape_hatch(self, env, off):
        self.select(env, "empty")
        env["mp"].setenv("TIMESLICE_FULLY_ASYNC", off)
        assert not hooks_mod.enabled()
        assert TimesliceHooksMixin(client_factory=factory)._wiring is None  # no fail fast when off

    def test_hook_init_resolves_labels(self, env, capsys):
        self.select(env, "labelled")
        mixin = TimesliceHooksMixin(client_factory=factory)
        out = capsys.readouterr().out
        assert (
            f"[timeslice] wiring mode=ns-downward job={JOB} group={GROUP} orch={wiring.DEFAULT_ORCH_ADDR} "
            "source=job:label,group:label,orch:default pod=rl-c-pod-0 enabled=true\n" in out
        )
        locks = mixin._get_locks()
        c = locks._client
        assert (c.target, c.job_id, c.group_id) == (wiring.DEFAULT_ORCH_ADDR, JOB, GROUP)
        assert out.count("wiring mode=") == 1

    def test_hook_init_fails_fast_when_unlabelled(self, env, capsys):
        self.select(env, "empty")
        with pytest.raises(wiring.WiringError):
            TimesliceHooksMixin(client_factory=factory)
        out = capsys.readouterr().out
        podinfo = os.path.join(env["empty"], "labels")
        assert (
            "[timeslice] FATAL wiring: pod=rl-c-pod-0 missing=timeslice.io/job-id,timeslice.io/group "
            f"podinfo={podinfo}: this process is not in a pod labelled timeslice.io/donor" in out
        )

    def test_phaselocks_fails_fast_when_unlabelled(self, env):
        self.select(env, "empty")
        with pytest.raises(wiring.WiringError):
            PhaseLocks.from_env(client_factory=factory)

    def test_env_override(self, env):
        self.select(env, "labelled")
        env["mp"].setenv("TIMESLICE_ORCH_ADDR", "127.0.0.1:50051")
        c = PhaseLocks.from_env(client_factory=factory)._client
        assert (c.target, c.job_id, c.group_id) == ("127.0.0.1:50051", JOB, GROUP)

    def test_needs_the_client(self, env):
        self.select(env, "labelled")
        env["mp"].setitem(sys.modules, "timeslice", None)
        with pytest.raises(ImportError):
            PhaseLocks.from_env(client_factory=factory)

    def test_ray_fallback_from_samplers_pod(self, env, capsys):
        self.select(env, "empty")
        donor = {
            "pod": "rl-c-trainers-0",
            "path": "/etc/timeslice/podinfo/labels",
            "labels": wiring.parse_labels(LABELS),
            "error": None,
        }
        fake = FakeRay({"trainer_node": 100.0, "CPU": 48.0}, result=donor)
        env["mp"].setattr(nsd, "_ray", lambda: fake)
        mixin = TimesliceHooksMixin(client_factory=factory, donor_resource="trainer_node")
        assert fake.remote_opts == {"num_cpus": 0, "resources": {"trainer_node": nsd.PROBE_RESOURCE_SHARE}}
        assert fake.task_opts == {"scheduling_strategy": "DEFAULT"}
        assert fake.timeout == nsd.PROBE_TIMEOUT_SEC
        w = mixin._wiring
        assert (w.job_id, w.group, w.pod, w.via) == (JOB, GROUP, "rl-c-trainers-0", "ray:trainer_node")
        assert "pod=rl-c-trainers-0 enabled=true via=ray:trainer_node\n" in capsys.readouterr().out

    def test_ray_fallback_resource_missing_fails_fast(self, env, capsys):
        self.select(env, "empty")
        fake = FakeRay({"CPU": 48.0})
        env["mp"].setattr(nsd, "_ray", lambda: fake)
        with pytest.raises(wiring.WiringError):
            TimesliceHooksMixin(client_factory=factory, donor_resource="trainer_node")
        assert fake.remote_opts is None
        assert "no Ray node has resource trainer_node" in capsys.readouterr().out

    def test_ray_fallback_task_error_fails_fast(self, env, capsys):
        self.select(env, "empty")
        env["mp"].setattr(nsd, "_ray", lambda: FakeRay({"trainer_node": 1.0}, exc=TimeoutError("60s")))
        with pytest.raises(wiring.WiringError):
            TimesliceHooksMixin(client_factory=factory)
        assert "donor probe on Ray resource trainer_node failed: TimeoutError: 60s" in capsys.readouterr().out

    def test_donor_resource_selection(self, env):
        cfg = {"ray_pg_extra_resources": {"trainer_pool": {"gpu_host": 1}, "rollout_pool": {"rollout_node": 1}}}
        assert nsd.resource_from_config(cfg) == "gpu_host"
        assert nsd.resource_from_config(None) is None
        assert nsd.resource_from_config({}) is None
        assert nsd.donor_resource(None) == "trainer_node"
        assert nsd.donor_resource("gpu_host") == "gpu_host"
        env["mp"].setenv("TIMESLICE_DONOR_RAY_RESOURCE", "override")
        assert nsd.donor_resource("gpu_host") == "override"
