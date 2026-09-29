"""D-NS-16 ns-downward: keep identity resolution inside a donor (trainers) pod.

The timeslice trainer driver actor is CPU-only, so Ray may place it on any
worker, including a samplers pod that has no donor labels and no podinfo
volume. When this pod's labels lack the identity, ``ray_donor_podinfo`` runs a
zero-CPU Ray task pinned to the donor Ray resource (the trainer_pool resource
of ``ray_pg_extra_resources``, default ``trainer_node``). The task reads the
podinfo labels of the donor pod it lands on and returns them, and the client
uses that pod's identity. No manifest line and no llm-d-async change.

Nothing here imports ray or verl at module import time.
"""

import os

from timeslice_verl.locks import _log

# Override for the donor Ray resource; otherwise it comes from the trainer config.
ENV_DONOR_RESOURCE = "TIMESLICE_DONOR_RAY_RESOURCE"
DEFAULT_DONOR_RESOURCE = "trainer_node"
PROBE_TIMEOUT_SEC = 60.0
# A tiny share of the resource: the donor workers advertise it in bulk
# (rayStartParams resources, for example trainer_node: 100).
PROBE_RESOURCE_SHARE = 0.001


def resource_from_config(config) -> str | None:
    """First resource of ``ray_pg_extra_resources.trainer_pool`` in the verl
    config (a dict or an OmegaConf DictConfig), or None."""
    try:
        pool = config.get("ray_pg_extra_resources", {}).get("trainer_pool", {})
        for key in pool:
            return str(key)
    except (AttributeError, TypeError):
        return None
    return None


def donor_resource(from_config: str | None = None, environ=None) -> str:
    env = os.environ if environ is None else environ
    return env.get(ENV_DONOR_RESOURCE, "").strip() or from_config or DEFAULT_DONOR_RESOURCE


def _read_podinfo_on_donor() -> dict:
    """Runs in a Ray worker on a donor pod: that pod's podinfo labels."""
    from timeslice import wiring

    info = wiring.read_podinfo()
    return {"pod": info.pod, "path": info.path, "labels": info.labels, "error": info.error}


def _ray():
    try:
        import ray
    except ImportError:
        return None
    return ray if ray.is_initialized() else None


def ray_donor_podinfo(resource: str, ray_module=None, timeout_sec: float = PROBE_TIMEOUT_SEC):
    """A ``donor_podinfo`` callable for timeslice.wiring.resolve. It returns
    None (and resolve then fails fast) when Ray is not running, no node has
    ``resource``, or the task fails or times out."""

    def probe():
        from timeslice import wiring

        ray = ray_module if ray_module is not None else _ray()
        if ray is None:
            return None
        if ray.cluster_resources().get(resource, 0) <= 0:
            _log(f"WARNING wiring: no Ray node has resource {resource}; cannot reach a donor pod")
            return None
        # scheduling_strategy="DEFAULT": do not inherit the caller's placement group.
        task = ray.remote(num_cpus=0, resources={resource: PROBE_RESOURCE_SHARE})(_read_podinfo_on_donor)
        try:
            out = ray.get(task.options(scheduling_strategy="DEFAULT").remote(), timeout=timeout_sec)
        except Exception as e:  # any Ray failure: fall through to the FATAL line
            _log(f"WARNING wiring: donor probe on Ray resource {resource} failed: {type(e).__name__}: {e}")
            return None
        return wiring.PodInfo(via=f"ray:{resource}", **out)

    return probe
