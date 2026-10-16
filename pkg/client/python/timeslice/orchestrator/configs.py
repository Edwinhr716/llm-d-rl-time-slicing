"""Helpers for building the per-job backend config passed to the orchestrator.

The orchestrator accepts a ``JobBackendConfig`` on ``acquire``/``release``: the
subset of the snapshot agent's ``BackendConfig`` a client may choose per job.
Process targets are never part of it (the agent discovers PIDs from the job's
pods), nor are experimental backends.

These mirror ``timeslice.snapshot_agent.configs`` but build the orchestrator's
own message type; the two packages stay independently importable.
"""

from collections.abc import Sequence

from timeslice.orchestrator._generated import pb2

BackendConfigLike = pb2.JobBackendConfig | str | None

_APPS = {
    "vllm": pb2.APP_VLLM,
    "sglang": pb2.APP_SGLANG,
}

_SUSPEND_MODES = {
    "offload": pb2.SUSPEND_MODE_OFFLOAD,
    "discard": pb2.SUSPEND_MODE_DISCARD,
}

# Backends that take no options and can be named by a plain string.
_BODYLESS_BACKENDS = ("cuda", "tpu", "app_channel")


def _suspend_mode(mode: str | int | None) -> int:
    """None -> UNSPECIFIED (workload/application default); accepts friendly
    strings ("offload"/"discard") or the raw enum value."""
    if mode is None:
        return pb2.SUSPEND_MODE_UNSPECIFIED
    if isinstance(mode, str):
        try:
            return _SUSPEND_MODES[mode.lower()]
        except KeyError:
            raise ValueError(
                f"unknown suspend mode {mode!r}; expected one of {sorted(_SUSPEND_MODES)}"
            )
    pb2.SuspendMode.Name(mode)  # raises ValueError if invalid
    return mode


def _tags(tags: Sequence[str] | None) -> list:
    if isinstance(tags, str):
        raise ValueError("tags must be a sequence of non-empty strings")
    tags = list(tags or [])
    if any(not isinstance(t, str) or not t for t in tags):
        raise ValueError(f"tags must be non-empty strings, got {tags!r}")
    return tags


def cuda_config() -> pb2.JobBackendConfig:
    """Selects the cuda (cuda-checkpoint) backend. PIDs are discovered by the
    agent from the job's pods."""
    return pb2.JobBackendConfig(cuda=pb2.CudaBackend())


def tpu_config() -> pb2.JobBackendConfig:
    """Selects the TPU (libtpu) backend. PIDs are discovered by the agent from
    the job's pods."""
    return pb2.JobBackendConfig(tpu=pb2.TpuBackend())


def app_endpoint_config(
    app: str | int,
    endpoints: Sequence[str],
    mode: str | int | None = None,
    tags: Sequence[str] | None = None,
) -> pb2.JobBackendConfig:
    """Selects the app_endpoint backend, which suspends/resumes an
    application-aware workload through its HTTP API.

    ``app`` is "vllm", "sglang", or an App enum value. ``endpoints`` targets
    the server(s), e.g. ``["http://localhost:8000"]``. ``mode`` is "offload",
    "discard", a SuspendMode value, or None for the application's default.
    ``tags`` selects regions, e.g. ``["weights", "kv_cache"]``; None or empty
    means the application's full default region set.
    """
    if isinstance(app, str):
        try:
            app = _APPS[app.lower()]
        except KeyError:
            raise ValueError(f"unknown app {app!r}; expected one of {sorted(_APPS)}")
    else:
        pb2.App.Name(app)  # raises ValueError if invalid
    if app == pb2.APP_UNSPECIFIED:
        raise ValueError(f"app must be specified; expected one of {sorted(_APPS)}")
    if isinstance(endpoints, str):
        raise ValueError("endpoints must be a sequence of non-empty endpoint URLs")
    endpoints = list(endpoints)
    if not endpoints or any(not isinstance(e, str) or not e for e in endpoints):
        raise ValueError("at least one non-empty endpoint URL is required")
    return pb2.JobBackendConfig(
        app_endpoint=pb2.AppEndpointBackend(
            app=app, endpoints=endpoints, mode=_suspend_mode(mode), tags=_tags(tags)
        )
    )


def vllm_config(
    endpoints: Sequence[str],
    mode: str | int | None = None,
    tags: Sequence[str] | None = None,
) -> pb2.JobBackendConfig:
    """app_endpoint_config preset for vLLM's sleep/wake API."""
    return app_endpoint_config("vllm", endpoints, mode=mode, tags=tags)


def sglang_config(
    endpoints: Sequence[str],
    mode: str | int | None = None,
    tags: Sequence[str] | None = None,
) -> pb2.JobBackendConfig:
    """app_endpoint_config preset for SGLang's memory-occupation API."""
    return app_endpoint_config("sglang", endpoints, mode=mode, tags=tags)


def app_channel_config(
    mode: str | int | None = None,
    tags: Sequence[str] | None = None,
) -> pb2.JobBackendConfig:
    """Selects the app_channel backend, which suspends/resumes an
    application-aware workload through the workload channel it registered
    with the node-local snapshot agent. The workload is addressed by job id.

    ``mode`` is "offload", "discard", a SuspendMode value, or None for the
    workload's registered default. ``tags`` selects regions.
    """
    return pb2.JobBackendConfig(
        app_channel=pb2.AppChannelBackend(mode=_suspend_mode(mode), tags=_tags(tags))
    )


def coerce_backend_config(value: BackendConfigLike) -> pb2.JobBackendConfig | None:
    """Normalizes a user-supplied backend config.

    Accepts None (no preference), a ``JobBackendConfig``, or one of the
    strings "cuda", "tpu", "app_channel" for the option-less backends (handy
    when the choice comes from an environment variable).
    """
    if value is None:
        return None
    if isinstance(value, pb2.JobBackendConfig):
        return value
    if isinstance(value, str):
        name = value.lower()
        if name == "cuda":
            return cuda_config()
        if name == "tpu":
            return tpu_config()
        if name == "app_channel":
            return app_channel_config()
        raise ValueError(
            f"unknown backend {value!r}; expected one of {list(_BODYLESS_BACKENDS)} "
            "or a JobBackendConfig built with app_endpoint_config/vllm_config/sglang_config"
        )
    raise TypeError(
        "backend_config must be None, a JobBackendConfig, or one of "
        f"{list(_BODYLESS_BACKENDS)}; got {type(value).__name__}"
    )
