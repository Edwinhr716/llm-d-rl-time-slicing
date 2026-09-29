"""Client wiring: where a donor's timeslice client finds its job id, group and
orchestrator address.

The mode comes from TIMESLICE_CLIENT_WIRING (default ``keep``):

keep
    TIMESLICE_JOB_ID, TIMESLICE_GROUP and TIMESLICE_ORCH_ADDR from the
    environment, nothing else. No startup check: if one is missing the
    resolution is ``enabled=False`` and the caller runs as a no-op.
ns-downward
    Job id and group from the pod's own labels ``timeslice.io/job-id`` and
    ``timeslice.io/group``, read from the downward API file ``labels`` in the
    podinfo directory (TIMESLICE_PODINFO_DIR, default /etc/timeslice/podinfo).
    The address defaults to the orchestrator Service DNS name
    (DEFAULT_ORCH_ADDR). Each TIMESLICE_* variable above, when set and not
    empty, overrides its own field. If the job id or group is still missing,
    resolve() raises WiringError: callers let it propagate so the process
    fails at startup instead of running unlocked.

resolve() is pure: it reads only the mapping it is given and the podinfo
file. It never opens a network connection or looks up DNS.
"""

import dataclasses
import json
import os
import socket
from collections.abc import Callable, Mapping

ENV_WIRING = "TIMESLICE_CLIENT_WIRING"
MODE_KEEP = "keep"
MODE_NS_DOWNWARD = "ns-downward"
MODES = (MODE_KEEP, MODE_NS_DOWNWARD)

ENV_JOB_ID = "TIMESLICE_JOB_ID"
ENV_GROUP = "TIMESLICE_GROUP"
ENV_ORCH_ADDR = "TIMESLICE_ORCH_ADDR"
ENV_PODINFO_DIR = "TIMESLICE_PODINFO_DIR"

# The keys the orchestrator and the snapshot agent select pods on.
LABEL_JOB_ID = "timeslice.io/job-id"
LABEL_GROUP = "timeslice.io/group"

# Downward API volume mount path and item path ("labels" -> metadata.labels).
DEFAULT_PODINFO_DIR = "/etc/timeslice/podinfo"
PODINFO_LABELS_FILE = "labels"

# The orchestrator chart's Service (release "timeslice" in "timeslice-system").
DEFAULT_ORCH_ADDR = "timeslice-timesliceorchestrator.timeslice-system.svc:50051"

# Values of Wiring.source.
SOURCE_ENV = "env"
SOURCE_LABEL = "label"
SOURCE_DEFAULT = "default"
SOURCE_NONE = "none"


class WiringError(RuntimeError):
    """ns-downward found no identity for this process. Fail at startup."""


@dataclasses.dataclass(frozen=True)
class PodInfo:
    """The labels of one pod, as read from its downward API file."""

    pod: str
    path: str
    labels: dict | None  # None when the file could not be read
    error: str | None = None
    via: str = "local"  # how the pod was reached: local, or for example ray:<resource>


@dataclasses.dataclass(frozen=True)
class Wiring:
    """What the client resolved. ``source`` maps job_id, group and orch_addr to
    env, label, default or none. ``pod`` is the pod whose labels were used (or
    this pod), ``via`` says how that pod was reached (local or a callback)."""

    mode: str
    job_id: str | None
    group: str | None
    orch_addr: str | None
    enabled: bool
    source: dict
    pod: str = ""
    podinfo: str = ""
    via: str = "local"

    def log_line(self) -> str:
        s = self.source
        line = (
            f"wiring mode={self.mode} job={self.job_id} group={self.group} "
            f"orch={self.orch_addr} source=job:{s.get('job_id', SOURCE_NONE)},"
            f"group:{s.get('group', SOURCE_NONE)},"
            f"orch:{s.get('orch_addr', SOURCE_NONE)} pod={self.pod} "
            f"enabled={'true' if self.enabled else 'false'}"
        )
        if self.via != "local":
            line += f" via={self.via}"
        return line

    def as_dict(self) -> dict:
        return dataclasses.asdict(self)


def _log(msg: str) -> None:
    print(f"[timeslice] {msg}", flush=True)


_logged = False


def log_once(wiring: Wiring) -> None:
    """Print the resolution line once per process."""
    global _logged
    if _logged:
        return
    _logged = True
    _log(wiring.log_line())


def pod_name(environ: Mapping[str, str]) -> str:
    return environ.get("HOSTNAME") or socket.gethostname()


def selected_mode(environ: Mapping[str, str]) -> str:
    """The mode named by TIMESLICE_CLIENT_WIRING (default keep)."""
    value = environ.get(ENV_WIRING, "").strip().lower() or MODE_KEEP
    if value not in MODES:
        raise WiringError(
            f"FATAL wiring: {ENV_WIRING}={value!r} is not one of {', '.join(MODES)}"
        )
    return value


def parse_labels(text: str) -> dict:
    """Parse the kubelet's downward API labels file: ``key="value"`` per line,
    Go-quoted values, the last line possibly without a newline."""
    labels = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] == '"':
            try:
                value = json.loads(value)
            except ValueError:
                value = value[1:-1]
        labels[key.strip()] = value
    return labels


def read_podinfo(
    environ: Mapping[str, str] | None = None, podinfo_dir: str | None = None
) -> PodInfo:
    """Read this pod's labels from the podinfo directory."""
    env = os.environ if environ is None else environ
    directory = podinfo_dir or env.get(ENV_PODINFO_DIR) or DEFAULT_PODINFO_DIR
    path = os.path.join(directory, PODINFO_LABELS_FILE)
    pod = pod_name(env)
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        reason = f"{type(e).__name__}: {e.strerror or e}"
        return PodInfo(pod=pod, path=path, labels=None, error=reason)
    return PodInfo(pod=pod, path=path, labels=parse_labels(text))


def _env(environ: Mapping[str, str], name: str) -> str | None:
    value = environ.get(name, "").strip()
    return value or None


def resolve(
    environ: Mapping[str, str] | None = None,
    podinfo_dir: str | None = None,
    mode: str | None = None,
    donor_podinfo: Callable[[], PodInfo | None] | None = None,
) -> Wiring:
    """Resolve the client identity for ``mode`` (default: TIMESLICE_CLIENT_WIRING).

    ``donor_podinfo`` (ns-downward only) is called when this pod's labels lack
    the identity; it may return the PodInfo of a donor pod reached another way
    (for example a Ray task pinned to the donor's resource).
    """
    env = os.environ if environ is None else environ
    mode = selected_mode(env) if mode is None else mode
    if mode == MODE_KEEP:
        return _resolve_keep(env)
    if mode == MODE_NS_DOWNWARD:
        return _resolve_ns_downward(env, podinfo_dir, donor_podinfo)
    raise WiringError(f"FATAL wiring: unknown mode {mode!r}")


# ------------------------------------------------------------- option: keep


def _resolve_keep(env: Mapping[str, str]) -> Wiring:
    # Raw values, exactly what the integrations read before this option existed.
    job_id = env.get(ENV_JOB_ID)
    group = env.get(ENV_GROUP)
    orch_addr = env.get(ENV_ORCH_ADDR)
    return Wiring(
        mode=MODE_KEEP,
        job_id=job_id,
        group=group,
        orch_addr=orch_addr,
        enabled=bool(job_id and orch_addr and group),
        source={
            "job_id": SOURCE_ENV if job_id else SOURCE_NONE,
            "group": SOURCE_ENV if group else SOURCE_NONE,
            "orch_addr": SOURCE_ENV if orch_addr else SOURCE_NONE,
        },
        pod=pod_name(env),
    )


# ------------------------------------------------------ option: ns-downward

_FIELDS = (
    ("job_id", "job", ENV_JOB_ID, LABEL_JOB_ID),
    ("group", "group", ENV_GROUP, LABEL_GROUP),
)


def _identity_missing(env: Mapping[str, str], info: PodInfo) -> list:
    labels = info.labels or {}
    missing = []
    for _, _, env_name, label in _FIELDS:
        if not _env(env, env_name) and not labels.get(label, "").strip():
            missing.append(label)
    return missing


def _resolve_ns_downward(
    env: Mapping[str, str],
    podinfo_dir: str | None,
    donor_podinfo: Callable[[], PodInfo | None] | None,
) -> Wiring:
    local = read_podinfo(env, podinfo_dir)
    info, via = local, "local"
    missing = _identity_missing(env, local)
    if missing and donor_podinfo is not None:
        remote = donor_podinfo()
        if remote is not None and not _identity_missing(env, remote):
            info, missing = remote, []
            via = remote.via if remote.via != "local" else "donor"
    if missing:
        raise WiringError(
            f"FATAL wiring: pod={local.pod} missing={','.join(missing)} "
            f"podinfo={local.path}: this process is not in a pod labelled "
            "timeslice.io/donor (is the trainers worker group labelled, and are "
            "trainer actors pinned to it?)"
        )

    labels = info.labels or {}
    values, source = {}, {}
    for fld, short, env_name, label in _FIELDS:
        from_env = _env(env, env_name)
        from_label = labels.get(label, "").strip() or None
        if from_env:
            values[fld], source[fld] = from_env, SOURCE_ENV
            if from_label and from_label != from_env:
                _log(
                    f"WARNING wiring conflict field={short} env={from_env} "
                    f"label={from_label}"
                )
        else:
            values[fld], source[fld] = from_label, SOURCE_LABEL
    addr = _env(env, ENV_ORCH_ADDR)
    values["orch_addr"] = addr or DEFAULT_ORCH_ADDR
    source["orch_addr"] = SOURCE_ENV if addr else SOURCE_DEFAULT
    return Wiring(
        mode=MODE_NS_DOWNWARD,
        enabled=True,
        source=source,
        pod=info.pod,
        podinfo=info.path,
        via=via,
        **values,
    )


def main() -> int:
    """``python -m timeslice.wiring``: print the resolution as JSON; exit 1
    with the FATAL line when ns-downward finds no identity."""
    try:
        wiring = resolve()
    except WiringError as e:
        _log(str(e))
        return 1
    print(json.dumps(wiring.as_dict()), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
