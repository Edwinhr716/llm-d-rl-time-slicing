#!/usr/bin/env python3
r"""Node-level time-slicing supervisor ("approach 7").

This is images/shadow-vllm-supervisor/supervisor.py moved OUT of the tenant
pod and up to a per-node DaemonSet. It keeps every hard-won behaviour of the
in-pod supervisor — the 0.5 s waiter poll, the drain-before-yield that works
around vllm#28714, the LOCAL_SLEEP escape hatch, the workload-channel
registration — and drops exactly one: it does not launch vLLM. The tenant runs
the STOCK vllm/vllm-openai:v0.9.2 image with `vllm serve` as pod args, which is
the entire point: a time-sliced batch tenant should not need a rebuilt image to
be a polite citizen.

Two things replace the things that only worked from inside the pod:

  1. Readiness. The in-pod supervisor gated the Service endpoint by touching
     and removing /tmp/serving under an exec readinessProbe. From outside the
     pod there is no /tmp to touch, so we use the mechanism Kubernetes provides
     for exactly this: the tenant declares
       spec.readinessGates: [{conditionType: "timeslice.io/serving"}]
     and we PATCH that condition on pod status. Pod Ready = ContainersReady AND
     every readiness gate, so the pod is an endpoint only when vLLM answers
     /health AND the platform says it holds the GPU. A gate whose condition is
     absent counts as False, so a supervisor that has not started yet, or has
     crashed before ever patching, leaves the tenant out of the Service. It
     fails closed, which is the correct direction for the failure we care
     about (dispatching into a frozen engine).

  2. The dispatch-budget RISING edge. See the DISPATCH BUDGET OWNERSHIP block
     below; this is the crux of the whole component.

Deployment shape (justified, because both flags are privileges):
  - hostPID: NOT used. The in-pod supervisor needed to be PID 1 because it
    fork/exec'd `vllm serve` and had to reap it. We launch nothing and signal
    nothing — every interaction with the engine is HTTP to the pod IP and
    every interaction with the process's CUDA state goes through the snapshot
    agent. Asking for hostPID here would buy nothing and would hand a
    node-wide process view to a component that only needs the API server.
  - hostNetwork: used. The self-probe in step (e) of the reacquire sequence is
    the load-bearing observation in this file, and it is only meaningful if it
    traverses the node's real kube-proxy rules rather than something that
    could short-circuit. In the host netns a ClusterIP connect is DNAT'd by
    the same host iptables/IPVS chains that a dispatch arriving at this node
    would hit, and a failure is unambiguously "the route is not programmed"
    rather than "the CNI is not ready". It also makes TIMESLICE_AGENT_ADDR
    (hostIP:9001, where the snapshot agent listens on hostNetwork) a literal
    node-local address. Cost: the manifest must set
    dnsPolicy: ClusterFirstWithHostNet or cluster DNS names do not resolve.

LOGGING IS AN INTERFACE, in two layers, and both are load-bearing:

  - New, structured. Every state transition is one flushed line:
      [node-supervisor] <RFC3339 UTC ts> job=<id> event=<name> k=v k=v ...
    matched by ^\[node-supervisor\] (\S+) job=(\S+) event=(\S+)(.*)$ . Events:
    condition_patched, endpoint_notready, drain_complete, lock_released,
    lock_reacquired, vllm_healthy_local, clusterip_probe_ok, budget_set.

  - Old, verbatim. Four phrases from supervisor.py are reproduced character for
    character so that the EXISTING, UNMODIFIED analysis scripts parse a run of
    this component: "lock acquired", "lock reacquired",
    "trainer is waiting - yielding GPU", "vLLM is up", plus
    "workload registered with agent ...". See legacy() for why these are a wire
    format rather than log text.
"""
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from kubernetes import client as k8s
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException

import redis as redis_lib

try:
    from timeslice import OrchestratorClient
except ImportError:  # client class was renamed upstream; API identical
    from timeslice import TimeSliceOrchestratorClient as OrchestratorClient
from timeslice.snapshot_agent import register_workload

# --- node / cluster identity -------------------------------------------------
NODE_NAME = os.environ["NODE_NAME"]                  # fieldRef spec.nodeName
NAMESPACE = os.environ.get("TENANT_NAMESPACE", "default")
ORCH = os.environ["TIMESLICE_ORCH_ADDR"]
AGENT = os.environ["TIMESLICE_AGENT_ADDR"]           # node-local, <hostIP>:9001

# --- tenant discovery --------------------------------------------------------
# Existence selector on the group label: any pod on this node that declares
# itself part of a time-slice group is ours to supervise. The job-id label
# gives us the lock identity.
GROUP_LABEL = os.environ.get("TIMESLICE_GROUP_LABEL", "timeslice.io/group")
JOB_ID_LABEL = os.environ.get("TIMESLICE_JOB_ID_LABEL", "timeslice.io/job-id")
DISCOVERY_POLL_S = float(os.environ.get("DISCOVERY_POLL_SECONDS", "1.0"))

# --- tenant engine -----------------------------------------------------------
VLLM_PORT = int(os.environ.get("VLLM_PORT", "8000"))
POLL_S = float(os.environ.get("WAITER_POLL_SECONDS", "0.5"))
# Same semantics as supervisor.py's LOCAL_SLEEP, and the same warning: sleep
# the engine ourselves before releasing the lock, OR leave the snapshot agent's
# cuda backend as the single owner of the process's CUDA state. Default 0 to
# match what manifests/10-shadow-vllm.yaml sets today — vLLM sleep and
# cuda-checkpoint on the same process conflict, and the engine dies during the
# agent's RESTORE of an already-slept process. Set to 1 only on platform builds
# whose agent resolves THIS job to the app-channel backend per job rather than
# one node-wide DEFAULT_BACKEND.
LOCAL_SLEEP = os.environ.get("LOCAL_SLEEP", "0") == "1"

# --- readiness gate ----------------------------------------------------------
# Must equal the tenant's spec.readinessGates[].conditionType.
GATE_CONDITION = os.environ.get("READINESS_GATE_CONDITION", "timeslice.io/serving")
# How long to wait for the API server to reflect Ready=False after we patch the
# gate to False. Bounded: see withdraw_endpoint() for why overrunning it is not
# a correctness problem here.
NOTREADY_TIMEOUT_S = float(os.environ.get("NOTREADY_TIMEOUT_SECONDS", "10.0"))

# --- ClusterIP self-probe ----------------------------------------------------
# The URL a batch dispatch would actually use. Probing it proves the route is
# programmed; see reacquire() step (e) for exactly how much that proves.
SERVICE_PROBE_URL = os.environ.get(
    "TIMESLICE_SERVICE_PROBE_URL",
    "http://shadow-vllm.default.svc.cluster.local:8000/health")
SERVICE_PROBE_TIMEOUT_S = float(
    os.environ.get("TIMESLICE_SERVICE_PROBE_TIMEOUT_SECONDS", "30.0"))

# --- llm-d-async dispatch budget --------------------------------------------
REDIS_ADDR = os.environ.get("DISPATCH_BUDGET_REDIS_ADDR",
                            "redis.default.svc.cluster.local:6379")
BUDGET_KEY = os.environ.get("DISPATCH_BUDGET_KEY", "dispatch-gate-budget")

_TS_RE_DOC = r"^\[node-supervisor\] (\S+) job=(\S+) event=(\S+)(.*)$"  # for results/ parsers


def _now():
    # Wall clock, RFC3339, microseconds, always UTC. Durations reported in the
    # k=v payload are measured with time.monotonic() so that an NTP step during
    # a 30 s quantum cannot produce a negative drain time in the analysis.
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def event(job, name, **kv):
    """One machine-parsable state-transition line. Nothing else in this file
    should print; freeform commentary goes through note()."""
    tail = "".join(f" {k}={v}" for k, v in kv.items())
    print(f"[node-supervisor] {_now()} job={job} event={name}{tail}", flush=True)


def note(job, msg):
    """Human commentary. Deliberately NOT in the event= shape so that a
    regex over event lines cannot accidentally match a log message."""
    print(f"[node-supervisor] {_now()} job={job} -- {msg}", flush=True)


def legacy(job, phrase):
    """A phrase from supervisor.py, reproduced VERBATIM.

    The analysis scripts in results/ predate this component and match the old
    supervisor's log by substring, not by structure — results/redis-signal-gate/
    cycle.py:67-78 builds every serving window from exactly two of them:

        if "lock reacquired" in body or "lock acquired" in body: open_at = ts
        elif "trainer is waiting - yielding GPU" in body ...: close the window

    Its line regex is `^(\\S+)\\s+(.*)$` over kubectl --timestamps output, so
    the body it searches is our whole line including the [node-supervisor]
    prefix and job id; containment is all that matters. These phrases are
    therefore a WIRE FORMAT and not log text: do not reword them, do not
    reflow them, do not "fix" the spaced ASCII hyphen in the yield phrase. They
    are emitted on their own lines rather than appended to the k=v event lines
    so that the structured parsers stay structured, and the event names next to
    them deliberately use underscores (event=lock_reacquired,
    event=yield_requested) so a single transition can never be counted twice.
    """
    print(f"[node-supervisor] {_now()} job={job} {phrase}", flush=True)


# =============================================================================
# DISPATCH BUDGET OWNERSHIP — read this before touching anything below.
#
# llm-d-async's `redis` dispatch gate reads ONE key (default
# "dispatch-gate-budget") as a budget in [0,1]: <=0 refuses dispatch before the
# broker even dequeues, >0 admits. Two components write that key and each is
# monotone in exactly ONE direction:
#
#   ORCHESTRATOR -> writes ONLY "0". It does so synchronously inside
#   GetGroupStatus, before returning the waiter_queue_depth>0 that tells this
#   supervisor to drain. The gate is therefore already shut before the drain
#   starts, and a 1 Hz idempotent refresh keeps it shut (and recreates the key
#   if Redis evicts it — the gate FAILS OPEN on an absent key, which is the
#   30%-loss regime).
#
#   THIS SUPERVISOR -> writes ONLY "1". Exactly once per serving window, and
#   only after the ClusterIP self-probe has succeeded.
#
# We do not write "0". Ever. There is no code path in this file that can. Two
# independent reasons, either one sufficient:
#   1. We would always be LATER than the orchestrator — it knows a yield is
#      coming before we do, because it is the thing that decided it.
#   2. We would fight its 1 Hz refresh: our "0" and its "0" are the same value
#      today, but the moment the orchestrator's policy grows any nuance (a
#      partial budget, a per-queue key) a second writer with its own opinion
#      turns an idempotent refresh into a race.
#
# Why the supervisor owns the RISING edge specifically: the orchestrator's
# notion of "serving" is GroupSnapshot.ServingSince(), which fires at CUDA
# context restore — measured 0.74-1.84 s before kube-proxy will route to the
# pod. AP has no cache and no smoothing on this gate (Budget() is a plain GET),
# so it empties the whole blackout backlog into that gap: 59 of 140 requests
# destroyed in a measured run. --dispatch-budget-open-delay=2s is the constant
# that papers over it. This component is the real fix the hold-down stands in
# for: a rising edge published by something that has positively observed the
# route, rather than a timer.
# =============================================================================
def budget_open(job, key, r):
    r.set(key, "1")


# --- Kubernetes --------------------------------------------------------------
try:
    k8s_config.load_incluster_config()
except Exception:
    k8s_config.load_kube_config()
CORE = k8s.CoreV1Api()


def list_tenants():
    """Pods on THIS node that carry the group label AND opt in to supervision
    by declaring our readiness gate.

    The readiness gate IS the opt-in, deliberately — there is no second label to
    keep in sync. A pod that lists conditionType timeslice.io/serving in
    spec.readinessGates is by definition a pod that cannot become an endpoint
    until this process patches that condition, which is exactly the set of pods
    this process is responsible for. Anything else is a co-tenant.

    That distinction is load-bearing, not hygiene. The trainer shares the group
    label and the node with the batch tenant — `timeslice.io/group: trainers` is
    on rl-head too, because it is how the platform assigns group membership. On
    the label alone we adopted rl-head as a tenant on the first run, and the
    consequences would have been: a second lock client for job-id rl-trainer
    racing the trainer's own client, a waiter enqueued against the group that
    nothing would ever satisfy, and this process draining and patching a pod
    running FSDP. The trainer supervises itself through the verl integration; it
    declares no readiness gate because nothing routes traffic to it.

    PoC choice: a 1 s LIST poll, not a watch. Production wants a watch — a
    field-selected watch is one long-lived connection instead of a LIST against
    the API server per node per second, and it delivers a deletion the instant
    it happens rather than up to a poll period late. We poll because the poll
    loop has no resync/410-Gone/relist state machine to get wrong, and at one
    time-slicing node with one tenant the API-server cost is irrelevant. The
    latency does not matter either: discovery latency only affects how fast a
    NEW tenant is picked up, never the yield or reacquire sequences, which run
    entirely inside an already-established per-tenant thread.
    """
    pods = CORE.list_namespaced_pod(
        namespace=NAMESPACE,
        field_selector=f"spec.nodeName={NODE_NAME}",
        label_selector=GROUP_LABEL,
    )
    out = {}
    for p in pods.items:
        labels = p.metadata.labels or {}
        job_id = labels.get(JOB_ID_LABEL)
        group = labels.get(GROUP_LABEL)
        if not job_id or not group:
            continue
        if p.metadata.deletion_timestamp is not None:
            continue
        gates = [g.condition_type for g in (p.spec.readiness_gates or [])]
        if GATE_CONDITION not in gates:
            continue
        out[p.metadata.name] = (job_id, group)
    return out


def get_pod(name):
    try:
        return CORE.read_namespaced_pod(name=name, namespace=NAMESPACE)
    except ApiException as e:
        if e.status == 404:
            return None
        raise


def patch_gate(pod_name, value):
    """Set the readiness-gate condition on the tenant pod's status.

    Strategic merge patch keyed on `type` is the correct mechanism:
    PodStatus.conditions carries patchMergeKey=type / patchStrategy=merge, so a
    one-element list merges into (or creates) the matching condition and leaves
    Ready / ContainersReady / PodScheduled — which the kubelet owns — untouched.
    A plain JSON merge patch would REPLACE the whole conditions array and stamp
    on the kubelet's entries; a JSON patch would need an index we do not know.
    """
    cond = {
        "type": GATE_CONDITION,
        "status": value,
        "lastTransitionTime": _now(),
        "reason": "TimesliceLock",
        "message": ("holds the group lock and the ClusterIP route is programmed"
                    if value == "True" else
                    "yielding the GPU to a waiting job"),
    }
    body = {"status": {"conditions": [cond]}}
    try:
        CORE.patch_namespaced_pod_status(
            name=pod_name, namespace=NAMESPACE, body=body,
            _content_type="application/strategic-merge-patch+json")
    except TypeError:
        # kubernetes<24 has no _content_type kwarg. Its select_header_content_type
        # only picks json-patch+json for a LIST body, so a dict still goes out as
        # a merge patch; keep the call working rather than pin harder than needed.
        CORE.patch_namespaced_pod_status(
            name=pod_name, namespace=NAMESPACE, body=body)


# --- tenant HTTP -------------------------------------------------------------
def http(ip, method, path, timeout):
    req = urllib.request.Request(
        f"http://{ip}:{VLLM_PORT}{path}",
        data=b"" if method == "POST" else None, method=method)
    return urllib.request.urlopen(req, timeout=timeout)


def inflight(ip):
    """running + waiting requests, from /metrics (None if unreadable)."""
    try:
        with http(ip, "GET", "/metrics", timeout=2) as r:
            text = r.read().decode()
        n = 0.0
        for line in text.splitlines():
            if line.startswith(("vllm:num_requests_running",
                                "vllm:num_requests_waiting")):
                n += float(line.rsplit(" ", 1)[1])
        return n
    except Exception:
        return None


def vllm_is_sleeping(ip):
    try:
        import json
        with http(ip, "GET", "/is_sleeping", timeout=5) as r:
            return bool(json.load(r).get("is_sleeping"))
    except Exception:
        return None  # endpoint unavailable — assume unknown


def vllm_healthy(ip):
    try:
        with http(ip, "GET", "/health", timeout=2):
            return True
    except Exception:
        return False


def clusterip_healthy():
    try:
        with urllib.request.urlopen(SERVICE_PROBE_URL, timeout=2) as r:
            return 200 <= r.status < 300
    except urllib.error.HTTPError as e:
        # A 5xx from a real backend still proves the route is programmed, but
        # /health is the wrong place to be lenient: treat anything non-2xx as
        # not-yet-serving and keep polling.
        return 200 <= e.code < 300
    except Exception:
        return False


class Tenant(threading.Thread):
    """One thread per time-sliced pod on this node. The PoC has exactly one.

    Everything in here is the in-pod supervisor's main loop with the pod
    boundary moved: `client` is still constructed with the TENANT's job_id and
    group (not the supervisor's — the supervisor has no lock identity of its
    own; it acts as the tenant), the waiter poll is still 0.5 s, and the drain
    constants are byte-for-byte the ones that were measured to work.
    """

    def __init__(self, pod_name, job_id, group):
        super().__init__(name=f"tenant-{job_id}", daemon=True)
        self.pod_name = pod_name
        self.job_id = job_id
        self.group = group
        self.stop = threading.Event()
        self.client = None
        self.handle = None
        self.redis = None
        self.holding = False   # do we believe we hold the lock?
        self._pod = None
        self._pod_at = 0.0

    # -- helpers ------------------------------------------------------------
    def _read_pod(self, max_age_s=0.5):
        """Cached GET of the tenant pod.

        The tight loops below re-check liveness every 100 ms — that cadence is
        chosen for the ClusterIP probe, where 100 ms of slack is a tenth of the
        gap we are trying to close — and an uncached read would be ten API
        calls per second per tenant for the whole run. 500 ms of staleness is
        irrelevant to every decision made from this object (pod IP does not
        change while a pod lives; a deletion noticed half a second late costs
        nothing) and keeps the DaemonSet's API-server load at ~2 reads/s/node.
        An API error is treated as "no change" rather than as "pod gone", so a
        blip cannot trigger a spurious release.
        """
        now = time.monotonic()
        if now - self._pod_at < max_age_s:
            return self._pod
        try:
            self._pod = get_pod(self.pod_name)
            self._pod_at = now
        except Exception as e:
            note(self.job_id, f"pod read error (transient): {e}")
        return self._pod

    def gone(self):
        """True once the tenant pod no longer exists (or is terminating).

        A disappearing tenant must never wedge the loop: every bounded wait in
        this class re-checks this, so a `kubectl delete pod shadow-vllm` during
        a drain unwinds the thread instead of spinning until its timeout.
        """
        if self.stop.is_set():
            return True
        p = self._read_pod()
        return p is None or p.metadata.deletion_timestamp is not None

    def ip(self):
        p = self._read_pod()
        return None if p is None or p.status is None else p.status.pod_ip

    def ready_condition(self):
        p = self._read_pod(max_age_s=0.0)   # the thing we are actively watching
        if p is None or p.status is None or not p.status.conditions:
            return None
        for c in p.status.conditions:
            if c.type == "Ready":
                return c.status
        return None

    def wait_pod_ip(self, timeout_s=300.0):
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout_s:
            if self.gone():
                return None
            ip = self.ip()
            if ip:
                return ip
            time.sleep(0.5)
        return None

    # -- engine sleep / wake (also the workload-channel callbacks) ----------
    def vllm_sleep(self, mode=None, tags=None):
        ip = self.ip()
        t0 = time.monotonic()
        http(ip, "POST", "/sleep?level=1", timeout=300)
        event(self.job_id, "vllm_slept", seconds=f"{time.monotonic()-t0:.2f}")

    def vllm_wake(self, tags=None):
        ip = self.ip()
        t0 = time.monotonic()
        http(ip, "POST", "/wake_up", timeout=300)
        event(self.job_id, "vllm_woke", seconds=f"{time.monotonic()-t0:.2f}")
        # NOTE the deliberate difference from supervisor.py's vllm_wake(), which
        # called gate_open() right here. It must NOT do that any more. Weights
        # being back in HBM is the FIRST of four preconditions for re-admitting
        # traffic, not the last; opening the endpoint here would reintroduce
        # exactly the rising-edge gap this component exists to close. The gate
        # is opened only by reacquire(), after the ClusterIP probe.

    # -- the yield sequence --------------------------------------------------
    # ORDER IS THE POINT. (a) withdraw the endpoint, (b) observe it withdrawn,
    # (c) drain, (d) optionally sleep, (e) release. The release is the
    # acknowledgement the orchestrator is blocked on: it cannot grant the GPU
    # to the trainer until it lands. Doing (a)-(d) strictly before it is what
    # makes "endpoints are withdrawn before the GPU moves" an ordering
    # guarantee instead of a hope about relative speeds.
    def yield_gpu(self):
        # (a) withdraw the endpoint.
        patch_gate(self.pod_name, "False")
        t_patch = time.monotonic()
        event(self.job_id, "condition_patched", condition=GATE_CONDITION,
              value="False", pod=self.pod_name)

        # (b) wait until the withdrawal is observable.
        #
        # HONESTY, because this is the easiest place in the file to overclaim:
        # observing Ready=False on the pod object is NOT the same as every
        # node's kube-proxy having deleted the DNAT rule for this endpoint. It
        # means the API server has accepted our patch and recomputed Ready;
        # the endpoints controller, the EndpointSlice write, the watch fan-out
        # to every kube-proxy, and each proxy's rule reprogramming all still
        # have to happen. A dispatch that was DNAT'd microseconds before can
        # also still be in flight on an established conntrack entry.
        #
        # This PoC does not try to close that gap on the FALLING edge, and does
        # not need to: the orchestrator already wrote budget "0" synchronously
        # inside the GetGroupStatus call that told us to yield — seconds ahead
        # of this point — so AP has stopped dequeuing before the endpoint even
        # starts to go away. Measured: across 28 falling edges exactly one
        # request was exposed, and it succeeded. The falling edge is sound by
        # construction; the rising edge is the one that needed a new mechanism.
        ready = None
        while time.monotonic() - t_patch < NOTREADY_TIMEOUT_S:
            if self.gone():
                return
            ready = self.ready_condition()
            if ready == "False":
                break
            time.sleep(0.1)
        elapsed = time.monotonic() - t_patch
        if ready == "False":
            event(self.job_id, "endpoint_notready", seconds=f"{elapsed:.2f}",
                  pod=self.pod_name)
        else:
            event(self.job_id, "endpoint_notready_timeout",
                  seconds=f"{elapsed:.2f}", ready=ready, pod=self.pod_name)
            note(self.job_id,
                 "Ready=False not observed within the bound; proceeding to "
                 "drain anyway — the drain is the real safety property and "
                 "the budget is already 0")

        # (c) drain.
        self.drain()

        # (d) optionally sleep locally. Same semantics, same warning as
        # supervisor.py: the workload channel is only honoured by agents that
        # resolve a backend PER JOB; an agent with a single node-wide
        # DEFAULT_BACKEND=cuda never invokes our callbacks, so nothing would
        # evacuate the tenant's HBM and the trainer OOMs on restore (it needs
        # the whole 24 GB L4 to itself). Sleeping here frees weights+KV to host
        # RAM in ~1 s regardless of which backend the agent uses, and the
        # agent's subsequent cuda-checkpoint then freezes an already-idle,
        # near-empty process. The counter-risk, and the reason the manifest
        # ships LOCAL_SLEEP=0, is that vLLM sleep and cuda-checkpoint on the
        # same process conflict: the engine dies during the agent's
        # cuda-checkpoint RESTORE of an already-slept process.
        if LOCAL_SLEEP and not self.gone() and not vllm_is_sleeping(self.ip()):
            try:
                self.vllm_sleep()
            except Exception as e:
                note(self.job_id, f"local sleep failed (continuing to release): {e}")

        # (e) release. Only now may the GPU move.
        self.client.release()
        self.holding = False
        event(self.job_id, "lock_released", group=self.group)

    def drain(self, settle_s=2.5, max_wait_s=12.0):
        """Wait for in-flight requests to finish. Constants ported verbatim
        from supervisor.py's drain(): settle_s=2.5, max_wait_s=12.0, and two
        CONSECUTIVE zero observations required after settle_s has elapsed (one
        zero is indistinguishable from the gap between two requests).

        REQUIRED before sleeping or freezing: vLLM's /sleep does NOT drain the
        scheduler (vllm-project/vllm#28714, unfixed as of v0.10.x) — freeing
        weights with a request mid-decode is a fatal CUDA error that kills the
        server. The endpoint is already withdrawn by the caller, so no new work
        can arrive while we count down.

        The one change from the in-pod version is the /metrics target: the pod
        IP instead of 127.0.0.1. inflight() returning None (pod gone, engine
        already dead, connection refused) is treated as "not yet zero" exactly
        as it was, so a dead engine drains by timeout rather than by a false
        zero.
        """
        t0 = time.monotonic()
        zeros = 0
        while time.monotonic() - t0 < max_wait_s:
            if self.gone():
                return
            n = inflight(self.ip())
            if n == 0 and time.monotonic() - t0 >= settle_s:
                zeros += 1
                if zeros >= 2:
                    event(self.job_id, "drain_complete",
                          seconds=f"{time.monotonic()-t0:.2f}")
                    return
            else:
                zeros = 0
            time.sleep(0.25)
        event(self.job_id, "drain_timeout", seconds=f"{max_wait_s:.2f}",
              in_flight=inflight(self.ip()))

    # -- the reacquire sequence ---------------------------------------------
    def reacquire(self):
        # (a) blocks until the trainer is done and our context is restored.
        res = self.client.acquire()
        self.holding = True
        # Verbatim phrase at the instant acquire() returns: cycle.py OPENS the
        # serving window here. Note that this is the lock clock, which is the
        # earliest of the three and deliberately so — the gap between it and
        # the clusterip_probe_ok below IS the rising-edge exposure this
        # component was built to measure and then close.
        legacy(self.job_id, "lock reacquired")
        event(self.job_id, "lock_reacquired", group=self.group,
              waited_ms=getattr(res, "waited_ms", "?"),
              context_restored=getattr(res, "context_restored", "?"))
        if self.gone():
            return
        ip = self.wait_pod_ip()
        if ip is None:
            return

        # (b) belt and braces: if the platform did not wake us, do it here.
        if vllm_is_sleeping(ip):
            note(self.job_id, "still sleeping after acquire - waking locally")
            self.vllm_wake()

        # (c) vLLM answers locally. This is the weakest of the three "is it
        # serving" signals and the only one the old in-pod supervisor had.
        t0 = time.monotonic()
        while not vllm_healthy(ip):
            if self.gone():
                return
            if time.monotonic() - t0 > 300:
                note(self.job_id, "vLLM not healthy 300s after acquire; still waiting")
                t0 = time.monotonic()
            time.sleep(0.2)
        event(self.job_id, "vllm_healthy_local",
              seconds=f"{time.monotonic()-t0:.2f}", ip=ip)

        # (d) tell Kubernetes we hold the GPU. ContainersReady is already true
        # (the stock /health readinessProbe), so this is the patch that flips
        # pod Ready and starts the endpoint-programming cascade.
        patch_gate(self.pod_name, "True")
        t_patch = time.monotonic()
        event(self.job_id, "condition_patched", condition=GATE_CONDITION,
              value="True", pod=self.pod_name)

        # (e) self-probe the ClusterIP. THIS is the observation the whole
        # component exists to make. A connect to the Service VIP is DNAT'd by
        # kube-proxy, so a 200 back through it means an endpoint for this
        # Service is programmed in this node's rules — i.e. the cascade in (d)
        # has actually completed somewhere, not merely been initiated.
        #
        # What it does NOT prove, stated plainly rather than hidden: it proves
        # it for THIS NODE'S kube-proxy. AP runs on some other node, and that
        # node's kube-proxy watches the same EndpointSlice independently. It
        # may program the rule slightly before or slightly after ours — the
        # residual is a cross-node convergence SKEW, not zero. We have traded
        # an unbounded 0.74-1.84 s gap (context-restore to routable) for a
        # bounded differential between two proxies observing the same write,
        # which in a healthy cluster is small and, crucially, is centred on
        # zero rather than systematically early. It is not exact and this
        # component must not be described as making it exact. If the residual
        # ever needs to be zero, the answer is a readiness signal AP itself
        # consumes, not a better probe here.
        ok = False
        while time.monotonic() - t_patch < SERVICE_PROBE_TIMEOUT_S:
            if self.gone():
                return
            if clusterip_healthy():
                ok = True
                break
            time.sleep(0.1)
        lag = time.monotonic() - t_patch
        if not ok:
            event(self.job_id, "clusterip_probe_timeout",
                  seconds=f"{lag:.2f}", url=SERVICE_PROBE_URL)
            note(self.job_id,
                 "NOT opening the dispatch budget: the route was never observed. "
                 "Leaving the budget at whatever the orchestrator last wrote (0) "
                 "is the safe direction — the tenant serves, AP just does not "
                 "dispatch, and the next cycle re-probes.")
            return
        event(self.job_id, "clusterip_probe_ok", seconds=f"{lag:.2f}",
              url=SERVICE_PROBE_URL)

        # (f) and ONLY now. See DISPATCH BUDGET OWNERSHIP above: this is the
        # single write this process makes to that key, and its value is always
        # "1".
        try:
            budget_open(self.job_id, BUDGET_KEY, self.redis)
            event(self.job_id, "budget_set", key=BUDGET_KEY, value="1",
                  seconds_after_condition=f"{time.monotonic()-t_patch:.2f}")
        except Exception as e:
            event(self.job_id, "budget_set_failed", key=BUDGET_KEY, error=type(e).__name__)
            note(self.job_id,
                 f"Redis SET failed ({e}); AP stays gated shut until the next "
                 "serving window. Fails closed, which is the correct direction.")

    # -- thread body ---------------------------------------------------------
    def run(self):
        try:
            self._run()
        except Exception as e:
            event(self.job_id, "tenant_thread_exit", error=f"{type(e).__name__}:{e}")
        finally:
            if self.handle is not None:
                try:
                    self.handle.close()
                except Exception:
                    pass
            if self.holding and self.client is not None:
                # The pod went away while we held the lock on its behalf. Give
                # the lock back explicitly: a crash-release FAULTS the group
                # and recovery is a manual procedure, not a pod restart.
                try:
                    self.client.release()
                    event(self.job_id, "lock_released", group=self.group,
                          reason="tenant_gone")
                except Exception:
                    pass

    def _run(self):
        host, _, port = REDIS_ADDR.rpartition(":")
        self.redis = redis_lib.Redis(host=host, port=int(port),
                                     socket_timeout=2, socket_connect_timeout=2)
        self.client = OrchestratorClient(target=ORCH, job_id=self.job_id,
                                         group_id=self.group)
        event(self.job_id, "tenant_discovered", pod=self.pod_name,
              group=self.group, node=NODE_NAME)

        ip = self.wait_pod_ip()
        if ip is None:
            return

        # COLD START — and a real wart of moving the supervisor out of the pod,
        # called out here rather than in the report only. The in-pod supervisor
        # took the lock BEFORE exec'ing `vllm serve`, so no CUDA context existed
        # until the lock was held. Here the kubelet starts the stock vLLM
        # container the moment the pod is scheduled, so the engine may allocate
        # its 70% of HBM while the trainer still holds the GPU. acquire() below
        # is therefore a best-effort ordering, not the guarantee it used to be.
        # The tenant-side fix is an initContainer (or the injected sidecar the
        # mutating webhook already has a landing site for) that blocks on the
        # lock before the vLLM container starts; that is out of scope for a
        # component that is explicitly not allowed to modify the tenant image.
        note(self.job_id, "requesting lock (cold start; note the engine may "
                          "already have a CUDA context — see comment)")
        res = self.client.acquire()
        self.holding = True
        legacy(self.job_id, "lock acquired")   # cycle.py opens the first window here
        event(self.job_id, "lock_acquired", group=self.group,
              waited_ms=getattr(res, "waited_ms", "?"),
              context_restored=getattr(res, "context_restored", "?"))

        # Register the tenant's workload channel with the node's snapshot agent
        # ON ITS BEHALF. This still works from out here — register_workload is
        # just a gRPC stream keyed by job_id, and the agent matches it against
        # the timeslice.io/job-id pod label, which we read from the very pod we
        # are supervising. The callbacks now run in THIS process and reach the
        # engine over HTTP to the pod IP instead of 127.0.0.1, which is strictly
        # better: they keep working while the tenant process is frozen mid-
        # cuda-checkpoint, whereas an in-pod handler cannot be scheduled then.
        self.handle = register_workload(
            AGENT, job_id=self.job_id, group=self.group,
            on_snapshot=self.vllm_sleep, on_restore=self.vllm_wake,
            supported_modes=["offload"], default_mode="offload")
        legacy(self.job_id, f"workload registered with agent {AGENT}")
        event(self.job_id, "workload_registered", agent=AGENT)

        # Cold start is a rising edge like any other: run the full sequence so
        # the gate, the condition and the budget all start from the same
        # observed state rather than from an assumption.
        self.open_serving_window()

        while not self.gone():
            # -- serving phase: hold the lock only while nobody waits --
            while not self.gone():
                try:
                    st = self.client.get_status(group_id=self.group)
                    g = getattr(st, "group", st)
                    if int(getattr(g, "waiter_queue_depth", 0)) > 0:
                        # Verbatim phrase FIRST and before anything else in the
                        # yield sequence: cycle.py closes the serving window at
                        # this timestamp, and the window it reports must be the
                        # lock's, not the lock's minus our patch+drain.
                        legacy(self.job_id, "trainer is waiting - yielding GPU")
                        event(self.job_id, "yield_requested", group=self.group)
                        break
                except Exception as e:
                    # Transient by default: a single failed GetGroupStatus is
                    # an RPC blip, not a reason to drop a lock we are holding.
                    note(self.job_id, f"status poll error (transient): {e}")
                time.sleep(POLL_S)
            if self.gone():
                return

            self.yield_gpu()
            time.sleep(1.0)
            self.reacquire()

    def open_serving_window(self):
        """Cold-start equivalent of reacquire() (b)-(f): we already hold the
        lock, so skip the acquire and run the same observation chain."""
        ip = self.wait_pod_ip()
        if ip is None:
            return
        t0 = time.monotonic()
        while not vllm_healthy(ip):
            if self.gone():
                return
            time.sleep(0.5)
        # supervisor.py printed this the moment its child answered /health at
        # launch; bring-up run-notes grep for it. It means the same thing here,
        # with one honest difference worth knowing while reading a log: the old
        # supervisor could only print it AFTER it had the lock, because it did
        # not exec the engine until then. We did not start this engine, so "up"
        # here says nothing about when the CUDA context appeared.
        legacy(self.job_id, "vLLM is up")
        event(self.job_id, "vllm_healthy_local",
              seconds=f"{time.monotonic()-t0:.2f}", ip=ip, phase="cold_start")
        patch_gate(self.pod_name, "True")
        t_patch = time.monotonic()
        event(self.job_id, "condition_patched", condition=GATE_CONDITION,
              value="True", pod=self.pod_name, phase="cold_start")
        ok = False
        while time.monotonic() - t_patch < SERVICE_PROBE_TIMEOUT_S:
            if self.gone():
                return
            if clusterip_healthy():
                ok = True
                break
            time.sleep(0.1)
        if not ok:
            event(self.job_id, "clusterip_probe_timeout",
                  seconds=f"{time.monotonic()-t_patch:.2f}", url=SERVICE_PROBE_URL,
                  phase="cold_start")
            return
        event(self.job_id, "clusterip_probe_ok",
              seconds=f"{time.monotonic()-t_patch:.2f}", url=SERVICE_PROBE_URL,
              phase="cold_start")
        try:
            budget_open(self.job_id, BUDGET_KEY, self.redis)
            event(self.job_id, "budget_set", key=BUDGET_KEY, value="1",
                  seconds_after_condition=f"{time.monotonic()-t_patch:.2f}",
                  phase="cold_start")
        except Exception as e:
            event(self.job_id, "budget_set_failed", key=BUDGET_KEY,
                  error=type(e).__name__, phase="cold_start")
            note(self.job_id, f"Redis SET failed at cold start ({e})")


def main():
    note("-", f"node supervisor starting on node={NODE_NAME} ns={NAMESPACE} "
              f"orch={ORCH} agent={AGENT} budget_key={BUDGET_KEY} "
              f"probe={SERVICE_PROBE_URL} local_sleep={int(LOCAL_SLEEP)}")
    note("-", f"event line regex: {_TS_RE_DOC}")
    assert re.compile(_TS_RE_DOC)  # the parsers in results/ depend on it

    threads = {}    # pod name -> Tenant currently being supervised
    retiring = []   # Tenants whose pod is gone but whose thread has not unwound
    while True:
        try:
            tenants = list_tenants()
        except Exception as e:
            # The API server being briefly unreachable must not take down the
            # supervisor: the per-tenant threads keep holding their locks and
            # keep serving, and discovery resumes on the next tick. Crucially it
            # must also not look like "every tenant disappeared" — hence the
            # continue rather than falling through to the removal pass below,
            # which would release a perfectly healthy tenant's lock because of
            # one failed LIST.
            note("-", f"tenant discovery error (transient): {e}")
            time.sleep(DISCOVERY_POLL_S)
            continue

        retiring = [t for t in retiring if t.is_alive()]

        for pod_name, (job_id, group) in tenants.items():
            t = threads.get(pod_name)
            if t is not None and t.is_alive():
                continue
            # Never run two lock clients for one job_id. A thread can still be
            # unwinding — most likely blocked in the acquire() it entered before
            # its pod was deleted, since acquire() has no deadline by design —
            # and starting a second client for the same job would put two
            # entries in the orchestrator's waiter queue for one tenant and
            # make the release/grant bookkeeping nonsense. The pod name is
            # stable across recreation (it is a bare Pod, not a Deployment), so
            # this is the ordinary restart path, not an exotic one.
            if any(r.job_id == job_id for r in retiring):
                continue
            if t is not None:
                event(job_id, "tenant_thread_restart", pod=pod_name)
            t = Tenant(pod_name, job_id, group)
            threads[pod_name] = t
            t.start()

        for pod_name in list(threads):
            if pod_name not in tenants:
                t = threads.pop(pod_name)
                event(t.job_id, "tenant_gone", pod=pod_name)
                t.stop.set()   # the thread unwinds itself and releases the lock
                retiring.append(t)

        time.sleep(DISCOVERY_POLL_S)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
