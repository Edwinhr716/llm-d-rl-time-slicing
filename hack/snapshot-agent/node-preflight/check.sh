#!/usr/bin/env bash
# Node preflight for the snapshot agent. Runs inside the pod in pod.yaml and
# prints one "CHECK <name>: yes|no" line per precondition, each followed by
# its evidence, then a summary table.
#
#   check.sh privileged    full check, run the way the snapshot agent runs:
#                          privileged, host /sys/fs/cgroup mounted read-write,
#                          host GPU driver and /dev mounted.
#   check.sh unprivileged  what an ordinary pod sees (no GPU, no host mounts).
#
# Checks:
#   cgroup-v2             /sys/fs/cgroup is a cgroup2 filesystem.
#   cgroup-kill           cgroup.kill exists in the container, pod and parent
#                         (QoS) cgroups, and killing a scratch cgroup empties it.
#   cgroup-rw             /sys/fs/cgroup is mounted read-write and a cgroup can
#                         be created and removed; cgroup2 can be mounted afresh.
#   cuda-checkpoint       cuda-checkpoint --get-state answers "running" for a
#                         live CUDA process in this pod.
# Extra evidence (not a gate): a lock/checkpoint/restore/unlock round trip.
#
# The script only creates and removes scratch cgroups under its own pod.

set -uo pipefail

MODE="${1:-privileged}"
HOST_CG="${HOST_CGROUP_MOUNT:-/host/sys/fs/cgroup}"
NV_DIR="${NVIDIA_DIR:-/usr/local/nvidia}"
# Same pinned NVIDIA/cuda-checkpoint commit the snapshot-agent image bundles
# (docker/snapshot-agent/Dockerfile).
CC_URL="${CUDA_CHECKPOINT_URL:-https://github.com/NVIDIA/cuda-checkpoint/raw/00d5cce84c628088d6caa203fc4af40c1538b6f7/bin/x86_64_Linux/cuda-checkpoint}"
HOLDER_MIB="${HOLDER_MIB:-256}"
export LD_LIBRARY_PATH="$NV_DIR/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

declare -a SUMMARY=()

say() { printf '%s\n' "$*"; }
ev() { printf '  evidence: %s\n' "$*"; }
result() { # name yes|no
	say "CHECK $1: $2"
	SUMMARY+=("$1 $2")
}

mount_opts() { # mountpoint -> "fstype opts" from /proc/self/mountinfo
	awk -v mp="$1" '$5 == mp { for (i = 7; $i != "-"; i++); print $(i + 1), $6 }' /proc/self/mountinfo | tail -1
}

probe_mkdir() { # dir -> 0 if a child cgroup can be created and removed
	local d="$1/preflight-probe-$$"
	if mkdir "$d" 2>/tmp/probe.err; then
		rmdir "$d"
		return 0
	fi
	return 1
}

section() { say ""; say "== $*"; }

# This container's own cgroup as seen through /sys/fs/cgroup. Privileged
# containers may share the host cgroup namespace, where /sys/fs/cgroup is the
# host root (which has no cgroup.kill); unprivileged ones see their own cgroup
# as the root.
own_cgroup() {
	local p
	p="$(sed -n "s/^0:://p" /proc/self/cgroup)"
	p="${p%/}"
	printf "/sys/fs/cgroup%s" "$p"
}

# nvidia-smi query, one line per GPU joined with "; ".
smi() { "$NV_DIR/bin/nvidia-smi" "$@" --format=csv,noheader 2>&1 | paste -sd ";" | sed "s/;/; /g"; }

node_info() {
	section "node"
	ev "kernel: $(uname -r)"
	if [ -r /host/etc/os-release ]; then
		ev "os-release: $(grep -E '^(NAME|VERSION|BUILD_ID|VERSION_ID)=' /host/etc/os-release | tr '\n' ' ')"
	fi
	if [ -r /proc/driver/nvidia/version ]; then
		ev "nvidia kernel module: $(head -1 /proc/driver/nvidia/version)"
	fi
	if [ -x "$NV_DIR/bin/nvidia-smi" ]; then
		ev "gpu: $(smi --query-gpu=name,driver_version,memory.total,memory.used)"
	fi
	ev "cpu: $(nproc) cores, mem: $(awk '/MemTotal/ {print $2 " kB"}' /proc/meminfo)"
}

check_cgroup_v2() {
	section "cgroup v2"
	local fs hostfs ok=yes
	fs="$(stat -fc %T /sys/fs/cgroup 2>&1)"
	ev "/sys/fs/cgroup fs type: $fs; mountinfo: $(mount_opts /sys/fs/cgroup)"
	ev "/proc/filesystems: $(grep -w cgroup2 /proc/filesystems | xargs)"
	ev "/proc/self/cgroup: $(tr '\n' ' ' </proc/self/cgroup)"
	[ "$fs" = "cgroup2fs" ] || ok=no
	if [ "$MODE" = privileged ]; then
		hostfs="$(stat -fc %T "$HOST_CG" 2>&1)"
		ev "$HOST_CG (host /sys/fs/cgroup) fs type: $hostfs; controllers: $(cat "$HOST_CG/cgroup.controllers" 2>&1)"
		[ "$hostfs" = "cgroup2fs" ] || ok=no
	fi
	result cgroup-v2 "$ok"
}

# Find this pod's cgroup directory in the host hierarchy by pod UID.
find_pod_cgroup() {
	local uid_dash="$POD_UID" uid_us="${POD_UID//-/_}"
	find "$HOST_CG" -maxdepth 4 -type d \( -name "*pod${uid_us}*" -o -name "*pod${uid_dash}*" \) 2>/dev/null | head -1
}

check_cgroup_kill() {
	section "cgroup.kill"
	local ok=yes own_cg pod_cg parent_cg cont_cg scratch pid
	own_cg="$(own_cgroup)"
	ev "cgroup namespace root: $([ "$own_cg" = /sys/fs/cgroup ] && echo private || echo host); own cgroup dir: $own_cg"
	if [ -e "$own_cg/cgroup.kill" ]; then ev "container view $own_cg/cgroup.kill: present"; else
		ev "container view $own_cg/cgroup.kill: missing"
		ok=no
	fi
	if [ "$MODE" != privileged ]; then
		result cgroup-kill-visible "$ok"
		return
	fi
	pod_cg="$(find_pod_cgroup)"
	if [ -z "$pod_cg" ]; then
		ev "pod cgroup for uid $POD_UID not found under $HOST_CG"
		result cgroup-kill no
		return
	fi
	parent_cg="$(dirname "$pod_cg")"
	cont_cg=""
	for d in "$pod_cg"/*/; do
		if grep -qx "$$" "${d}cgroup.procs" 2>/dev/null; then cont_cg="${d%/}"; fi
	done
	ev "host cgroup of this container: ${cont_cg#"$HOST_CG"}"
	for d in "$cont_cg" "$pod_cg" "$parent_cg"; do
		[ -n "$d" ] || continue
		if [ -e "$d/cgroup.kill" ]; then ev "${d#"$HOST_CG"}/cgroup.kill: present"; else
			ev "${d#"$HOST_CG"}/cgroup.kill: missing"
			ok=no
		fi
	done
	[ -n "$cont_cg" ] || {
		ev "own container cgroup not identified"
		ok=no
	}
	# Functional test: a scratch cgroup under the pod cgroup with one sleeper.
	scratch="$pod_cg/preflight-kill-$$"
	if mkdir "$scratch" 2>/tmp/kill.err; then
		sleep 600 &
		pid=$!
		if echo "$pid" >"$scratch/cgroup.procs" 2>>/tmp/kill.err; then
			ev "scratch cgroup ${scratch#"$HOST_CG"} procs before kill: $(xargs <"$scratch/cgroup.procs")"
			echo 1 >"$scratch/cgroup.kill"
			wait "$pid" 2>/dev/null
			ev "sleeper exit status after cgroup.kill: $? (137 = SIGKILL)"
			sleep 0.2
			ev "procs after kill: [$(xargs <"$scratch/cgroup.procs")]; events: $(tr '\n' ' ' <"$scratch/cgroup.events")"
			[ -z "$(cat "$scratch/cgroup.procs")" ] || ok=no
		else
			ev "could not move sleeper into scratch cgroup: $(cat /tmp/kill.err)"
			kill "$pid" 2>/dev/null
			ok=no
		fi
		rmdir "$scratch" 2>/dev/null || ev "rmdir scratch failed"
	else
		ev "mkdir scratch cgroup failed: $(cat /tmp/kill.err)"
		ok=no
	fi
	result cgroup-kill "$ok"
}

check_cgroup_rw() {
	section "/sys/fs/cgroup read-write"
	local ok=yes opts
	opts="$(mount_opts /sys/fs/cgroup)"
	ev "container /sys/fs/cgroup mount: $opts"
	case "$opts" in *" rw,"* | *" rw") ;; *) ok=no ;; esac
	if probe_mkdir "$(own_cgroup)"; then ev "container view: child cgroup create+remove under own cgroup ok"; else
		ev "container view: create under own cgroup failed: $(cat /tmp/probe.err)"
		ok=no
	fi
	if [ "$MODE" != privileged ]; then
		result cgroup-rw-unprivileged "$ok"
		return
	fi
	opts="$(mount_opts "$HOST_CG")"
	ev "host /sys/fs/cgroup hostPath mount at $HOST_CG: $opts"
	case "$opts" in *" rw,"* | *" rw") ;; *) ok=no ;; esac
	local pod_cg
	pod_cg="$(find_pod_cgroup)"
	if [ -n "$pod_cg" ] && probe_mkdir "$pod_cg"; then ev "host view: child cgroup create+remove under pod cgroup ok"; else
		ev "host view: create under pod cgroup failed: $(cat /tmp/probe.err 2>/dev/null)"
		ok=no
	fi
	mkdir -p /tmp/cg2
	if mount -t cgroup2 cgroup2 /tmp/cg2 2>/tmp/mount.err; then
		ev "fresh 'mount -t cgroup2' inside the pod: ok ($(mount_opts /tmp/cg2))"
		umount /tmp/cg2
	else
		ev "fresh 'mount -t cgroup2' failed: $(cat /tmp/mount.err)"
		ok=no
	fi
	result cgroup-rw "$ok"
}

start_cuda_holder() { # prints the PID of a process holding a CUDA context
	python3 - "$HOLDER_MIB" >/tmp/holder.log 2>&1 <<'EOF' &
import ctypes, sys, time
cuda = ctypes.CDLL("libcuda.so.1")
def ck(r, what):
    if r != 0:
        print(f"{what} failed: {r}", flush=True)
        sys.exit(1)
ck(cuda.cuInit(0), "cuInit")
dev = ctypes.c_int()
ck(cuda.cuDeviceGet(ctypes.byref(dev), 0), "cuDeviceGet")
ctx = ctypes.c_void_p()
ck(cuda.cuCtxCreate_v2(ctypes.byref(ctx), 0, dev), "cuCtxCreate")
ptr = ctypes.c_uint64()
size = int(sys.argv[1]) << 20
ck(cuda.cuMemAlloc_v2(ctypes.byref(ptr), ctypes.c_size_t(size)), "cuMemAlloc")
ck(cuda.cuMemsetD8_v2(ptr, 0xAB, ctypes.c_size_t(size)), "cuMemsetD8")
ck(cuda.cuCtxSynchronize(), "cuCtxSynchronize")
print(f"ready: {size >> 20} MiB allocated", flush=True)
while True:
    time.sleep(0.5)
EOF
	echo $!
}

cc_state() { "$1" --get-state --pid "$2" 2>&1 | tr '\n' ' '; }

check_cuda_checkpoint() {
	section "cuda-checkpoint"
	local ok=no cands=() bin pid state
	# Where could it come from? node driver install, PATH, or bundled by us.
	for p in "$NV_DIR/bin/cuda-checkpoint" "$(command -v cuda-checkpoint 2>/dev/null)"; do
		[ -n "$p" ] && [ -x "$p" ] && cands+=("$p")
	done
	if [ ${#cands[@]} -eq 0 ]; then ev "no cuda-checkpoint in the driver install ($NV_DIR/bin) or PATH"; fi
	ev "cuda-checkpoint files anywhere in the driver install: [$(find "$NV_DIR" -name "cuda-checkpoint*" -printf "%p " 2>/dev/null)]"
	if python3 -c "import urllib.request,sys; urllib.request.urlretrieve(sys.argv[1], '/tmp/cuda-checkpoint')" "$CC_URL" 2>/tmp/dl.err; then
		chmod +x /tmp/cuda-checkpoint
		cands+=(/tmp/cuda-checkpoint)
	else
		ev "download of the pinned (bundled) cuda-checkpoint failed: $(tail -1 /tmp/dl.err)"
	fi
	for bin in "${cands[@]}"; do
		ev "binary $bin sha256=$(sha256sum "$bin" | cut -d" " -f1) help: $("$bin" --help 2>&1 | head -3 | tr "\n" " ")"
	done
	pid="$(start_cuda_holder)"
	for _ in $(seq 1 60); do
		grep -q ready /tmp/holder.log && break
		kill -0 "$pid" 2>/dev/null || break
		sleep 0.5
	done
	ev "CUDA holder pid $pid: $(tr '\n' ' ' </tmp/holder.log)"
	if ! grep -q ready /tmp/holder.log; then
		result cuda-checkpoint no
		return
	fi
	for bin in "${cands[@]}"; do
		state="$(cc_state "$bin" "$pid")"
		ev "$bin --get-state --pid $pid -> $state"
		case "$state" in running*) ok=yes ;; esac
	done
	result cuda-checkpoint "$ok"

	# Extra: the agent's suspend/resume calls, on the first working binary.
	for bin in "${cands[@]}"; do
		case "$(cc_state "$bin" "$pid")" in running*) ;; *) continue ;; esac
		section "extra: checkpoint round trip with $bin"
		ev "gpu used before: $(smi --query-gpu=memory.used)"
		ev "compute apps before: [$(smi --query-compute-apps=pid,used_memory)]"
		"$bin" --action lock --pid "$pid" 2>&1 | sed 's/^/  lock: /'
		ev "after lock: $(cc_state "$bin" "$pid")"
		"$bin" --action checkpoint --pid "$pid" 2>&1 | sed 's/^/  checkpoint: /'
		ev "after checkpoint: $(cc_state "$bin" "$pid")"
		ev "gpu used while checkpointed: $(smi --query-gpu=memory.used)"
		ev "compute apps while checkpointed: [$(smi --query-compute-apps=pid,used_memory)]"
		"$bin" --action restore --pid "$pid" 2>&1 | sed 's/^/  restore: /'
		"$bin" --action unlock --pid "$pid" 2>&1 | sed 's/^/  unlock: /'
		ev "after restore+unlock: $(cc_state "$bin" "$pid")"
		ev "gpu used after: $(smi --query-gpu=memory.used)"
		break
	done
	kill "$pid" 2>/dev/null
	wait "$pid" 2>/dev/null
}

say "snapshot-agent node preflight, mode=$MODE, pod=${POD_NAME:-?}"
case "$MODE" in
privileged)
	node_info
	check_cgroup_v2
	check_cgroup_kill
	check_cgroup_rw
	check_cuda_checkpoint
	;;
unprivileged)
	ev "id: $(id)"
	check_cgroup_v2
	check_cgroup_kill
	check_cgroup_rw
	;;
*)
	say "usage: $0 privileged|unprivileged"
	exit 2
	;;
esac

section "summary ($MODE)"
fail=0
for line in "${SUMMARY[@]}"; do
	printf '  %-24s %s\n' "${line% *}" "${line##* }"
	[ "${line##* }" = yes ] || fail=1
done
# Unprivileged results are informational; only privileged "no" fails the pod.
[ "$MODE" = privileged ] && exit "$fail"
exit 0
