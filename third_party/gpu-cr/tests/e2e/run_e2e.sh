#!/usr/bin/env bash
# End-to-end test for the GPU-CR stack on a GPU node.
#
# Drives a real CUDA workload under LD_PRELOAD through the checkpoint/
# restore surface, gating on byte-identical GPU memory after every restore:
#   G1  baseline pattern verify
#   G6  full checkpoint/restore data plane + verify (stubbed toggle unless
#       E2E_FULL_TOGGLE=1 and a real cuda-checkpoint is on PATH)
# (Gate numbers G2-G5 are reserved for later additions in this series;
#  their gates land with them.)
#
# Required env: CR_CLIENT, WORKLOAD, VGPU_SO, STORE.
# Optional env: E2E_NUM_BUFFERS, E2E_BUFFER_MB, CR_TIMEOUT (seconds per
# cr_client call, default 120).
set -u
HERE=$(dirname "$(readlink -f "$0")")
. "$HERE/e2e_lib.sh"

: "${CR_CLIENT:?}" "${WORKLOAD:?}" "${VGPU_SO:?}" "${STORE:?}"
NUM_BUFFERS=${E2E_NUM_BUFFERS:-4}
BUFFER_MB=${E2E_BUFFER_MB:-64}

PASS=0; FAIL=0
gate() {
    local name=$1; shift
    if "$@"; then echo "PASS: $name"; PASS=$((PASS+1));
    else echo "FAIL: $name"; FAIL=$((FAIL+1)); fi
}
cr() { env EXPORT_FILE_PATH="$STORE" timeout "${CR_TIMEOUT:-120}" "$CR_CLIENT" "$@"; }

trap 'stop_workload' EXIT
if [ "${E2E_FULL_TOGGLE:-0}" != "1" ]; then stub_cuda_checkpoint; fi

start_workload "$VGPU_SO" \
    E2E_NUM_BUFFERS="$NUM_BUFFERS" E2E_BUFFER_MB="$BUFFER_MB" || exit 1
echo "workload up: pid=$WL_PID (run dir $RUN)"

cr -i -p "$WL_PID" || { echo "FATAL: init failed ($?)"; exit 1; }

gate "G1 baseline verify" wl_cmd verify

gate "G6a full ckpt" cr -c -p "$WL_PID"
gate "G6b full restore" cr -r -p "$WL_PID"
gate "G6c verify after full restore" wl_cmd verify

stop_workload
echo
echo "=== e2e summary: $PASS passed, $FAIL failed ==="
echo "workload stderr: $RUN/workload.stderr"
[ "$FAIL" -eq 0 ]
