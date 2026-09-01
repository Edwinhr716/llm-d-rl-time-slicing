#!/usr/bin/env bash
# Integration tests for cr_client against the GPU-free fake workload.
#
# Exercises the real control-channel protocol end to end on Linux without
# a GPU: destination-path checkpoint/restore, dump validation, op_status
# propagation (including the cuda-checkpoint toggle gate), timeouts, and
# not-ready refusals. Asserts the documented exit codes: 0 OK,
# 1 usage, 2 op failed, 3 refused, 4 timeout.
#
# Usage: cr_client_integration_test.sh <cr_client> <fake_workload>
set -u

CR_CLIENT=$(readlink -f "$1")
FAKE=$(readlink -f "$2")

WORK=$(mktemp -d /tmp/gpu-cr-it-data.XXXXXX)
STUB_DIR=$(mktemp -d /tmp/gpu-cr-it-stub.XXXXXX)
TOGGLE_MARKER="$STUB_DIR/toggled"
cat > "$STUB_DIR/cuda-checkpoint" <<EOF
#!/bin/sh
touch "$TOGGLE_MARKER"
exit 0
EOF
chmod +x "$STUB_DIR/cuda-checkpoint"
export PATH="$STUB_DIR:$PATH"
export GPU_CR_CUDA_CHECKPOINT="$STUB_DIR/cuda-checkpoint"
export GPU_CR_OP_TIMEOUT_SEC=10

FAKE_PID=""
PASS=0
FAIL=0

cleanup() {
    stop_fake
    rm -rf "$WORK" "$STUB_DIR"
}
trap cleanup EXIT

# start_fake [ENV=VAL ...] — starts fake_workload with the given extra env
# and waits for its control channel to come up.
start_fake() {
    stop_fake
    local ready="$WORK/ready.$$"
    rm -f "$ready"
    env "$@" FAKE_READY_FILE="$ready" FAKE_LOG="$WORK/fake.log" \
        "$FAKE" 2>> "$WORK/fake.stderr" &
    FAKE_PID=$!
    for _ in $(seq 1 100); do
        [ -f "$ready" ] && return 0
        sleep 0.1
    done
    echo "FATAL: fake workload did not come up" >&2
    exit 1
}

stop_fake() {
    if [ -n "$FAKE_PID" ]; then
        kill "$FAKE_PID" 2>/dev/null
        wait "$FAKE_PID" 2>/dev/null
        FAKE_PID=""
    fi
    rm -f "$WORK"/control* "$WORK"/dump* "$WORK"/fake.log \
          "$TOGGLE_MARKER" 2>/dev/null
    return 0
}

# check <name> <expected_exit> <cmd...>
check() {
    local name=$1 expected=$2
    shift 2
    "$@" > "$WORK/out.log" 2>&1
    local got=$?
    if [ "$got" -eq "$expected" ]; then
        echo "ok:   $name"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $name (exit $got, expected $expected)"
        sed 's/^/      | /' "$WORK/out.log" | tail -15
        FAIL=$((FAIL + 1))
    fi
}

expect_file() {
    if [ -e "$2" ]; then echo "ok:   $1"; PASS=$((PASS + 1));
    else echo "FAIL: $1 (missing $2)"; FAIL=$((FAIL + 1)); fi
}

expect_no_file() {
    if [ ! -e "$2" ]; then echo "ok:   $1"; PASS=$((PASS + 1));
    else echo "FAIL: $1 (unexpected $2)"; FAIL=$((FAIL + 1)); fi
}

ENV="EXPORT_FILE_PATH=$WORK"
REGIONS="0x7f0000000000:4096,0x7f0000100000:8192"

# --- happy paths -------------------------------------------------------------
start_fake $ENV
check "init"                    0 env $ENV "$CR_CLIENT" -i -p "$FAKE_PID"
check "dest-path selective ckpt" 0 env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/dump.bin"
expect_file "dump created" "$WORK/dump.bin"
check "dest-path selective restore" 0 env $ENV "$CR_CLIENT" -r -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/dump.bin"
check "buffer selective ckpt"   0 env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS"
check "full ckpt"               0 env $ENV "$CR_CLIENT" -c -p "$FAKE_PID"
check "full restore (stub toggle)" 0 env $ENV "$CR_CLIENT" -r -p "$FAKE_PID"
expect_file "cuda-checkpoint toggled on success" "$TOGGLE_MARKER"

# --- -b (buffer-only) never touches cuda-checkpoint --------------------------
start_fake $ENV
check "full ckpt -b"            0 env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -b
expect_no_file "-b ckpt did not toggle" "$TOGGLE_MARKER"
check "full restore -b"         0 env $ENV "$CR_CLIENT" -r -p "$FAKE_PID" -b
expect_no_file "-b restore did not toggle" "$TOGGLE_MARKER"

# --- torn dump is refused post-checkpoint ----------------------------------
start_fake $ENV FAKE_SKIP_COMMIT=1
check "torn dump: ckpt fails validation" 2 env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/dump.bin"

# --- restore refuses a torn dump (fake mirrors .so via ValidateDumpFd) -----
start_fake $ENV
head -c 100 /dev/zero > "$WORK/torn.bin"
check "torn dump: restore refused"   2 env $ENV "$CR_CLIENT" -r -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/torn.bin"

# --- op_status propagation --------------------------------------------------
start_fake $ENV FAKE_OP_STATUS=28    # ENOSPC
check "op_status: selective ckpt surfaces failure" 2 env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/dump.bin"
check "op_status: full ckpt fails cleanly" 2 env $ENV "$CR_CLIENT" -c -p "$FAKE_PID"
expect_no_file "op_status: NOT frozen after a failed ckpt" "$TOGGLE_MARKER"
# Restore thaws before the op outcome is known (upstream ordering: the
# in-process handler needs a live CUDA context), so a failed restore
# still exits 2 but the toggle has already run.
check "op_status: full restore fails cleanly" 2 env $ENV "$CR_CLIENT" -r -p "$FAKE_PID"
expect_file "op_status: restore thawed before the failed op" "$TOGGLE_MARKER"

# --- recycled destination is truncated at precreate --------------------------
# A dest path holding an older, larger dump must not contribute stale bytes
# to the new file: after a smaller dump to the same path, the file must
# shrink (O_TRUNC), or a torn write could false-validate against leftover
# marker bytes.
start_fake $ENV
check "recycle setup: 2-region dump"  0 env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/recycle.bin"
big_size=$(stat -c %s "$WORK/recycle.bin")
check "recycled dest: smaller dump"   0 env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "0x7f0000000000:4096" -o "$WORK/recycle.bin"
small_size=$(stat -c %s "$WORK/recycle.bin")
if [ "$small_size" -lt "$big_size" ]; then
    echo "ok:   recycled dest truncated ($big_size -> $small_size bytes)"; PASS=$((PASS + 1))
else
    echo "FAIL: recycled dest not truncated ($big_size -> $small_size bytes)"; FAIL=$((FAIL + 1))
fi

# --- timeout ----------------------------------------------------------------
start_fake $ENV FAKE_NO_FINISH=1
check "timeout: wedged workload fails the op" 4 \
    env $ENV GPU_CR_OP_TIMEOUT_SEC=2 "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS"

# --- timeout leaves an uncommitted dest artifact ------------------------------
# Pins the exit-4 on-disk contract: the precreated dest survives, carries no
# commit marker, and a later restore must refuse it.
start_fake $ENV FAKE_NO_FINISH=1 FAKE_SKIP_COMMIT=1
check "timeout: dest-path ckpt fails" 4 \
    env $ENV GPU_CR_OP_TIMEOUT_SEC=2 "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/t4.bin"
expect_file "timeout leaves the precreated dest" "$WORK/t4.bin"
start_fake $ENV
check "timeout artifact: restore refuses the unmarked dump" 2 \
    env $ENV "$CR_CLIENT" -r -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/t4.bin"

# --- not-ready .so: selective ops refused pre-signal -------------------------
start_fake $ENV FAKE_NOT_READY=1
check "not-ready .so: dest-path op refused pre-signal" 3 \
    env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/dump.bin"
check "not-ready .so: buffer selective op refused pre-signal" 3 \
    env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS"
# The gate's worst case: a dest-path restore served from the stale per-PID
# buffer would replay old bytes into GPU memory — must refuse pre-signal.
check "not-ready .so: dest-path restore refused pre-signal" 3 \
    env $ENV "$CR_CLIENT" -r -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/dump.bin"
check "not-ready .so: buffer selective restore refused pre-signal" 3 \
    env $ENV "$CR_CLIENT" -r -p "$FAKE_PID" -s "$REGIONS"
# Full-process ops stay ungated: a silent .so (no op_status) keeps
# historical tolerance.
check "not-ready .so: full ckpt keeps historical behavior" 0 \
    env $ENV "$CR_CLIENT" -c -p "$FAKE_PID"

# --- usage errors ------------------------------------------------------------
check "usage: -o without -s"          1 env $ENV "$CR_CLIENT" -c -o "$WORK/dump.bin" -p 1
check "usage: -s without -c or -r"    1 env $ENV "$CR_CLIENT" -i -p 1 -s "$REGIONS"
check "usage: -o path too long"       1 env $ENV "$CR_CLIENT" -c -p 1 -s "$REGIONS" \
    -o "/$(printf 'a%.0s' $(seq 1 300))"
start_fake $ENV
check "usage: malformed -s fails at parse" 1 \
    env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "bogus"
check "usage: restore from missing file" 2 \
    env $ENV "$CR_CLIENT" -r -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/nonexistent.bin"

# --- destination hardening ----------------------------------------------------
check "dest: relative -o refused" 2 \
    env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS" -o "rel/dump.bin"
ln -s /etc/hostname "$WORK/link.bin"
check "dest: symlink dest refused (ELOOP)" 2 \
    env $ENV "$CR_CLIENT" -c -p "$FAKE_PID" -s "$REGIONS" -o "$WORK/link.bin"

echo
echo "cr_client integration: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
