#!/usr/bin/env bash
# Checks for render.sh. Needs bash, sed, awk and sha256sum only.
#   - RL_DONOR=pod with all defaults prints today's RL manifest byte for byte,
#     and the claim prints today's ResourceClaim (sha256 below; update both
#     hashes together with rl-pod.yaml / rl-claim.yaml when today's manifest
#     changes).
#   - Every form renders with no placeholder left.
#   - The RayJob keeps the three north-star deltas on the right groups.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
pod_sha=0548642c0c679d83d2abae71521d6bf897f3afef0f37b25b7d470d9364328ad5
claim_sha=15f96d4151eabdb302b8634e2776307c4dcac8d744d43909aad346c0631faa30
fail=0
ok() { echo "PASS $*"; }
bad() { echo "FAIL $*"; fail=1; }
sha() { sha256sum | cut -d' ' -f1; }

got=$(env -u NS -u REGISTRY -u GROUP -u JOB -u GROUP_NODES -u TRAINER_REPLICAS -u ORCH_ADDR \
  RL_DONOR=pod "$here/render.sh" | sha)
[ "$got" = "$pod_sha" ] && ok "pod default is byte-identical" || bad "pod default sha $got, want $pod_sha"
got=$(env -u NS RL_DONOR=pod "$here/render.sh" claim | sha)
[ "$got" = "$claim_sha" ] && ok "claim default is byte-identical" || bad "claim default sha $got, want $claim_sha"
got=$(env -u RL_DONOR -u NS -u REGISTRY -u GROUP -u JOB "$here/render.sh" | sha)
[ "$got" = "$pod_sha" ] && ok "RL_DONOR defaults to pod" || bad "unset RL_DONOR did not render the pod form"

for d in pod rayjob; do
  for w in donor stock claim; do
    out=$(RL_DONOR=$d NS=ns1 REGISTRY=reg.example/r GROUP=g1 JOB=job1 "$here/render.sh" "$w")
    if grep -v '^ *#' <<<"$out" | grep -n '__[A-Z_]*__\|\${REGISTRY}'; then bad "$d $w leaves placeholders"; else ok "$d $w renders"; fi
  done
done

out=$(RL_DONOR=pod NS=ns1 "$here/render.sh")
[ "$(grep -c 'rl-head.ns1.svc.cluster.local:6379' <<<"$out")" = 1 ] && ok "pod outside default gets RAY_HEAD_ADDR" || bad "pod outside default lacks RAY_HEAD_ADDR"
if RL_DONOR=pod GROUP_NODES=2 "$here/render.sh" >/dev/null 2>&1; then bad "pod accepted GROUP_NODES=2"; else ok "pod refuses GROUP_NODES=2"; fi

out=$(RL_DONOR=rayjob JOB=job1 GROUP=g1 GROUP_NODES=2 "$here/render.sh")
for p in 'name: job1$' 'jobId: job1$' 'replicas: 2$' 'timeslice.io/donor: "true"' \
  'timeslice.io/job-id: job1$' 'TIMESLICE_JOB_ID: "job1"' 'trainer_node\\": 100' 'rollout_node\\": 100' \
  ' async_training.trainer_name=timeslice$' 'ray_pg_extra_resources='; do
  grep -q -- "$p" <<<"$out" && ok "rayjob has $p" || bad "rayjob lacks $p"
done
[ "$(grep -c 'timeslice.io/donor: ' <<<"$out")" = 1 ] && ok "donor label on one group" || bad "donor label count"

exit "$fail"
