#!/usr/bin/env bash
# Render the RL donor manifest for the demo to stdout.
#
#   RL_DONOR=pod|rayjob [NS=..] [REGISTRY=..] [GROUP=..] [JOB=..] \
#     [GROUP_NODES=1] [TRAINER_REPLICAS=..] [ORCH_ADDR=..] render.sh [donor|stock|claim]
#
# RL_DONOR   pod: today's bare Pods with hand labels (rl-pod.yaml).
#            rayjob: a KubeRay RayJob whose trainers group carries
#            timeslice.io/donor (rl-rayjob.yaml; needs KubeRay, see kuberay/).
#            Default pod. A stack that defaults to the RayJob sets it here.
# NS, REGISTRY, GROUP, JOB
#            Defaults default, the literal ${REGISTRY}, trainers, rl-trainer.
#            With all defaults, RL_DONOR=pod prints today's manifest byte for
#            byte (check.sh). JOB is also the fixed job-id: the
#            timeslice.io/job-id label, TIMESLICE_JOB_ID and, for rayjob, the
#            RayJob name.
# GROUP_NODES, TRAINER_REPLICAS
#            Number of group hosts; TRAINER_REPLICAS defaults to GROUP_NODES
#            and sets the trainers group size (rayjob). The Pod form has one
#            trainer Pod, so it takes GROUP_NODES=1 only.
# ORCH_ADDR  Orchestrator address for TIMESLICE_ORCH_ADDR. Default: the
#            installed chart's Service.
# Argument   donor (default): the donor manifest. stock: its stock reference
#            (the same workload without time-slicing, for the diff). claim:
#            the shared ResourceClaim the trainer references, in NS.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
what=${1:-donor}
RL_DONOR=${RL_DONOR:-pod}
NS=${NS:-default}
# shellcheck disable=SC2016 # the literal placeholder of today's manifest
REGISTRY=${REGISTRY:-'${REGISTRY}'}
GROUP=${GROUP:-trainers}
JOB=${JOB:-rl-trainer}
GROUP_NODES=${GROUP_NODES:-1}
TRAINER_REPLICAS=${TRAINER_REPLICAS:-$GROUP_NODES}
default_orch=timeslice-timesliceorchestrator.timeslice-system.svc:50051
ORCH_ADDR=${ORCH_ADDR:-$default_orch}

die() { echo "render.sh: $*" >&2; exit 2; }

case "$RL_DONOR" in
  pod) donor=rl-pod.yaml stock=rl-pod-stock.yaml ;;
  rayjob) donor=rl-rayjob.yaml stock=rl-rayjob-stock.yaml ;;
  *) die "RL_DONOR must be pod or rayjob, got '$RL_DONOR'" ;;
esac
case "$what" in
  donor) src=$donor ;;
  stock) src=$stock ;;
  claim) src=rl-claim.yaml ;;
  *) die "argument must be donor, stock or claim, got '$what'" ;;
esac
if [ "$RL_DONOR" = pod ] && [ "$GROUP_NODES" != 1 ]; then
  die "the Pod form has one trainer Pod; GROUP_NODES must be 1, got '$GROUP_NODES'"
fi
for v in "$NS" "$GROUP" "$JOB" "$TRAINER_REPLICAS" "$ORCH_ADDR" "$REGISTRY"; do
  case "$v" in *'|'* | *'&'* | *'\'*) die "value '$v' contains | & or \\" ;; esac
done

render() {
  # Line 1 of the Pod-form files only tells yamllint to accept today's
  # sequence indentation; it is not part of the manifest.
  sed -e '1{/^# yamllint disable rule:indentation$/d;}' \
    -e "s|__NS__|$NS|g" \
    -e "s|__REGISTRY__|$REGISTRY|g" \
    -e "s|__GROUP__|$GROUP|g" \
    -e "s|__JOB__|$JOB|g" \
    -e "s|__TRAINER_REPLICAS__|$TRAINER_REPLICAS|g" \
    -e "s|$default_orch|$ORCH_ADDR|g" \
    "$here/$1"
}

# The Pod form's rollout joins the head at run_rollout.sh's default address,
# which names namespace default; elsewhere it gets the address in its env.
if [ "$RL_DONOR" = pod ] && [ "$what" != claim ] && [ "$NS" != default ]; then
  render "$src" | awk -v addr="rl-head.$NS.svc.cluster.local:6379" '
    /^  name: rl-rollout$/ { rollout = 1 }
    { print }
    rollout && /^    env:$/ {
      print "    - name: RAY_HEAD_ADDR"
      print "      value: \"" addr "\""
      rollout = 0
    }'
else
  render "$src"
fi
