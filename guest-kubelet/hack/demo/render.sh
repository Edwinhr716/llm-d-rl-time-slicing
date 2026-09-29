#!/usr/bin/env bash
# Renders the demo manifests for one host or for a group of two hosts (decision D-NS-18:
# option keep is GROUP_NODES=1, option ns-multi is GROUP_NODES=2). Output goes to stdout, for
# `render.sh <part> | kubectl apply -f -`. Needs only bash and sed, so it runs in the
# google-cloud-cli image next to kubectl.
#
# GROUP_NODES=1 (the default) prints today's manifests byte for byte: the same sed as
# `make deploy` and `make gpu-guest` for the guest-kubelet files, the RL files unchanged.
# GROUP_NODES=2 derives every per-host object from those same files:
#   vk         one guest-kubelet Deployment per host (--host-node, --node-name, its own claim or
#              --gpu-claim-mode=donor; the Lease name follows the node name)
#   claim      one ResourceClaim and one trainer stand-in per host
#   guest      the vLLM guest as a Deployment, one replica per VK node (required anti-affinity
#              on the VK hostname); the pods keep the app label, so a pool selecting it has both
#   rl-claims  one shared trainer claim per host
#   rl-job     verl trainer.nnodes=2: rl-head on host 0, rl-trainer-1 on host 1, each with its
#              host's claim; rl-rollout keeps its anti-affinity to the group label
#   labels     one `kubectl label node` line per host (the group label)
#
# Usage: render.sh vk|claim|guest|rl-claims|rl-job|labels
# Environment:
#   GROUP_NODES  1 (default) or 2
#   HOSTS        the group's real nodes, space separated, GROUP_NODES of them (HOST works for 1);
#                the VK node of host h is vk-<last dash-separated part of h>, as in the Makefile
#   IMAGE        guest-kubelet image (part vk)
#   CLAIM_MODE   static (default, today): the VK on host i gets --gpu-claim=<claim>-<i>
#                (GROUP_NODES=1: the claim of deployment.yaml); donor: --gpu-claim-mode=donor,
#                and the trainer stand-ins carry the job label the donor selector looks for
#   GROUP_LABEL  group label of the hosts (default group.timeslice.io/trainers=true)
#   RL_DIR       directory of rl-job.yaml and resource-claims.yaml
set -euo pipefail
shopt -u patsub_replacement 2>/dev/null || true # bash 5.2: '&' in a replacement stays literal

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DEPLOY=$HERE/../../deploy
RL_DIR=${RL_DIR:-$HERE/../../../guides/rl-batch-interleaving/examples}
GROUP_NODES=${GROUP_NODES:-1}
CLAIM_MODE=${CLAIM_MODE:-static}
GROUP_LABEL=${GROUP_LABEL:-group.timeslice.io/trainers=true}
IMAGE=${IMAGE:-}
read -r -a HOSTS_A <<<"${HOSTS:-${HOST:-}}"

die() {
  echo "render.sh: $*" >&2
  exit 1
}

# vnode <host>: the VK node name, as VNODE in the Makefile.
vnode() { printf 'vk-%s' "${1##*-}"; }

# count <text> <literal>: how many times literal occurs in text.
count() {
  local s=$1 n=0
  while [[ $s == *"$2"* ]]; do
    s=${s#*"$2"}
    n=$((n + 1))
  done
  echo "$n"
}

# sub <want> <text> <old> <new>: replace every old, which must occur exactly want times.
sub() {
  local n
  n=$(count "$2" "$3")
  [[ $n == "$1" ]] || die "want $1 of '${3%%$'\n'*}', found $n"
  printf '%s' "${2//"$3"/"$4"}"
}

# today <file>: the Makefile's SUBST (first match of each placeholder on each line).
today() {
  sed -e "s|__IMAGE__|${IMAGE}|" -e "s|__HOST__|${HOSTS_A[0]:-}|" -e "s|__VNODE__|$(vnode "${HOSTS_A[0]:-}")|" "$1"
}

# docs <doc>...: print YAML documents separated by ---.
docs() {
  local first=1 d
  for d in "$@"; do
    [[ $first == 1 ]] || printf -- '---\n'
    first=0
    printf '%s\n' "$d"
  done
}

need_hosts() {
  [[ ${#HOSTS_A[@]} == "$GROUP_NODES" ]] || die "HOSTS has ${#HOSTS_A[@]} hosts, GROUP_NODES is $GROUP_NODES"
  local v seen=" " h
  for h in "${HOSTS_A[@]}"; do
    v=$(vnode "$h")
    [[ $seen != *" $v "* ]] || die "hosts share the VK node name $v"
    seen+="$v "
  done
}

CLAIM_ARG='            - --gpu-claim=shared-m1-gpu-claim'
DONOR_ARG='            - --gpu-claim-mode=donor'
STANDIN_META=$'  name: trainer-standin\n  namespace: guest-kubelet-proto\n'
STANDIN_LABELS=$'  labels: {timeslice.io/job-id: trainer-standin}\n'

render_vk() {
  need_hosts
  [[ -n $IMAGE ]] || die "IMAGE is not set"
  if [[ $GROUP_NODES == 1 ]]; then
    if [[ $CLAIM_MODE == static ]]; then
      today "$DEPLOY/deployment.yaml"
    else
      sub 1 "$(today "$DEPLOY/deployment.yaml")" "$CLAIM_ARG" "$DONOR_ARG"
      echo
    fi
    return
  fi
  local src out=() i h v txt claim=$DONOR_ARG
  src=$(<"$DEPLOY/deployment.yaml")
  for i in "${!HOSTS_A[@]}"; do
    h=${HOSTS_A[$i]}
    v=$(vnode "$h")
    [[ $CLAIM_MODE == donor ]] || claim="$CLAIM_ARG-$i"
    txt=$(sub 1 "$src" $'\n  name: guest-kubelet\n' $'\n  name: guest-kubelet-'"$v"$'\n')
    txt=$(sub 2 "$txt" '{app: guest-kubelet}' "{app: guest-kubelet, timeslice.io/vk-node: $v}")
    txt=$(sub 2 "$txt" '__HOST__' "$h")
    txt=$(sub 2 "$txt" '__IMAGE__' "$IMAGE")
    txt=$(sub 1 "$txt" $'\n            - --leader-elect=true\n' \
      $'\n            - --leader-elect=true\n            - --host-node='"$h"$'\n            - --node-name='"$v"$'\n')
    txt=$(sub 1 "$txt" "$CLAIM_ARG" "$claim")
    out+=("# GROUP_NODES=2, host $i: the VK for $h (node $v)."$'\n'"$txt")
  done
  docs "${out[@]}"
}

render_claim() {
  need_hosts
  local src txt
  if [[ $GROUP_NODES == 1 ]]; then
    if [[ $CLAIM_MODE == static ]]; then
      today "$DEPLOY/m1/claim.yaml"
    else
      sub 1 "$(today "$DEPLOY/m1/claim.yaml")" "$STANDIN_META" "$STANDIN_META$STANDIN_LABELS"
      echo
    fi
    return
  fi
  local out=() i h
  src=$(<"$DEPLOY/m1/claim.yaml")
  for i in "${!HOSTS_A[@]}"; do
    h=${HOSTS_A[$i]}
    txt=$src
    [[ $CLAIM_MODE == static ]] || txt=$(sub 1 "$txt" "$STANDIN_META" "$STANDIN_META$STANDIN_LABELS")
    txt=$(sub 2 "$txt" 'shared-m1-gpu-claim' "shared-m1-gpu-claim-$i")
    txt=$(sub 1 "$txt" 'name: trainer-standin' "name: trainer-standin-$i")
    txt=$(sub 1 "$txt" '__HOST__' "$h")
    out+=("# GROUP_NODES=2, host $i ($h)."$'\n'"$txt")
  done
  docs "${out[@]}"
}

render_guest() {
  need_hosts
  if [[ $GROUP_NODES == 1 ]]; then
    today "$DEPLOY/m1/vllm-guest.yaml"
    return
  fi
  local src head body vnodes="" h
  for h in "${HOSTS_A[@]}"; do vnodes+="${vnodes:+, }$(vnode "$h")"; done
  src=$(<"$DEPLOY/m1/vllm-guest.yaml")
  src=$(sub 1 "$src" 'labels: {app: vllm-guest}' 'labels: {app: vllm-guest}')
  src=$(sub 1 "$src" $'\nspec:\n' $'\nspec:\n')
  head=${src%%$'\nspec:\n'*}
  body=${src#*$'\nspec:\n'}
  head=$(sub 1 "$head" $'apiVersion: v1\nkind: Pod\n' $'apiVersion: apps/v1\nkind: Deployment\n')
  body=$(sub 1 "$body" $'  nodeSelector:\n    kubernetes.io/hostname: __VNODE__\n' "$(
    cat <<EOF
  # GROUP_NODES=2: one replica on each VK node of the group.
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - {key: kubernetes.io/hostname, operator: In, values: [$vnodes]}
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - topologyKey: kubernetes.io/hostname
          labelSelector:
            matchLabels: {app: vllm-guest}
EOF
  )"$'\n')
  printf '%s\n' "$head"
  cat <<EOF
spec:
  replicas: $GROUP_NODES
  selector:
    matchLabels: {app: vllm-guest}
  template:
    metadata:
      labels: {app: vllm-guest}
    spec:
EOF
  printf '%s\n' "$body" | sed 's/^./    &/'
}

render_rl_claims() {
  if [[ $GROUP_NODES == 1 ]]; then
    cat "$RL_DIR/resource-claims.yaml"
    return
  fi
  local src out=() i
  src=$(<"$RL_DIR/resource-claims.yaml")
  for ((i = 0; i < GROUP_NODES; i++)); do
    out+=("$(sub 1 "$src" 'name: shared-trainers-gpu-claim' "name: shared-trainers-gpu-claim-$i")")
  done
  docs "${out[@]}"
}

# The second trainer's script: the rollout's join loop with the trainer_node resource, so verl's
# trainer pool (nnodes=2) puts one rank here. Inserted into the rlbatch-trainer ConfigMap.
IFS= read -r -d '' TRAINER_WORKER_SH <<'EOF' || true
  run_trainer_worker.sh: |
    #!/usr/bin/env bash
    # GROUP_NODES=2 only: the second trainer, on the second group host. It joins rl-head's
    # ray cluster with the trainer_node resource, so verl's trainer pool (trainer.nnodes=2)
    # places one trainer rank on this node.
    set -xeuo pipefail

    trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

    RESULTS_DIR="/workspace/results"
    RAY_HEAD_ADDR="rl-head.default.svc.cluster.local:6379"

    mkdir -p "$RESULTS_DIR"

    echo "=== Phase 0: verify GPU visibility (DRA-injected) ==="
    NVIS=$(nvidia-smi -L | wc -l)
    nvidia-smi -L
    if [ "$NVIS" != 1 ]; then
        echo "FATAL: expected exactly 1 visible GPU on the trainer node, got $NVIS"; exit 96
    fi

    source /workspace/scripts/install_common.sh

    # Same dataset as the head (same seed): verl may place a trainer-side reader here.
    python3 /workspace/scripts/data_prep.py --out_dir /workspace/data/eurus_code \
        --tokenizer deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B --train_size 512 --val_size 64

    echo "=== Phase 2: join the ray cluster as a trainer node ==="
    set +e
    while true; do
        ray start --block --address="$RAY_HEAD_ADDR" --num-gpus=1 --resources='{"trainer_node": 100}'
        RC=$?
        echo "ray start exited rc=$RC (head not up yet, or head gone); retrying in 15s — $(date)"
        ray stop --force >/dev/null 2>&1 || true
        sleep 15
    done
EOF

render_rl_job() {
  if [[ $GROUP_NODES == 1 ]]; then
    cat "$RL_DIR/rl-job.yaml"
    return
  fi
  need_hosts
  local src sep pre rest headdoc after head0 worker
  local rl_claim='resourceClaimName: shared-trainers-gpu-claim   # SAME claim as shadow-vllm'
  local group_sel=$'    group.timeslice.io/trainers: "true"\n'
  src=$(<"$RL_DIR/rl-job.yaml")
  src=$(sub 1 "$src" "trainer.nnodes=1 \\" "trainer.nnodes=2 \\")
  src=$(sub 1 "$src" 'sys.exit(0 if gpus >= 2 else 1)' 'sys.exit(0 if gpus >= 3 else 1)')
  src=$(sub 1 "$src" $'\n  run_rollout.sh: |\n' $'\n'"$TRAINER_WORKER_SH"$'\n  run_rollout.sh: |\n')

  # Split out the rl-head document; rl-trainer-1 is a copy of it.
  sep=$'\n---\napiVersion: v1\nkind: Pod\nmetadata:\n  name: rl-head\n'
  src=$(sub 1 "$src" "$sep" "$sep")
  pre=${src%%"$sep"*}
  rest=${src#"$pre"$'\n---\n'}
  headdoc=${rest%%$'\n---\n'*}
  after=${rest#"$headdoc"}

  head0=$(sub 1 "$headdoc" "$group_sel" "$group_sel    kubernetes.io/hostname: ${HOSTS_A[0]}"$'\n')
  head0=$(sub 1 "$head0" "$rl_claim" 'resourceClaimName: shared-trainers-gpu-claim-0   # SAME claim as the guest on this host')

  worker=$(sub 1 "$headdoc" $'\n  name: rl-head\n' $'\n  name: rl-trainer-1\n')
  worker=$(sub 1 "$worker" 'app: rl-head' 'app: rl-trainer-1')
  worker=$(sub 1 "$worker" "$group_sel" "$group_sel    kubernetes.io/hostname: ${HOSTS_A[1]}"$'\n')
  worker=$(sub 1 "$worker" "$rl_claim" 'resourceClaimName: shared-trainers-gpu-claim-1   # SAME claim as the guest on this host')
  worker=$(sub 1 "$worker" 'HEAD (trainer node)' 'TRAINER WORKER (second group host)')
  worker=$(sub 1 "$worker" 'bash /workspace/scripts/run_head.sh' 'bash /workspace/scripts/run_trainer_worker.sh')

  printf '%s\n---\n%s\n---\n' "$pre" "$head0"
  printf '%s\n' '# GROUP_NODES=2: the second trainer, on the second group host. It has the job and group' \
    '# labels of rl-head, so the orchestrator sees one job on two hosts.'
  printf '%s%s\n' "$worker" "$after"
}

render_labels() {
  need_hosts
  local h
  for h in "${HOSTS_A[@]}"; do
    printf 'kubectl label node %s %s --overwrite\n' "$h" "$GROUP_LABEL"
  done
}

[[ $GROUP_NODES == 1 || $GROUP_NODES == 2 ]] || die "GROUP_NODES must be 1 or 2, not $GROUP_NODES"
[[ $CLAIM_MODE == static || $CLAIM_MODE == donor ]] || die "CLAIM_MODE must be static or donor, not $CLAIM_MODE"
case ${1:-} in
  vk) render_vk ;;
  claim) render_claim ;;
  guest) render_guest ;;
  rl-claims) render_rl_claims ;;
  rl-job) render_rl_job ;;
  labels) render_labels ;;
  *) die "usage: render.sh vk|claim|guest|rl-claims|rl-job|labels" ;;
esac
