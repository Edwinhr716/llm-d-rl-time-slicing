#!/usr/bin/env bash
# Installs the D-VK-7 "rule" webhook (webhook.yaml) and returns only once it is active: a
# server-side dry-run DaemonSet in CHECK_NS comes back with the virtual-node exclusion.
#
# Env: NS (guest-kubelet namespace, required), IMAGE (guest-kubelet image, required),
#      HOST (node for the webhook pods; empty = any node),
#      SCOPE_KEY / SCOPE_VALUE (namespace label the rule is limited to; empty = all namespaces),
#      CHECK_NS (namespace for the activity check, default NS; must match the scope),
#      TIMEOUT_S (default 180). VK_NODE is accepted and not needed.
set -euo pipefail
: "${NS:?NS is required}" "${IMAGE:?IMAGE is required}"
HOST="${HOST:-}"
SCOPE_KEY="${SCOPE_KEY:-}"
SCOPE_VALUE="${SCOPE_VALUE:-}"
CHECK_NS="${CHECK_NS:-$NS}"
TIMEOUT_S="${TIMEOUT_S:-180}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

render() {
  local drop=()
  [ -z "$SCOPE_KEY" ] && drop+=(-e '/namespaceSelector:/,/__SCOPE_KEY__/d')
  [ -z "$HOST" ] && drop+=(-e '/nodeSelector:/,/__HOST__/d')
  sed ${drop[@]+"${drop[@]}"} \
    -e "s|__NS__|${NS}|g" -e "s|__IMAGE__|${IMAGE}|g" -e "s|__HOST__|${HOST}|g" \
    -e "s|__SCOPE_KEY__|${SCOPE_KEY}|g" -e "s|__SCOPE_VALUE__|${SCOPE_VALUE}|g" \
    "$DIR/webhook.yaml"
}

probe() {
  kubectl create --dry-run=server -o yaml -f - <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: daemonset-rule-probe
  namespace: ${CHECK_NS}
spec:
  selector:
    matchLabels: {app: daemonset-rule-probe}
  template:
    metadata:
      labels: {app: daemonset-rule-probe}
    spec:
      containers:
        - {name: c, image: registry.k8s.io/pause:3.10}
EOF
}

kubectl get ns "$NS" >/dev/null 2>&1 || kubectl create ns "$NS"
render | kubectl apply -f -
kubectl -n "$NS" rollout status deploy/daemonset-rule --timeout="${TIMEOUT_S}s"

end=$(( $(date +%s) + TIMEOUT_S ))
while [ "$(date +%s)" -lt "$end" ]; do
  if probe 2>/dev/null | grep -q 'timeslice.io/virtual-node'; then
    echo "daemonset-rule active (namespace ${NS}, scope ${SCOPE_KEY:-all}=${SCOPE_VALUE:-})"
    exit 0
  fi
  sleep 2
done
echo "daemonset-rule not active after ${TIMEOUT_S}s" >&2
kubectl -n "$NS" logs deploy/daemonset-rule --tail=30 >&2 || true
exit 1
