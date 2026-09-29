#!/usr/bin/env bash
# Removes everything install.sh created (the namespace itself is left alone).
# Env: NS (required). Other install.sh variables are accepted and not needed.
set -euo pipefail
: "${NS:?NS is required}"
kubectl delete mutatingwebhookconfiguration "${NS}-daemonset-rule" --ignore-not-found
kubectl delete clusterrolebinding,clusterrole "${NS}-daemonset-rule" --ignore-not-found
kubectl -n "$NS" delete deployment,service,serviceaccount,role,rolebinding daemonset-rule --ignore-not-found --wait=false
kubectl -n "$NS" delete secret daemonset-rule-tls --ignore-not-found
