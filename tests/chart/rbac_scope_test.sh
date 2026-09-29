#!/bin/sh
# Render tests for the timesliceorchestrator chart value rbac.scope
# (decision D-ORCH-7): cluster (default) and namespaced.
#
# Needs helm 3. POSIX sh, so it runs in the alpine/helm image:
#   sh tests/chart/rbac_scope_test.sh
# BASE_CHART=<dir> (optional): a copy of the chart before rbac.scope existed.
# When set, TestRBACScope_Cluster_MatchesBase checks that rbac.scope=cluster
# (and the default) renders byte-for-byte what that chart renders.
set -eu

CHART=${CHART:-deploy/timesliceorchestrator}
BASE_CHART=${BASE_CHART:-}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
FAILS=0

fail() {
  echo "    FAIL: $*"
  return 1
}

run() {
  echo "=== RUN   $1"
  if "$1"; then
    echo "--- PASS: $1"
  else
    echo "--- FAIL: $1"
    FAILS=$((FAILS + 1))
  fi
}

render() { # render <out> <chart> <release> [helm args...]
  out=$1 chart=$2 rel=$3
  shift 3
  helm template "$rel" "$chart" "$@" >"$out"
}

# doc <file> <kind> <name> [namespace]: print the YAML document of that kind
# and name (and namespace, when given).
doc() {
  awk -v kind="$2" -v name="$3" -v want="${4:-}" '
    function flush() { if (k == kind && n == name && (want == "" || ns == want)) printf "%s", buf; buf = ""; k = ""; n = ""; ns = ""; meta = 0 }
    /^---/ { flush(); next }
    { buf = buf $0 "\n" }
    /^kind: / { k = $2 }
    /^metadata:/ { meta = 1; next }
    meta && /^  name: / { n = $2 }
    meta && /^  namespace: / { ns = $2 }
    /^[a-z]/ && !/^metadata:/ { meta = 0 }
    END { flush() }
  ' "$1"
}

# count <file> <kind> <name>: number of documents of that kind and name.
count() {
  awk -v kind="$2" -v name="$3" '
    function flush() { if (k == kind && n == name) c++; k = ""; n = ""; meta = 0 }
    /^---/ { flush(); next }
    /^kind: / { k = $2 }
    /^metadata:/ { meta = 1; next }
    meta && /^  name: / { n = $2; meta = 0 }
    /^[a-z]/ && !/^metadata:/ { meta = 0 }
    END { flush(); print c + 0 }
  ' "$1"
}

# namespaces <file> <kind> <name>: sorted namespaces of those documents.
namespaces() {
  awk -v kind="$2" -v name="$3" '
    function flush() { if (k == kind && n == name) print ns; k = ""; n = ""; ns = ""; meta = 0 }
    /^---/ { flush(); next }
    /^kind: / { k = $2 }
    /^metadata:/ { meta = 1; next }
    meta && /^  name: / { n = $2 }
    meta && /^  namespace: / { ns = $2 }
    /^[a-z]/ && !/^metadata:/ { meta = 0 }
    END { flush() }
  ' "$1" | sort
}

FULL="t-timesliceorchestrator"
V1="--set scope.watchNamespaces={w1}"
V3="--set scope.watchNamespaces={w1,w2,w3}"

# ---- option cluster (default) ----

TestRBACScope_Cluster_DefaultIsCluster() {
  # shellcheck disable=SC2086
  for vs in "" "$V1" "$V3"; do
    render "$TMP/d.yaml" "$CHART" t $vs
    render "$TMP/c.yaml" "$CHART" t $vs --set rbac.scope=cluster
    cmp -s "$TMP/d.yaml" "$TMP/c.yaml" || fail "default and rbac.scope=cluster differ for [$vs]" || return 1
  done
}

TestRBACScope_Cluster_MatchesBase() {
  if [ -z "$BASE_CHART" ]; then
    echo "    skip: BASE_CHART not set"
    return 0
  fi
  # shellcheck disable=SC2086
  for vs in "" "$V1" "$V3" \
    "--set namespace=demo-orch --set lock.configMap=demo-locks --set scope.watchNamespaces={demo-a,demo-b} --set scope.nodeSelector=pool=demo" \
    "--set rbac.create=false"; do
    render "$TMP/b.yaml" "$BASE_CHART" t $vs
    render "$TMP/h.yaml" "$CHART" t $vs
    render "$TMP/c.yaml" "$CHART" t $vs --set rbac.scope=cluster
    cmp -s "$TMP/b.yaml" "$TMP/h.yaml" || { diff "$TMP/b.yaml" "$TMP/h.yaml" || true; fail "default render differs from base for [$vs]"; } || return 1
    cmp -s "$TMP/b.yaml" "$TMP/c.yaml" || { diff "$TMP/b.yaml" "$TMP/c.yaml" || true; fail "rbac.scope=cluster render differs from base for [$vs]"; } || return 1
  done
}

TestRBACScope_Cluster_PodsAndNodesClusterWide() {
  # shellcheck disable=SC2086
  render "$TMP/c.yaml" "$CHART" t $V3 --set rbac.scope=cluster
  doc "$TMP/c.yaml" ClusterRole "$FULL" | grep -qx '    resources: \["pods", "nodes"\]' || fail "ClusterRole lacks pods and nodes" || return 1
  [ "$(count "$TMP/c.yaml" Role "$FULL-pods")" -eq 0 ] || fail "cluster renders pod Roles" || return 1
  [ "$(count "$TMP/c.yaml" RoleBinding "$FULL-pods")" -eq 0 ] || fail "cluster renders pod RoleBindings" || return 1
}

# ---- option namespaced ----

check_namespaced() { # check_namespaced <file> <n> <sorted namespaces, space separated>
  [ "$(count "$1" Role "$FULL-pods")" -eq "$2" ] || fail "want $2 pod Roles, got $(count "$1" Role "$FULL-pods")" || return 1
  [ "$(count "$1" RoleBinding "$FULL-pods")" -eq "$2" ] || fail "want $2 pod RoleBindings" || return 1
  got=$(namespaces "$1" Role "$FULL-pods" | tr '\n' ' ' | sed 's/ $//')
  [ "$got" = "$3" ] || fail "Role namespaces: want [$3], got [$got]" || return 1
  got=$(namespaces "$1" RoleBinding "$FULL-pods" | tr '\n' ' ' | sed 's/ $//')
  [ "$got" = "$3" ] || fail "RoleBinding namespaces: want [$3], got [$got]" || return 1
  cr=$(doc "$1" ClusterRole "$FULL")
  echo "$cr" | grep -qx '    resources: \["nodes"\]' || fail "ClusterRole does not grant nodes alone" || return 1
  if echo "$cr" | grep -q pods; then fail "ClusterRole still mentions pods"; return 1; fi
  echo "$cr" | grep -qx '    verbs: \["get", "list", "watch"\]' || fail "ClusterRole verbs changed" || return 1
  for ns in $3; do
    r=$(doc "$1" Role "$FULL-pods" "$ns")
    echo "$r" | grep -qx '    resources: \["pods"\]' || fail "Role in $ns does not grant pods" || return 1
    echo "$r" | grep -qx '    verbs: \["get", "list", "watch"\]' || fail "Role in $ns verbs" || return 1
  done
  for ns in $3; do
    b=$(doc "$1" RoleBinding "$FULL-pods" "$ns")
    echo "$b" | grep -q "^  kind: Role$" || fail "RoleBinding in $ns roleRef kind" || return 1
    echo "$b" | grep -q "^  name: $FULL-pods$" || fail "RoleBinding in $ns roleRef name" || return 1
    echo "$b" | grep -q "^    name: $FULL$" || fail "RoleBinding in $ns subject name" || return 1
    echo "$b" | grep -q "^    namespace: timeslice-system$" || fail "RoleBinding in $ns subject namespace" || return 1
  done
}

TestRBACScope_Namespaced_OneNamespace() {
  # shellcheck disable=SC2086
  render "$TMP/n.yaml" "$CHART" t $V1 --set rbac.scope=namespaced
  check_namespaced "$TMP/n.yaml" 1 "w1"
}

TestRBACScope_Namespaced_ThreeNamespaces() {
  # shellcheck disable=SC2086
  render "$TMP/n.yaml" "$CHART" t $V3 --set rbac.scope=namespaced
  check_namespaced "$TMP/n.yaml" 3 "w1 w2 w3"
}

TestRBACScope_Namespaced_DuplicatesCollapse() {
  render "$TMP/n.yaml" "$CHART" t --set 'scope.watchNamespaces={w2,w1,w2}' --set rbac.scope=namespaced
  check_namespaced "$TMP/n.yaml" 2 "w1 w2"
}

TestRBACScope_Namespaced_EmptyListFallsBack() {
  render "$TMP/c.yaml" "$CHART" t --set rbac.scope=cluster
  render "$TMP/n.yaml" "$CHART" t --set rbac.scope=namespaced
  cmp -s "$TMP/c.yaml" "$TMP/n.yaml" || { diff "$TMP/c.yaml" "$TMP/n.yaml" || true; fail "namespaced with [] differs from cluster"; } || return 1
}

TestRBACScope_Namespaced_OnlyRBACChanges() {
  # Everything except the ClusterRole and the new pod Roles/RoleBindings is
  # identical to the cluster render (lock ConfigMap Role included).
  # shellcheck disable=SC2086
  render "$TMP/c.yaml" "$CHART" t $V3 --set rbac.scope=cluster
  # shellcheck disable=SC2086
  render "$TMP/n.yaml" "$CHART" t $V3 --set rbac.scope=namespaced
  for kn in "Role $FULL-configmap" "RoleBinding $FULL-configmap" "ClusterRoleBinding $FULL" \
    "Deployment $FULL" "Service $FULL" "ServiceAccount $FULL"; do
    # shellcheck disable=SC2086
    [ "$(doc "$TMP/c.yaml" $kn)" = "$(doc "$TMP/n.yaml" $kn)" ] || fail "$kn differs" || return 1
    # shellcheck disable=SC2086
    [ -n "$(doc "$TMP/n.yaml" $kn)" ] || fail "$kn missing" || return 1
  done
  # No new verbs or resources anywhere.
  grep -E '^ +(resources|verbs):' "$TMP/c.yaml" | tr -d ' []"' | tr ',' '\n' | sed 's/^.*://' | sort -u >"$TMP/c.set"
  grep -E '^ +(resources|verbs):' "$TMP/n.yaml" | tr -d ' []"' | tr ',' '\n' | sed 's/^.*://' | sort -u >"$TMP/n.set"
  extra=$(comm -13 "$TMP/c.set" "$TMP/n.set")
  [ -z "$extra" ] || fail "namespaced adds verbs or resources: $extra" || return 1
  # Exactly 2 objects more per watched namespace.
  cn=$(grep -c '^kind: ' "$TMP/c.yaml")
  nn=$(grep -c '^kind: ' "$TMP/n.yaml")
  [ "$nn" -eq $((cn + 6)) ] || fail "want $((cn + 6)) objects, got $nn" || return 1
}

TestRBACScope_Namespaced_ReleaseNamesDoNotCollide() {
  render "$TMP/a.yaml" "$CHART" alpha --set 'scope.watchNamespaces={w1}' --set rbac.scope=namespaced
  render "$TMP/b.yaml" "$CHART" beta --set 'scope.watchNamespaces={w1}' --set rbac.scope=namespaced
  [ "$(count "$TMP/a.yaml" Role alpha-timesliceorchestrator-pods)" -eq 1 ] || fail "alpha Role name" || return 1
  [ "$(count "$TMP/b.yaml" Role beta-timesliceorchestrator-pods)" -eq 1 ] || fail "beta Role name" || return 1
  [ "$(count "$TMP/a.yaml" RoleBinding alpha-timesliceorchestrator-pods)" -eq 1 ] || fail "alpha RoleBinding name" || return 1
  [ "$(count "$TMP/b.yaml" RoleBinding beta-timesliceorchestrator-pods)" -eq 1 ] || fail "beta RoleBinding name" || return 1
}

TestRBACScope_Namespaced_CustomServiceAccountAndNamespace() {
  render "$TMP/n.yaml" "$CHART" t --set namespace=demo-orch --set serviceAccount.name=orch-sa \
    --set 'scope.watchNamespaces={w1}' --set rbac.scope=namespaced
  b=$(doc "$TMP/n.yaml" RoleBinding "$FULL-pods")
  echo "$b" | grep -q '^    name: orch-sa$' || fail "subject is not the chart ServiceAccount" || return 1
  echo "$b" | grep -q '^    namespace: demo-orch$' || fail "subject namespace is not the chart namespace" || return 1
}

TestRBACScope_Namespaced_RBACCreateFalse() {
  render "$TMP/n.yaml" "$CHART" t --set 'scope.watchNamespaces={w1}' --set rbac.scope=namespaced --set rbac.create=false
  if grep -qE '^kind: (Cluster)?Role(Binding)?$' "$TMP/n.yaml"; then fail "RBAC rendered with rbac.create=false"; return 1; fi
}

TestRBACScope_InvalidValueFails() {
  if helm template t "$CHART" --set rbac.scope=bogus >"$TMP/x.yaml" 2>"$TMP/x.err"; then
    fail "rbac.scope=bogus rendered"
    return 1
  fi
  grep -q 'rbac.scope must be' "$TMP/x.err" || fail "unexpected error: $(cat "$TMP/x.err")" || return 1
}

helm lint "$CHART" >/dev/null
helm lint "$CHART" --set rbac.scope=namespaced --set 'scope.watchNamespaces={w1}' >/dev/null

for t in \
  TestRBACScope_Cluster_DefaultIsCluster \
  TestRBACScope_Cluster_MatchesBase \
  TestRBACScope_Cluster_PodsAndNodesClusterWide \
  TestRBACScope_Namespaced_OneNamespace \
  TestRBACScope_Namespaced_ThreeNamespaces \
  TestRBACScope_Namespaced_DuplicatesCollapse \
  TestRBACScope_Namespaced_EmptyListFallsBack \
  TestRBACScope_Namespaced_OnlyRBACChanges \
  TestRBACScope_Namespaced_ReleaseNamesDoNotCollide \
  TestRBACScope_Namespaced_CustomServiceAccountAndNamespace \
  TestRBACScope_Namespaced_RBACCreateFalse \
  TestRBACScope_InvalidValueFails; do
  run "$t"
done

if [ "$FAILS" -ne 0 ]; then
  echo "FAIL ($FAILS)"
  exit 1
fi
echo "PASS"
