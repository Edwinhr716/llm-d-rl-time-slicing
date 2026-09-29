#!/usr/bin/env bash
# Make the pinned KubeRay available for RL_DONOR=rayjob.
#
#   install.sh           install the operator if the RayJob CRD is absent
#   install.sh --check   only report what is installed; exit 1 if absent
#
# If the CRD already exists (for example a managed Ray add-on, or an earlier
# install), nothing is installed over it: the script prints the operator
# version it finds and warns when it differs from the pin. The RayJob in
# ../rl-rayjob.yaml uses ray.io/v1 fields that exist in KubeRay >= 1.1.
set -euo pipefail

KUBERAY_VERSION=${KUBERAY_VERSION:-1.5.1}
KUBERAY_NS=${KUBERAY_NS:-kuberay-system}
here=$(cd "$(dirname "$0")" && pwd)

if kubectl get crd rayjobs.ray.io >/dev/null 2>&1; then
  found=$(kubectl get deploy -A -o jsonpath='{range .items[*]}{.spec.template.spec.containers[*].image}{"\n"}{end}' |
    grep -E 'kuberay/operator|kuberay-operator|ray-operator' | head -1 || true)
  echo "KUBERAY present crd=rayjobs.ray.io operator_image=${found:-not-visible (managed)} pin=v${KUBERAY_VERSION}"
  case "$found" in
    *":v${KUBERAY_VERSION}"* | "") ;;
    *) echo "KUBERAY warning: operator image differs from the pin v${KUBERAY_VERSION}" >&2 ;;
  esac
  exit 0
fi
if [ "${1:-}" = --check ]; then
  echo "KUBERAY absent"
  exit 1
fi
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ >/dev/null
helm repo update kuberay >/dev/null
helm upgrade --install kuberay-operator kuberay/kuberay-operator \
  --version "$KUBERAY_VERSION" --namespace "$KUBERAY_NS" --create-namespace \
  -f "$here/values.yaml" --wait --timeout 5m
kubectl wait --for=condition=Established crd/rayjobs.ray.io --timeout=120s
echo "KUBERAY installed version=v${KUBERAY_VERSION} namespace=${KUBERAY_NS}"
