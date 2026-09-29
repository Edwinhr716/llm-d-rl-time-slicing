# Nothing here runs Go or Docker locally; builds and tests run in Cloud Build.
PROJECT ?= YOUR_PROJECT
REGION  ?= YOUR_REGION
TAG     ?= $(shell git rev-parse --short HEAD)
IMAGE   := $(REGION)-docker.pkg.dev/$(PROJECT)/guest-kubelet/guest-kubelet:$(TAG)
CONTEXT ?= YOUR_KUBE_CONTEXT
KUBECTL := kubectl --context $(CONTEXT)
HOST    ?= YOUR_GPU_NODE
VNODE   ?= vk-$(lastword $(subst -, ,$(HOST)))
GROUP   ?= YOUR_GROUP
ORCH    ?= timeslice-timesliceorchestrator.timeslice-system.svc:50051
SUBST   := sed -e 's|__IMAGE__|$(IMAGE)|' -e 's|__HOST__|$(HOST)|' -e 's|__VNODE__|$(VNODE)|' \
	-e 's|__GROUP__|$(GROUP)|' -e 's|__ORCH_ADDR__|$(ORCH)|'

.PHONY: build deploy test-guest gpu-guest status undeploy

build: ## vet, test, build and push the image remotely
	gcloud builds submit --project=$(PROJECT) --config cloudbuild.yaml --substitutions=_TAG=$(TAG) .

deploy: ## namespace, RBAC, Deployment (image tag = TAG; GROUP = the donor's group, ORCH = orchestrator)
	$(KUBECTL) apply -f deploy/namespace.yaml -f deploy/rbac.yaml
	$(SUBST) deploy/deployment.yaml | $(KUBECTL) apply -f -

test-guest: ## step one: CPU-only guest and a Service selecting it
	$(SUBST) deploy/test-guest.yaml | $(KUBECTL) apply -f -

gpu-guest: ## step two: claim + trainer stand-in, then the vLLM guest
	$(SUBST) deploy/m1/claim.yaml | $(KUBECTL) apply -f -
	$(SUBST) deploy/m1/vllm-guest.yaml | $(KUBECTL) apply -f -

status:
	$(KUBECTL) get node $(VNODE) -o wide
	$(KUBECTL) -n guest-kubelet-proto get pods -o wide
	$(KUBECTL) -n guest-kubelet-proto get lease

undeploy: ## namespace first (guests, then mirrors via ownerRef), then cluster-scoped objects
	-$(KUBECTL) delete ns guest-kubelet-proto --wait=true
	-$(KUBECTL) delete node $(VNODE)
	-$(KUBECTL) -n kube-node-lease delete lease $(VNODE)
	-$(KUBECTL) delete clusterrole,clusterrolebinding guest-kubelet-proto
	-$(KUBECTL) -n kube-node-lease delete role,rolebinding guest-kubelet-proto
