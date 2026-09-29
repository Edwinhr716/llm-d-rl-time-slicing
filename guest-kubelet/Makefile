# Nothing here runs Go or Docker locally; builds and tests run in Cloud Build.
PROJECT ?= YOUR_PROJECT
REGION  ?= YOUR_REGION
TAG     ?= $(shell git rev-parse --short HEAD)
IMAGE   := $(REGION)-docker.pkg.dev/$(PROJECT)/guest-kubelet/guest-kubelet:$(TAG)
CONTEXT ?= YOUR_KUBE_CONTEXT
KUBECTL := kubectl --context $(CONTEXT)
HOST    ?= YOUR_GPU_NODE
VNODE   ?= vk-$(lastword $(subst -, ,$(HOST)))
SUBST   := sed -e 's|__IMAGE__|$(IMAGE)|' -e 's|__HOST__|$(HOST)|' -e 's|__VNODE__|$(VNODE)|' -e 's|__VK_NODE__|$(VNODE)|g'

.PHONY: build deploy node-guard test-guest gpu-guest m2-deploy q5-force q5-toggle status undeploy

build: ## vet, test, build and push the image remotely
	gcloud builds submit --project=$(PROJECT) --config cloudbuild.yaml --substitutions=_TAG=$(TAG) .

deploy: ## namespace, RBAC, Deployment (image tag = TAG); option d: then make node-guard
	$(KUBECTL) apply -f deploy/namespace.yaml -f deploy/rbac.yaml
	$(SUBST) deploy/deployment.yaml | $(KUBECTL) apply -f -

node-guard: ## D-VK-2 option d: delete policy for the virtual Node plus the donor-controller stand-in
	for f in deploy/opt-d/*.yaml; do $(SUBST) $$f | $(KUBECTL) apply -f - || exit 1; done

test-guest: ## step one: CPU-only guest and a Service selecting it
	$(SUBST) deploy/test-guest.yaml | $(KUBECTL) apply -f -

gpu-guest: ## step two: claim + trainer stand-in, then the vLLM guest
	$(SUBST) deploy/m1/claim.yaml | $(KUBECTL) apply -f -
	$(SUBST) deploy/m1/vllm-guest.yaml | $(KUBECTL) apply -f -

m2-deploy: ## M2: probed guests, InferencePool + EPP + Envoy router, measurement RBAC
	$(SUBST) deploy/m2/guests.yaml | $(KUBECTL) apply -f -
	$(KUBECTL) apply -f deploy/m2/router.yaml -f deploy/m2/measure-rbac.yaml

q5-force q5-toggle: q5-%: ## M2: one Q5 run (re-runnable); results in the pod log
	-$(KUBECTL) -n guest-kubelet-proto delete pod q5-measure-$* --wait=true
	$(SUBST) deploy/m2/measure-$*.yaml | $(KUBECTL) apply -f -
	$(KUBECTL) -n guest-kubelet-proto wait --for=condition=Ready pod/q5-measure-$* --timeout=120s
	$(KUBECTL) -n guest-kubelet-proto logs -f q5-measure-$*

status:
	$(KUBECTL) get node $(VNODE) -o wide
	$(KUBECTL) -n guest-kubelet-proto get pods -o wide
	$(KUBECTL) -n guest-kubelet-proto get lease

undeploy: ## namespace first (guests, then mirrors via ownerRef), then cluster-scoped objects
	-$(KUBECTL) delete ns guest-kubelet-proto --wait=true
	-for f in deploy/opt-d/*.yaml; do $(SUBST) $$f | $(KUBECTL) delete --ignore-not-found -f -; done
	-$(KUBECTL) delete node $(VNODE)
	-$(KUBECTL) -n kube-node-lease delete lease $(VNODE)
	-$(KUBECTL) delete clusterrole,clusterrolebinding guest-kubelet-proto
	-$(KUBECTL) -n kube-node-lease delete role,rolebinding guest-kubelet-proto
	-$(KUBECTL) delete clusterrole q5-epp-auth-reviewer
	-$(KUBECTL) delete clusterrolebinding q5-epp-auth-reviewer-binding
