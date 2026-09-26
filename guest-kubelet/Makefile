# Nothing here runs Go or Docker locally; builds and tests run in Cloud Build.
PROJECT ?= YOUR_PROJECT
REGION  ?= YOUR_REGION
TAG     ?= $(shell git rev-parse --short HEAD)
IMAGE   := $(REGION)-docker.pkg.dev/$(PROJECT)/guest-kubelet/guest-kubelet:$(TAG)
CONTEXT ?= YOUR_KUBE_CONTEXT
KUBECTL := kubectl --context $(CONTEXT)

.PHONY: build deploy test-guest status undeploy

build: ## vet, test, build and push the image remotely
	gcloud builds submit --project $(PROJECT) --config cloudbuild.yaml --substitutions=_TAG=$(TAG) .

deploy: ## namespace, RBAC, Deployment (image tag = TAG)
	$(KUBECTL) apply -f deploy/namespace.yaml -f deploy/rbac.yaml
	sed 's|IMAGE|$(IMAGE)|' deploy/deployment.yaml | $(KUBECTL) apply -f -

test-guest: ## a guest pod and a Service selecting it
	$(KUBECTL) apply -f deploy/test-guest.yaml

status:
	$(KUBECTL) get node vk-abcd -o wide
	$(KUBECTL) -n kube-node-lease get lease vk-abcd -o jsonpath='{.spec.renewTime}{"\n"}'
	$(KUBECTL) -n guest-kubelet-proto get pods -o wide
	$(KUBECTL) -n guest-kubelet-proto get endpointslices

undeploy: ## teardown in the order from the prototype plan (section 8)
	-$(KUBECTL) delete -f deploy/test-guest.yaml --wait=true
	-$(KUBECTL) delete ns guest-kubelet-proto --wait=true
	-$(KUBECTL) delete node vk-abcd
	-$(KUBECTL) -n kube-node-lease delete lease vk-abcd
	-$(KUBECTL) delete -f deploy/rbac.yaml
