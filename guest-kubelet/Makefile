# Nothing here runs Go or Docker locally; builds and tests run in Cloud Build, from the
# repository root (the module imports the root module's gRPC APIs). Deploy with the Helm chart in
# ../deploy (guest-kubelet and gpu-shadow subcharts).
PROJECT ?= YOUR_PROJECT
REGION  ?= YOUR_REGION
TAG     ?= $(shell git rev-parse --short HEAD)
IMAGE   := $(REGION)-docker.pkg.dev/$(PROJECT)/guest-kubelet/guest-kubelet:$(TAG)
PLUGIN_IMAGE ?= $(subst /guest-kubelet:,/gpu-shadow-plugin:,$(IMAGE))

.PHONY: build build-gpu-shadow

build: ## vet, test, build and push the image remotely
	cd .. && gcloud builds submit --project=$(PROJECT) --config guest-kubelet/cloudbuild.yaml \
		--substitutions=_REGION=$(REGION),_TAG=$(TAG) .

build-gpu-shadow: ## the GPU shadow device plugin image, for --gpu-mode=pooled
	cd .. && gcloud builds submit --project=$(PROJECT) --config guest-kubelet/cloudbuild-gpu-shadow.yaml \
		--substitutions=_REGISTRY=$(patsubst %/,%,$(dir $(PLUGIN_IMAGE))),_TAG=$(TAG) .
