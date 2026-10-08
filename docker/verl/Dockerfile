# verl image with the timeslice integration built in: a stock verl image plus
#   - the verl fork pinned in docker/verl/verl-pin.env (upstream verl plus the
#     fully-async lifecycle hooks and trainer registry), replacing nothing: the
#     base image has no verl package of its own;
#   - the timeslice client (pkg/client/python) and verl plugin
#     (pkg/integrations/verl) from the same commit as the platform images;
#   - TransferQueue and cupy-cuda12x, which verl's fully-async mode imports, so
#     RL pods need no PyPI access at start.
#
# With this image the RayJob needs no pip install of verl or the plugin, and the
# platform's webhook does not need to inject them: install the chart with
# deploy/values-baked-verl.yaml (timeslice-webhook.rlIntegration.enabled=false).
# The plugin is inert unless TIMESLICE_FULLY_ASYNC=1 (set by the webhook), so
# the image also runs plain verl jobs.
#
# The same commit the rl-integration image injects (one pin, verl-pin.env), so
# a stock image with injection and this image run the same verl.
#
# Build context is the repository root, same as the other images in docker/:
#   docker buildx build -f docker/verl/Dockerfile .
# The result is about 24 GB unpacked (11 GB compressed), almost all of it the
# base image.
ARG VERL_BASE=verlai/verl:vllm020.dev2
FROM ${VERL_BASE}

# Optional overrides of docker/verl/verl-pin.env for one build.
ARG VERL_REPO
ARG VERL_COMMIT

# Same versions the quickstart RayJob installs at start today (pinned here).
ARG TRANSFERQUEUE_VERSION=0.1.11
ARG CUPY_VERSION=14.2.0

COPY docker/verl/verl-pin.env /tmp/ts/verl-pin.env
RUN set -eu; repo="${VERL_REPO:-}"; commit="${VERL_COMMIT:-}"; . /tmp/ts/verl-pin.env; \
    repo="${repo:-$VERL_REPO}"; commit="${commit:-$VERL_COMMIT}"; \
    pip install --no-cache-dir "TransferQueue==${TRANSFERQUEUE_VERSION}" "cupy-cuda12x==${CUPY_VERSION}"; \
    pip install --no-cache-dir --no-deps "verl @ git+${repo}@${commit}"; \
    mkdir -p /usr/local/share/timeslice && echo "${repo}@${commit}" > /usr/local/share/timeslice/verl-SOURCE

# The timeslice client and verl plugin, without dependencies (the base image
# has grpcio and protobuf >= 6).
COPY pkg/client/python /tmp/ts/client
COPY pkg/integrations/verl /tmp/ts/verl-integration
COPY docker/verl/verl_check.py /tmp/ts/verl_check.py
RUN pip install --no-cache-dir --no-deps /tmp/ts/client /tmp/ts/verl-integration \
 && python3 /tmp/ts/verl_check.py \
 && rm -rf /tmp/ts

LABEL org.opencontainers.image.title="verl with the timeslice integration" \
      org.opencontainers.image.description="verlai/verl plus the pinned verl fork with the fully-async lifecycle hooks and the timeslice client and verl plugin" \
      org.opencontainers.image.licenses="Apache-2.0"
