# RL integration init image: the timeslice Python packages that the admission webhook
# (--rl-integration-image) copies into RL pods, so `async_training.trainer_name=timeslice`
# resolves in an unmodified verl image.
#
#   /opt/timeslice-rl/python/timeslice/                  timeslice client (pkg/client/python)
#   /opt/timeslice-rl/python/timeslice_verl/             verl plugin (pkg/integrations/verl)
#   /opt/timeslice-rl/python/*.dist-info/                metadata, including the verl.plugins
#                                                        entry point that registers the trainer
#   /opt/timeslice-rl/verl/verl/                         verl with the fully-async lifecycle
#                                                        hooks (source only, pinned commit)
#   /opt/timeslice-rl/verl/LICENSE, NOTICE               verl's Apache-2.0 license and notice
#
# verl: upstream verl does not yet have the fully-async lifecycle hooks and trainer registry the
# plugin needs. The image carries the verl package source of a fork that adds them, pinned in
# docker/verl/verl-pin.env (upstream 983cb0f2 plus two commits; docker/verl/Dockerfile, the verl
# image with the integration built in, uses the same pin). The webhook puts it on PYTHONPATH after the
# timeslice packages and before the RL image's own site-packages (--rl-integration-verl), so it
# replaces the image's verl in those pods; verl's dependencies still come from the RL image.
# This goes away once the hooks are upstream.
#
# Pure Python, installed without dependencies and without bytecode: the RL image provides
# Python >= 3.10, grpcio, protobuf >= 6 and verl's dependencies.
#
# Build context is the repository root, same as the other images in docker/:
#   docker buildx build -f docker/rl-integration/Dockerfile .

# --- Build stage ---
FROM python:3.12-slim AS builder

WORKDIR /workspace
COPY pkg/client/python client
COPY pkg/integrations/verl verl-integration
RUN pip install --no-cache-dir --no-deps --no-compile --target /out/python ./client ./verl-integration \
 && rm -rf /out/python/bin \
 && chmod -R a+rX /out/python

# --- verl stage: the pinned verl source ---
FROM alpine/git:2.47.2 AS verl
# The pin lives in docker/verl/verl-pin.env (shared with docker/verl/Dockerfile);
# these build args override it for one build.
ARG VERL_REPO
ARG VERL_COMMIT
COPY docker/verl/verl-pin.env /verl-pin.env
RUN set -eu; repo="${VERL_REPO:-}"; commit="${VERL_COMMIT:-}"; . /verl-pin.env; \
    VERL_REPO="${repo:-$VERL_REPO}"; VERL_COMMIT="${commit:-$VERL_COMMIT}"; \
    git init /src && cd /src \
 && git fetch --depth 1 "$VERL_REPO" "$VERL_COMMIT" && git checkout --detach FETCH_HEAD \
 && test "$(git rev-parse HEAD)" = "$VERL_COMMIT" \
 && mkdir -p /out/verl && cp -R verl /out/verl/verl && cp LICENSE /out/verl/LICENSE \
 && if [ -f NOTICE ]; then cp NOTICE /out/verl/NOTICE; fi \
 && echo "$VERL_REPO@$VERL_COMMIT" > /out/verl/SOURCE \
 && find /out/verl -name __pycache__ -prune -exec rm -rf {} + \
 && chmod -R a+rX /out/verl

# --- Runtime stage: busybox for the cp the webhook runs ---
FROM busybox:1.37.0

COPY --from=builder /out/python /opt/timeslice-rl/python
COPY --from=verl /out/verl /opt/timeslice-rl/verl

USER 65532:65532

ENTRYPOINT ["cp", "-R", "/opt/timeslice-rl/python", "/timeslice-rl/"]
