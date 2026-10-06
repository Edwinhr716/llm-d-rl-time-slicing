# RL integration init image: the timeslice Python packages that the admission webhook
# (--rl-integration-image) copies into RL pods, so `async_training.trainer_name=timeslice`
# resolves in an unmodified verl image.
#
#   /opt/timeslice-rl/python/timeslice/                  timeslice client (pkg/client/python)
#   /opt/timeslice-rl/python/timeslice_verl/             verl plugin (pkg/integrations/verl)
#   /opt/timeslice-rl/python/*.dist-info/                metadata, including the verl.plugins
#                                                        entry point that registers the trainer
#
# Pure Python, installed without dependencies and without bytecode: the RL image provides
# Python >= 3.10, grpcio, protobuf >= 6 and a verl with the fully-async lifecycle hooks.
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

# --- Runtime stage: busybox for the cp the webhook runs ---
FROM busybox:1.37.0

COPY --from=builder /out/python /opt/timeslice-rl/python

USER 65532:65532

ENTRYPOINT ["cp", "-R", "/opt/timeslice-rl/python", "/timeslice-rl/"]
