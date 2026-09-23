# Node supervisor image: the time-slicing supervisor lifted out of the tenant
# pod and into a per-node DaemonSet (pkg/node-supervisor/node_supervisor.py).
#
# No CUDA, no GPU, no vLLM. Everything this process does to the engine is HTTP
# to the tenant's pod IP, so the vllm/vllm-openai base the in-pod supervisor
# inherited is pure dead weight here — a slim Python is the whole runtime. That
# is also the point of the DaemonSet form: the tenant gets to run the STOCK
# vLLM image because the only thing that needed a rebuild now lives in an image
# the platform owns.
#
# Build context is the repository root, same as the other images in docker/:
#   docker buildx build -f docker/node-supervisor/Dockerfile .
FROM python:3.11-slim

WORKDIR /workspace

# The timeslice orchestrator/agent client, installed from THIS tree rather than
# from a VCS requirement: the client is a first-party package in this repo, so
# the image must be built from the same commit as the supervisor it ships. This
# is also why no `git` is installed — there is no remote requirement to resolve.
COPY pkg/client/python /workspace/client-python

# grpcio and protobuf are deliberately NOT pinned here. They are declared in
# pkg/client/python/pyproject.toml (grpcio>=1.81.0, protobuf>=6.33.5), which is
# the single source of truth — 6.33.5 is exactly the gencode version baked into
# the generated *_pb2.py stubs by ValidateProtobufRuntimeVersion. Restating a
# different floor here is how the image silently drifts away from the stubs it
# ships when the client is regenerated.
#
# Only the two dependencies the supervisor adds on top of the client are named:
#   kubernetes — pod list (field-selected to this node) and the
#     patch_namespaced_pod_status call that drives the readiness gate. >=24.2
#     for the `_content_type` kwarg, without which a dict body goes out as a
#     json-patch and the API server rejects it.
#   redis      — the single SET of llm-d-async's dispatch budget key on the
#     rising edge. Write-only, one key, one value ("1", never "0").
RUN pip install --no-cache-dir \
      /workspace/client-python \
      "kubernetes>=24.2.0" "redis>=5.0.0" \
    && rm -rf /workspace/client-python

COPY pkg/node-supervisor/node_supervisor.py /opt/supervisor/node_supervisor.py

# Unbuffered on top of the explicit flush=True in the logger: the log lines are
# a parsed interface (see the module docstring), and a partially buffered line
# at a state transition is a missing data point.
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python3", "/opt/supervisor/node_supervisor.py"]
