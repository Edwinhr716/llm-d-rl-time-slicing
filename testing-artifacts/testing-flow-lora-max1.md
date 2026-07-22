# Testing Flow for LoRA Swapping using GPU-CR (max_loras=1)

This flow describes how to build, deploy, and run the LoRA swapping test using GPU-CR selective checkpoint/restore when vLLM is configured with `max_loras=1` (single-slot swapping).

## Prerequisites
Ensure the `snapshot-agent` is running on the cluster (see [Testing-flow.md](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts/Testing-flow.md) for installation instructions).

## 1. Build and Push the Test Image
From the `/usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts` directory:

Build the docker image:
```bash
docker build -t asia-southeast1-docker.pkg.dev/edwinhernandez-gke-dev/snapshot-example/lora-swap-client-max1:latest -f Dockerfile.lora-max1 .
```

Push to Artifact Registry:
```bash
docker push asia-southeast1-docker.pkg.dev/edwinhernandez-gke-dev/snapshot-example/lora-swap-client-max1:latest
```

## 2. Deploy to GKE
Update the image tag in [deployment-lora-swap-max1.yaml](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts/deployment-lora-swap-max1.yaml) if you used a different one.

Deploy:
```bash
kubectl apply -f deployment-lora-swap-max1.yaml
```

## 3. Run and Monitor
The script runs automatically on startup. You can monitor the logs of the deployed pod:

```bash
kubectl logs -f deployment/lora-swap-max1-deployment
```

The script will:
1.  Initialize vLLM with `facebook/opt-125m` and `max_loras=1` (allocating exactly one LoRA slot in GPU memory).
2.  Dynamically generate two dummy LoRA adapters (A and B) with different weights (A = 1x scale, B = 10x scale).
3.  Load LoRA A into Slot 0 and run a query, recording output A.
4.  Snapshot LoRA A (saving Slot 0 to Group A).
5.  Load LoRA B, which evicts A and overwrites Slot 0. Run a query, recording output B.
6.  Snapshot LoRA B (saving Slot 0 to Group B).
7.  Restore LoRA A (overwriting Slot 0 with A's weights).
8.  **Verify via Hijacking**: Query the model requesting LoRA B. Since vLLM thinks B is active, it will not reload. However, because we restored A's weights, the output should match output A.
9.  Restore LoRA B (overwriting Slot 0 with B's weights).
10. Query the model requesting LoRA B. The output should match output B.

Check the logs for `SUCCESS` messages at the end.
