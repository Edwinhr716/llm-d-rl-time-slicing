# Testing Flow for LoRA Swapping using GPU-CR

This flow describes how to build, deploy, and run the LoRA swapping test using GPU-CR selective checkpoint/restore.

## Prerequisites
Ensure the `snapshot-agent` is running on the cluster (see [Testing-flow.md](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts/Testing-flow.md) for installation instructions).

## 1. Build and Push the Test Image
From the `/usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts` directory:

Build the docker image:
```bash
docker build -t asia-southeast1-docker.pkg.dev/edwinhernandez-gke-dev/snapshot-example/lora-swap-client:latest -f Dockerfile.lora .
```

Push to Artifact Registry:
```bash
docker push asia-southeast1-docker.pkg.dev/edwinhernandez-gke-dev/snapshot-example/lora-swap-client:latest
```

## 2. Deploy to GKE
Update the image tag in [deployment-lora-swap.yaml](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts/deployment-lora-swap.yaml) if you used a different one.

Deploy:
```bash
kubectl apply -f deployment-lora-swap.yaml
```

## 3. Run and Monitor
The script runs automatically on startup. You can monitor the logs of the deployed pod:

```bash
kubectl logs -f deployment/lora-swap-deployment
```

The script will:
1.  Initialize vLLM with `facebook/opt-125m`.
2.  Dynamically generate two dummy LoRA adapters (A and B) with different weights.
3.  Load LoRA A (Slot 0) and run a query, recording output A.
4.  Load LoRA B (Slot 1) and run a query, recording output B (verifying it is different from A).
5.  Snapshot both LoRA A and B (this evicts them from GPU memory).
6.  Restore LoRA A and verify the query output matches the initial run of A.
7.  Restore LoRA B and verify the query output matches the initial run of B.

Check the logs for `SUCCESS` messages at the end.
