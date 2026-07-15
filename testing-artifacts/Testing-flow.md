# Testing Flow for Selective Checkpoint/Restore

First make sure that the snapshot-agent isn't already running:

```
kubectl get pods -n=timeslice-system
```

If it is running, run `helm uninstall snapshot-agent` from this dir `/usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing`. 

Then, run this command from the same dir

```
helm upgrade --install snapshot-agent ./deploy/snapshot-agent --set image.repository=asia-southeast1-docker.pkg.dev/edwinhernandez-gke-dev/time-slicing/llm-d-rl-time-slicing/snapshot-agent --set image.tag=5c45dd2-dirty --set-string "nodeSelector.cloud\.google\.com/gke-gpu=true"
```

Next, build this dockerfile `/usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts/Dockerfile` by running

```
docker build --no-cache -t asia-southeast1-docker.pkg.dev/edwinhernandez-gke-dev/snapshot-example/snapshot-client:<image-tag> -f Dockerfile .
```

replacing <image-tag> with a generated image tag name such as <commit-hash>-dirty or <commit-hash>-release.

And push it to the AR

```
docker push asia-southeast1-docker.pkg.dev/edwinhernandez-gke-dev/snapshot-example/snapshot-client:<image-tag>
```

Update /usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts/deployment-cr.yaml with asia-southeast1-docker.pkg.dev/edwinhernandez-gke-dev/snapshot-example/snapshot-client:<image-tag>, and deploy it in the cluster.

To validate that the changes are successful,  line 129 of /usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts/validate_selective_cr.py should not throw and error. 