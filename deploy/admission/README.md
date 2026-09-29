# Time-slicing admission: D-NS-15 option keep

This tree is the keep variant of decision D-NS-15 on the north-star
stack. Admission is the CEL ValidatingAdmissionPolicy in
`timeslice-vap.yaml` and nothing else:

- Install `timeslice-vap.yaml`. On this stack the guest marker is the
  label `timeslice.io/guest=true`, so replace `__GUEST_MARKER__` with
  `label`, and `__VK_USERNAME__` with the guest-kubelet service account
  (`system:serviceaccount:<namespace>:guest-kubelet`).
- Do not install `deploy/timeslice-webhook/`. The webhook code stays in
  the tree, but under keep nothing mutates pods: users write every
  time-slicing field by hand.

What users write by hand under keep:

- Guests: the `timeslice.io/guest=true` label, the guest toleration and
  the steering to the virtual Node.
- Donors: `timeslice.io/donor=true` plus explicit `timeslice.io/job-id`
  and `timeslice.io/group` labels (rule R4). CEL cannot derive them
  from the KubeRay labels, so a RayJob donor needs them in its pod
  template, and the client wiring the webhook would inject.

The policy and the webhook's own policy have different names
(`timeslice-pod-rules` and `timeslice-webhook-pods`), so a cluster that
still has the webhook installed does not clash, but it then runs both
rule sets. Remove the webhook configuration and its policy when
switching to keep.
