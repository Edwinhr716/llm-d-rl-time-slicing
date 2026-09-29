"""Tests for timeslice.wiring (D-NS-16: client wiring keep vs ns-downward)."""

import contextlib
import io
import os
import tempfile
import unittest

from timeslice import wiring

JOB = "rl-b-9qz"
GROUP = "ns-b.rl-b-9qz.trainers"
LABELS = (
    'ray.io/group="trainers"\n'
    'timeslice.io/donor="true"\n'
    f'timeslice.io/group="{GROUP}"\n'
    f'timeslice.io/job-id="{JOB}"'  # the kubelet writes no trailing newline
)


def podinfo_dir(content):
    d = tempfile.mkdtemp(prefix="wiring-test-")
    if content is not None:
        with open(os.path.join(d, "labels"), "w") as f:
            f.write(content)
    return d


def resolve_quiet(*args, **kwargs):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        w = wiring.resolve(*args, **kwargs)
    return w, out.getvalue()


class TestClientWiringSwitch(unittest.TestCase):
    def test_default_mode_is_keep(self):
        w = wiring.resolve({}, podinfo_dir=podinfo_dir(LABELS))
        self.assertEqual(w.mode, "keep")
        self.assertFalse(w.enabled)

    def test_env_selects_ns_downward(self):
        env = {"TIMESLICE_CLIENT_WIRING": "ns-downward"}
        w = wiring.resolve(env, podinfo_dir=podinfo_dir(LABELS))
        self.assertEqual((w.mode, w.job_id), ("ns-downward", JOB))

    def test_unknown_mode_fails(self):
        with self.assertRaises(wiring.WiringError):
            wiring.resolve({"TIMESLICE_CLIENT_WIRING": "downward"})


class TestClientWiringKeep(unittest.TestCase):
    def test_env_only(self):
        env = {
            "TIMESLICE_JOB_ID": "j",
            "TIMESLICE_GROUP": "g",
            "TIMESLICE_ORCH_ADDR": "o:1",
        }
        w = wiring.resolve(env, podinfo_dir=podinfo_dir(LABELS), mode="keep")
        self.assertTrue(w.enabled)
        self.assertEqual((w.job_id, w.group, w.orch_addr), ("j", "g", "o:1"))
        self.assertEqual(set(w.source.values()), {"env"})

    def test_ignores_labels_and_is_noop_without_env(self):
        w = wiring.resolve({}, podinfo_dir=podinfo_dir(LABELS), mode="keep")
        self.assertFalse(w.enabled)
        self.assertIsNone(w.job_id)
        self.assertIsNone(w.orch_addr)  # no Service DNS default in keep
        self.assertEqual(set(w.source.values()), {"none"})

    def test_missing_one_var_disables(self):
        env = {"TIMESLICE_JOB_ID": "j", "TIMESLICE_ORCH_ADDR": "o:1"}
        self.assertFalse(wiring.resolve(env, mode="keep").enabled)

    def test_never_reads_podinfo(self):
        called = []
        wiring.resolve({}, mode="keep", donor_podinfo=lambda: called.append(1))
        self.assertEqual(called, [])


class TestClientWiringNsDownward(unittest.TestCase):
    def test_labels_and_default_addr(self):
        w, out = resolve_quiet({}, podinfo_dir=podinfo_dir(LABELS), mode="ns-downward")
        self.assertTrue(w.enabled)
        self.assertEqual((w.job_id, w.group), (JOB, GROUP))
        self.assertEqual(w.orch_addr, wiring.DEFAULT_ORCH_ADDR)
        self.assertEqual(
            w.source, {"job_id": "label", "group": "label", "orch_addr": "default"}
        )
        self.assertEqual(out, "")

    def test_default_addr_is_service_dns(self):
        self.assertEqual(
            wiring.DEFAULT_ORCH_ADDR,
            "timeslice-timesliceorchestrator.timeslice-system.svc:50051",
        )

    def test_podinfo_dir_from_env(self):
        env = {"TIMESLICE_PODINFO_DIR": podinfo_dir(LABELS)}
        w = wiring.resolve(env, mode="ns-downward")
        self.assertEqual((w.job_id, w.group), (JOB, GROUP))

    def test_parser_handles_escapes(self):
        text = 'example.com/note="a \\"b\\" \\\\ c"\n' + LABELS + "\n"
        labels = wiring.parse_labels(text)
        self.assertEqual(labels["example.com/note"], 'a "b" \\ c')
        self.assertEqual(labels["timeslice.io/job-id"], JOB)

    def test_env_overrides_each_field(self):
        env = {"TIMESLICE_GROUP": "g2", "TIMESLICE_ORCH_ADDR": "alt:1"}
        w, out = resolve_quiet(env, podinfo_dir=podinfo_dir(LABELS), mode="ns-downward")
        self.assertEqual((w.job_id, w.group, w.orch_addr), (JOB, "g2", "alt:1"))
        self.assertEqual(
            w.source, {"job_id": "label", "group": "env", "orch_addr": "env"}
        )
        self.assertEqual(
            out.strip(),
            f"[timeslice] WARNING wiring conflict field=group env=g2 label={GROUP}",
        )

    def test_empty_env_does_not_override(self):
        env = {"TIMESLICE_JOB_ID": "", "TIMESLICE_GROUP": "  "}
        w, _ = resolve_quiet(env, podinfo_dir=podinfo_dir(LABELS), mode="ns-downward")
        self.assertEqual((w.job_id, w.group), (JOB, GROUP))

    def test_env_rescues_unlabelled_pod(self):
        env = {"TIMESLICE_JOB_ID": "j", "TIMESLICE_GROUP": "g"}
        w = wiring.resolve(env, podinfo_dir=podinfo_dir(None), mode="ns-downward")
        self.assertEqual(
            (w.job_id, w.group, w.orch_addr), ("j", "g", wiring.DEFAULT_ORCH_ADDR)
        )

    def test_fail_fast(self):
        cases = {
            "no file": (None, "timeslice.io/job-id,timeslice.io/group"),
            "no group": (f'timeslice.io/job-id="{JOB}"', "timeslice.io/group"),
            "empty": (
                'timeslice.io/job-id=""\ntimeslice.io/group=""',
                "timeslice.io/job-id,timeslice.io/group",
            ),
        }
        for name, (content, missing) in cases.items():
            with self.subTest(name):
                d = podinfo_dir(content)
                with self.assertRaises(wiring.WiringError) as ctx:
                    wiring.resolve(
                        {"HOSTNAME": "rl-b-samplers-x"},
                        podinfo_dir=d,
                        mode="ns-downward",
                    )
                msg = str(ctx.exception)
                self.assertTrue(
                    msg.startswith("FATAL wiring: pod=rl-b-samplers-x "), msg
                )
                self.assertIn(f"missing={missing} ", msg)
                self.assertIn(f"podinfo={os.path.join(d, 'labels')}:", msg)
                self.assertIn("timeslice.io/donor", msg)
                self.assertNotIn("\n", msg)

    def test_donor_podinfo_fallback(self):
        def donor():
            labels = wiring.parse_labels(LABELS)
            return wiring.PodInfo(
                pod="rl-b-trainers-y",
                path="/x/labels",
                labels=labels,
                via="ray:trainer_node",
            )

        env = {"HOSTNAME": "rl-b-samplers-x"}
        w = wiring.resolve(
            env, podinfo_dir=podinfo_dir(None), mode="ns-downward", donor_podinfo=donor
        )
        self.assertEqual(
            (w.job_id, w.group, w.pod, w.via),
            (JOB, GROUP, "rl-b-trainers-y", "ray:trainer_node"),
        )
        self.assertTrue(w.log_line().endswith(" via=ray:trainer_node"))

    def test_donor_podinfo_not_called_when_labelled(self):
        called = []
        wiring.resolve(
            {},
            podinfo_dir=podinfo_dir(LABELS),
            mode="ns-downward",
            donor_podinfo=lambda: called.append(1),
        )
        self.assertEqual(called, [])

    def test_donor_podinfo_none_still_fails(self):
        with self.assertRaises(wiring.WiringError):
            wiring.resolve(
                {},
                podinfo_dir=podinfo_dir(None),
                mode="ns-downward",
                donor_podinfo=lambda: None,
            )

    def test_log_line(self):
        w, _ = resolve_quiet(
            {"HOSTNAME": "p-1"}, podinfo_dir=podinfo_dir(LABELS), mode="ns-downward"
        )
        self.assertEqual(
            w.log_line(),
            f"wiring mode=ns-downward job={JOB} group={GROUP} orch={wiring.DEFAULT_ORCH_ADDR} "
            "source=job:label,group:label,orch:default pod=p-1 enabled=true",
        )
        k = wiring.resolve({"HOSTNAME": "p-1"}, mode="keep")
        self.assertEqual(
            k.log_line(),
            "wiring mode=keep job=None group=None orch=None source=job:none,group:none,orch:none pod=p-1 enabled=false",
        )


if __name__ == "__main__":
    unittest.main()
