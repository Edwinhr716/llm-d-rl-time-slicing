"""Tests for the BackendConfig builder helpers."""

import unittest

from timeslice.snapshot_agent import cuda_config, direct_memory_config


class TestBackendConfigBuilders(unittest.TestCase):
    def test_cuda_config_builds_explicit_target(self):
        """Explicit PIDs land in cuda.explicit_target."""
        cfg = cuda_config([101, 102])
        self.assertEqual(cfg.WhichOneof("backend"), "cuda")
        self.assertEqual(list(cfg.cuda.explicit_target.pids), [101, 102])

    def test_direct_memory_config_builds_explicit_target(self):
        """Explicit PIDs land in direct_memory.explicit_target."""
        cfg = direct_memory_config([101, 102])
        self.assertEqual(cfg.WhichOneof("backend"), "direct_memory")
        self.assertEqual(list(cfg.direct_memory.explicit_target.pids), [101, 102])

    def test_cuda_config_omits_target_when_pids_not_given(self):
        """Omitting pids leaves cuda.explicit_target unset (k8s discovery)."""
        cfg = cuda_config()
        self.assertEqual(cfg.WhichOneof("backend"), "cuda")
        self.assertFalse(cfg.cuda.HasField("explicit_target"))

    def test_direct_memory_config_omits_target_when_pids_not_given(self):
        """Omitting pids leaves direct_memory.explicit_target unset (k8s discovery)."""
        cfg = direct_memory_config()
        self.assertEqual(cfg.WhichOneof("backend"), "direct_memory")
        self.assertFalse(cfg.direct_memory.HasField("explicit_target"))

    def test_rejects_empty_pids(self):
        """An explicitly empty PID list is an error, not discovery."""
        with self.assertRaises(ValueError):
            cuda_config([])
        with self.assertRaises(ValueError):
            direct_memory_config([])

    def test_rejects_non_positive_pids(self):
        """Zero and negative PIDs are rejected."""
        for pids in ([0], [-5]):
            with self.assertRaises(ValueError):
                cuda_config(pids)
            with self.assertRaises(ValueError):
                direct_memory_config(pids)

    def test_rejects_non_integer_pids(self):
        """Non-integer PID values are rejected."""
        for pids in ([1.5], ["123"], [True]):
            with self.assertRaises((ValueError, TypeError)):
                cuda_config(pids)
            with self.assertRaises((ValueError, TypeError)):
                direct_memory_config(pids)


if __name__ == "__main__":
    unittest.main()
