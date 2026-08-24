"""Tests for the BackendConfig builder helpers."""

import unittest

from timeslice.snapshot_agent import cuda_config, direct_memory_config


class TestBackendConfigBuilders(unittest.TestCase):
    def test_builds_config_with_pids(self):
        for builder, oneof in [
            (cuda_config, "cuda"),
            (direct_memory_config, "direct_memory"),
        ]:
            with self.subTest(backend=oneof):
                cfg = builder([101, 102])
                self.assertEqual(cfg.WhichOneof("backend"), oneof)
                target = getattr(cfg, oneof).explicit_target
                self.assertEqual(list(target.pids), [101, 102])

    def test_rejects_invalid_pids(self):
        for builder in (cuda_config, direct_memory_config):
            for pids in ([], [0], [-5], [1.5], ["123"], [True]):
                with self.subTest(builder=builder.__name__, pids=pids):
                    with self.assertRaises((ValueError, TypeError)):
                        builder(pids)


if __name__ == "__main__":
    unittest.main()
