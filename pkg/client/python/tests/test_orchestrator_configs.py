import unittest

from timeslice.orchestrator import configs
from timeslice.orchestrator._generated import pb2


class TestOrchestratorConfigs(unittest.TestCase):
    def test_cuda_and_tpu_select_backend(self):
        self.assertEqual(configs.cuda_config().WhichOneof("backend"), "cuda")
        self.assertEqual(configs.tpu_config().WhichOneof("backend"), "tpu")

    def test_app_endpoint_config(self):
        cfg = configs.app_endpoint_config(
            "vllm", ["http://a:8000", "http://b:8000"], mode="discard", tags=["weights"]
        )
        self.assertEqual(cfg.WhichOneof("backend"), "app_endpoint")
        self.assertEqual(cfg.app_endpoint.app, pb2.APP_VLLM)
        self.assertEqual(
            list(cfg.app_endpoint.endpoints), ["http://a:8000", "http://b:8000"]
        )
        self.assertEqual(cfg.app_endpoint.mode, pb2.SUSPEND_MODE_DISCARD)
        self.assertEqual(list(cfg.app_endpoint.tags), ["weights"])

    def test_app_endpoint_presets(self):
        self.assertEqual(
            configs.vllm_config(["http://a"]).app_endpoint.app, pb2.APP_VLLM
        )
        self.assertEqual(
            configs.sglang_config(["http://a"]).app_endpoint.app, pb2.APP_SGLANG
        )

    def test_app_endpoint_accepts_enum_values(self):
        cfg = configs.app_endpoint_config(
            pb2.APP_SGLANG, ["http://a"], mode=pb2.SUSPEND_MODE_OFFLOAD
        )
        self.assertEqual(cfg.app_endpoint.app, pb2.APP_SGLANG)
        self.assertEqual(cfg.app_endpoint.mode, pb2.SUSPEND_MODE_OFFLOAD)

    def test_app_endpoint_validation(self):
        with self.assertRaises(ValueError):
            configs.app_endpoint_config("tgi", ["http://a"])
        with self.assertRaises(ValueError):
            configs.app_endpoint_config(pb2.APP_UNSPECIFIED, ["http://a"])
        with self.assertRaises(ValueError):
            configs.app_endpoint_config("vllm", [])
        with self.assertRaises(ValueError):
            configs.app_endpoint_config("vllm", "http://a")
        with self.assertRaises(ValueError):
            configs.app_endpoint_config("vllm", ["http://a", ""])
        with self.assertRaises(ValueError):
            configs.app_endpoint_config("vllm", ["http://a"], mode="hibernate")
        with self.assertRaises(ValueError):
            configs.app_endpoint_config("vllm", ["http://a"], tags=["weights", ""])
        with self.assertRaises(ValueError):
            configs.app_endpoint_config("vllm", ["http://a"], tags="weights")

    def test_app_channel_config(self):
        cfg = configs.app_channel_config()
        self.assertEqual(cfg.WhichOneof("backend"), "app_channel")
        self.assertEqual(cfg.app_channel.mode, pb2.SUSPEND_MODE_UNSPECIFIED)
        self.assertEqual(list(cfg.app_channel.tags), [])

        cfg = configs.app_channel_config(mode="offload", tags=["kv_cache"])
        self.assertEqual(cfg.app_channel.mode, pb2.SUSPEND_MODE_OFFLOAD)
        self.assertEqual(list(cfg.app_channel.tags), ["kv_cache"])

        with self.assertRaises(ValueError):
            configs.app_channel_config(mode="nope")

    def test_coerce_backend_config(self):
        self.assertIsNone(configs.coerce_backend_config(None))
        cfg = configs.app_channel_config()
        self.assertIs(configs.coerce_backend_config(cfg), cfg)
        self.assertEqual(configs.coerce_backend_config("CUDA"), configs.cuda_config())
        self.assertEqual(configs.coerce_backend_config("tpu"), configs.tpu_config())
        self.assertEqual(
            configs.coerce_backend_config("app_channel"), configs.app_channel_config()
        )
        with self.assertRaises(ValueError):
            configs.coerce_backend_config("app_endpoint")  # needs endpoints
        with self.assertRaises(TypeError):
            configs.coerce_backend_config(["cuda"])

    def test_enum_values_match_snapshot_agent(self):
        """The orchestrator converts these enums to the agent's by numeric cast."""
        from timeslice.snapshot_agent import snapshot_agent_pb2 as agent_pb2

        for name, value in pb2.App.items():
            self.assertEqual(agent_pb2.App.Value(name), value)
        for name, value in pb2.SuspendMode.items():
            self.assertEqual(agent_pb2.SuspendMode.Value(name), value)


if __name__ == "__main__":
    unittest.main()
