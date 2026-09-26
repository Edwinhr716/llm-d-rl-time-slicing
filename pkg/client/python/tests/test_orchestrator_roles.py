import datetime
import unittest
from unittest.mock import MagicMock, patch

from google.protobuf import duration_pb2

from timeslice import TimeSliceOrchestratorClient
from timeslice.orchestrator.types import AgentJobState, GroupLockState

from timeslice.orchestrator._generated import pb2


class TestOrchestratorClientRoles(unittest.TestCase):
    """Covers the expected_idle hint and the background protocol fields."""

    def setUp(self):
        self.mock_channel_patcher = patch(
            "timeslice.orchestrator.client.grpc.insecure_channel"
        )
        self.mock_stub_patcher = patch(
            "timeslice.orchestrator.client.pb2_grpc.TimeSliceOrchestratorServiceStub"
        )
        self.mock_channel_patcher.start()
        mock_stub_class = self.mock_stub_patcher.start()
        self.mock_stub = MagicMock()
        mock_stub_class.return_value = self.mock_stub
        self.mock_stub.Yield.return_value = pb2.YieldResponse(success=True)
        self.client = TimeSliceOrchestratorClient(
            "localhost:50051", "test-job", "test-group"
        )

    def tearDown(self):
        self.client.close()
        self.mock_channel_patcher.stop()
        self.mock_stub_patcher.stop()

    def _yield_request(self):
        args, _ = self.mock_stub.Yield.call_args
        return args[0]

    def test_release_without_expected_idle_leaves_field_unset(self):
        self.client.release()
        request = self._yield_request()
        self.assertFalse(request.HasField("expected_idle"))
        self.assertEqual(request.role, pb2.ROLE_UNSPECIFIED)

    def test_yield_expected_idle_seconds(self):
        result = self.client.yield_(expected_idle=45)
        self.assertTrue(result.success)
        request = self._yield_request()
        self.assertTrue(request.HasField("expected_idle"))
        self.assertEqual(request.expected_idle, duration_pb2.Duration(seconds=45))
        self.assertEqual(request.job_id, "test-job")
        self.assertEqual(request.group_id, "test-group")

    def test_yield_expected_idle_fractional_seconds(self):
        self.client.yield_(expected_idle=1.5)
        self.assertEqual(
            self._yield_request().expected_idle,
            duration_pb2.Duration(seconds=1, nanos=500_000_000),
        )

    def test_release_expected_idle_timedelta(self):
        self.client.release(expected_idle=datetime.timedelta(minutes=2))
        self.assertEqual(
            self._yield_request().expected_idle, duration_pb2.Duration(seconds=120)
        )

    def test_yield_expected_idle_zero_is_sent(self):
        self.client.yield_(expected_idle=0)
        self.assertTrue(self._yield_request().HasField("expected_idle"))

    def test_yield_negative_expected_idle_raises(self):
        with self.assertRaises(ValueError):
            self.client.yield_(expected_idle=-1)
        with self.assertRaises(ValueError):
            self.client.release(expected_idle=datetime.timedelta(seconds=-1))
        self.mock_stub.Yield.assert_not_called()

    def test_yield_passes_overrides_and_timeout(self):
        self.client.yield_(job_id="other-job", group_id="other-group", timeout_sec=3.0)
        args, kwargs = self.mock_stub.Yield.call_args
        self.assertEqual(args[0].job_id, "other-job")
        self.assertEqual(args[0].group_id, "other-group")
        self.assertEqual(kwargs.get("timeout"), 3.0)

    def test_acquire_maps_vram_unconfirmed(self):
        self.mock_stub.Acquire.return_value = pb2.AcquireResponse(
            success=True, vram_unconfirmed=True
        )
        self.assertTrue(self.client.acquire().vram_unconfirmed)

    def test_acquire_vram_unconfirmed_defaults_false(self):
        self.mock_stub.Acquire.return_value = pb2.AcquireResponse(success=True)
        self.assertFalse(self.client.acquire().vram_unconfirmed)

    def test_get_status_background_fields(self):
        self.mock_stub.GetGroupStatus.return_value = pb2.GetGroupStatusResponse(
            group=pb2.GroupStatus(
                group_id="test-group",
                group_state=pb2.GroupStatus.State.STATE_VACATING,
                background_protocol=1,
                vacate_within=duration_pb2.Duration(seconds=27, nanos=250_000_000),
            ),
            agent_job_states=[
                pb2.SnapshotAgentJobState(
                    agent="node-1",
                    job_id="vk/node-1",
                    job_state=pb2.SnapshotAgentJobState.State.STATE_SUSPENDED,
                ),
            ],
        )
        status = self.client.get_status()
        self.assertEqual(status.group.group_state, GroupLockState.VACATING)
        self.assertEqual(status.group.background_protocol, 1)
        self.assertEqual(
            status.group.vacate_within,
            datetime.timedelta(seconds=27, milliseconds=250),
        )
        self.assertEqual(status.agent_job_states[0].job_state, AgentJobState.SUSPENDED)

    def test_get_status_background_state(self):
        self.mock_stub.GetGroupStatus.return_value = pb2.GetGroupStatusResponse(
            group=pb2.GroupStatus(
                group_id="test-group",
                group_state=pb2.GroupStatus.State.STATE_BACKGROUND,
            ),
        )
        status = self.client.get_status()
        self.assertEqual(status.group.group_state, GroupLockState.BACKGROUND)

    def test_get_status_from_server_without_background_protocol(self):
        # A server built before roles sends neither field.
        self.mock_stub.GetGroupStatus.return_value = pb2.GetGroupStatusResponse(
            group=pb2.GroupStatus(
                group_id="test-group",
                group_state=pb2.GroupStatus.State.STATE_IDLE_YIELDED,
            ),
        )
        status = self.client.get_status()
        self.assertEqual(status.group.background_protocol, 0)
        self.assertIsNone(status.group.vacate_within)
        self.assertEqual(status.group.group_state, GroupLockState.IDLE_YIELDED)


if __name__ == "__main__":
    unittest.main()
