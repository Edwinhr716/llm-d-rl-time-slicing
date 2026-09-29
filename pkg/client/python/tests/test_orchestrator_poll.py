import unittest
from unittest.mock import MagicMock, patch

import grpc

from timeslice import TimeSliceOrchestratorClient
from timeslice.orchestrator.client import (
    ACQUIRE_MODE_METADATA,
    DEFAULT_ACQUIRE_POLL_INTERVAL_SEC,
)
from timeslice.orchestrator.exceptions import (
    OrchestratorConnectionError,
    OrchestratorTimeoutError,
)

from timeslice.orchestrator._generated import pb2


class FakeClock:
    """Stands in for the time module: sleep advances monotonic."""

    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, sec):
        self.sleeps.append(sec)
        self.now += sec


def pending(waited_ms=0):
    return pb2.AcquireResponse(success=False, waited_ms=waited_ms)


def granted(waited_ms=0):
    return pb2.AcquireResponse(success=True, waited_ms=waited_ms, context_restored=True)


class TestAcquirePoll(unittest.TestCase):
    """Foreground wait option async-poll: acquire() polls until granted."""

    def setUp(self):
        self.channel_patcher = patch(
            "timeslice.orchestrator.client.grpc.insecure_channel"
        )
        self.stub_patcher = patch(
            "timeslice.orchestrator.client.pb2_grpc.TimeSliceOrchestratorServiceStub"
        )
        self.clock = FakeClock()
        self.time_patcher = patch("timeslice.orchestrator.client.time", self.clock)
        self.channel_patcher.start()
        stub_class = self.stub_patcher.start()
        self.time_patcher.start()
        self.stub = MagicMock()
        stub_class.return_value = self.stub
        self.client = TimeSliceOrchestratorClient(
            "localhost:50051", "test-job", "test-group"
        )

    def tearDown(self):
        self.client.close()
        self.time_patcher.stop()
        self.stub_patcher.stop()
        self.channel_patcher.stop()

    def test_sends_poll_metadata(self):
        self.stub.Acquire.return_value = granted()
        self.client.acquire()
        _, kwargs = self.stub.Acquire.call_args
        self.assertEqual(kwargs["metadata"], ACQUIRE_MODE_METADATA)
        self.assertEqual(ACQUIRE_MODE_METADATA, (("x-timeslice-acquire-mode", "poll"),))

    def test_polls_until_granted(self):
        self.stub.Acquire.side_effect = [pending(0), pending(1000), granted(2000)]
        result = self.client.acquire()
        self.assertTrue(result.success)
        self.assertEqual(result.waited_ms, 2000)
        self.assertTrue(result.context_restored)
        self.assertEqual(self.stub.Acquire.call_count, 3)
        self.assertEqual(
            self.clock.sleeps,
            [DEFAULT_ACQUIRE_POLL_INTERVAL_SEC, DEFAULT_ACQUIRE_POLL_INTERVAL_SEC],
        )
        for call in self.stub.Acquire.call_args_list:
            request = call.args[0]
            self.assertEqual(request.job_id, "test-job")
            self.assertEqual(request.group_id, "test-group")
            self.assertIsNone(call.kwargs["timeout"])

    def test_granted_first_poll_does_not_sleep(self):
        # Also what a blocking server returns: one RPC, no polling.
        self.stub.Acquire.return_value = granted(150)
        result = self.client.acquire(timeout_sec=10.0)
        self.assertEqual(result.waited_ms, 150)
        self.stub.Acquire.assert_called_once()
        self.assertEqual(self.stub.Acquire.call_args.kwargs["timeout"], 10.0)
        self.assertEqual(self.clock.sleeps, [])

    def test_custom_poll_interval(self):
        self.stub.Acquire.side_effect = [pending(), granted()]
        self.client.acquire(poll_interval_sec=0.25)
        self.assertEqual(self.clock.sleeps, [0.25])

    def test_timeout_spans_polls(self):
        self.stub.Acquire.return_value = pending(500)
        with self.assertRaises(OrchestratorTimeoutError):
            self.client.acquire(timeout_sec=2.5)
        timeouts = [c.kwargs["timeout"] for c in self.stub.Acquire.call_args_list]
        # 2.5 s, then what is left after each sleep; the last poll at the
        # deadline gets 0 s (a real server answers DEADLINE_EXCEEDED).
        self.assertEqual(timeouts, [2.5, 1.5, 0.5, 0.0])
        self.assertEqual(self.clock.sleeps, [1.0, 1.0, 0.5])
        self.assertLessEqual(sum(self.clock.sleeps), 2.5)

    def test_error_during_poll_is_wrapped(self):
        error = grpc.RpcError()
        error.code = MagicMock(return_value=grpc.StatusCode.UNAVAILABLE)
        error.details = MagicMock(return_value="group is faulted")
        self.stub.Acquire.side_effect = [pending(), error]
        with self.assertRaises(OrchestratorConnectionError):
            self.client.acquire()
        self.assertEqual(self.stub.Acquire.call_count, 2)

    def test_non_positive_poll_interval_raises(self):
        for interval in (0, -1):
            with self.assertRaises(ValueError):
                self.client.acquire(poll_interval_sec=interval)
        self.stub.Acquire.assert_not_called()


if __name__ == "__main__":
    unittest.main()
