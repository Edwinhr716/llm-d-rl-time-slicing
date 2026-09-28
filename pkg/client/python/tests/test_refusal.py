import unittest
from concurrent import futures

import grpc

from timeslice.snapshot_agent import snapshot_agent_pb2 as pb2
from timeslice.snapshot_agent.refusal import reason_of

_CODE = grpc.StatusCode.FAILED_PRECONDITION


class _Refuser:
    """In-process server whose one method aborts with the message it is sent."""

    def __init__(self):
        def refuse(request, context):
            context.abort(_CODE, request.decode("utf-8"))

        handler = grpc.method_handlers_generic_handler(
            "test.Refusal",
            {
                "Refuse": grpc.unary_unary_rpc_method_handler(
                    refuse, request_deserializer=bytes, response_serializer=bytes
                ),
            },
        )
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        self.server.add_generic_rpc_handlers((handler,))
        port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self.call = self.channel.unary_unary(
            "/test.Refusal/Refuse",
            request_serializer=lambda s: s.encode("utf-8"),
            response_deserializer=bytes,
        )

    def refused(self, message):
        try:
            self.call(message, timeout=10)
        except grpc.RpcError as err:
            return err
        raise AssertionError("expected the call to be refused")

    def close(self):
        self.channel.close()
        self.server.stop(None)


class TestRefusalWirePrefix(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.refuser = _Refuser()

    @classmethod
    def tearDownClass(cls):
        cls.refuser.close()

    def test_every_reason(self):
        for value in pb2.ErrorReason.values():
            name = pb2.ErrorReason.Name(value)
            if value == pb2.ERROR_REASON_UNSPECIFIED:
                continue
            with self.subTest(reason=name):
                err = self.refuser.refused(f"{name}: a: b")
                self.assertEqual(err.code(), _CODE)
                self.assertEqual(reason_of(err), name)

    def test_no_reason(self):
        cases = [
            "job_id is required",
            "",
            "stale_epoch: lower case",
            "STALE_EPOCH",
            "SOMETHING_ELSE: x",
            "call refused: STALE_EPOCH: x",
        ]
        for message in cases:
            with self.subTest(message=message):
                self.assertEqual(reason_of(self.refuser.refused(message)), "")

    def test_unicode_message(self):
        err = self.refuser.refused("VERIFY_FAILED: café\nline two")
        self.assertEqual(reason_of(err), "VERIFY_FAILED")

    def test_not_a_call(self):
        self.assertEqual(reason_of(grpc.RpcError()), "")


if __name__ == "__main__":
    unittest.main()
