import unittest
from concurrent import futures

import grpc
from google.rpc import error_details_pb2, status_pb2

from timeslice.snapshot_agent import snapshot_agent_pb2 as pb2
from timeslice.snapshot_agent.refusal import DOMAIN, reason_of

_CODE = grpc.StatusCode.FAILED_PRECONDITION


def _status(message, *details):
    status = status_pb2.Status(code=_CODE.value[0], message=message)
    for detail in details:
        status.details.add().Pack(detail)
    return status


def _info(reason, domain=DOMAIN):
    return error_details_pb2.ErrorInfo(reason=reason, domain=domain)


class _Refuser:
    """In-process server whose one method aborts with the Status it is sent."""

    def __init__(self):
        def refuse(request, context):
            status = status_pb2.Status()
            status.ParseFromString(request)
            if status.details:
                context.set_trailing_metadata((("grpc-status-details-bin", request),))
            context.abort(_CODE, status.message)

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
            request_serializer=lambda s: s.SerializeToString(),
            response_deserializer=bytes,
        )

    def refused(self, status):
        try:
            self.call(status, timeout=10)
        except grpc.RpcError as err:
            return err
        raise AssertionError("expected the call to be refused")

    def close(self):
        self.channel.close()
        self.server.stop(None)


class TestRefusalWireErrorInfo(unittest.TestCase):
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
                err = self.refuser.refused(_status("a: b", _info(name)))
                self.assertEqual(err.code(), _CODE)
                self.assertEqual(err.details(), "a: b")
                self.assertEqual(reason_of(err), name)

    def test_after_another_detail(self):
        err = self.refuser.refused(
            _status("x", error_details_pb2.RetryInfo(), _info("DEADLINE_INFEASIBLE"))
        )
        self.assertEqual(reason_of(err), "DEADLINE_INFEASIBLE")

    def test_message_names_another_reason(self):
        err = self.refuser.refused(_status("STALE_EPOCH: x", _info("BACKEND_ERROR")))
        self.assertEqual(reason_of(err), "BACKEND_ERROR")

    def test_unicode_message(self):
        err = self.refuser.refused(_status("café\nline two", _info("VERIFY_FAILED")))
        self.assertEqual(reason_of(err), "VERIFY_FAILED")

    def test_no_reason(self):
        cases = {
            "no detail": _status("job_id is required"),
            "no detail, message starts with a name": _status("STALE_EPOCH: x"),
            "other domain": _status("x", _info("STALE_EPOCH", domain="example.com")),
            "other detail only": _status("x", error_details_pb2.RetryInfo()),
        }
        for name, status in cases.items():
            with self.subTest(case=name):
                self.assertEqual(reason_of(self.refuser.refused(status)), "")

    def test_not_a_call(self):
        self.assertEqual(reason_of(grpc.RpcError()), "")


if __name__ == "__main__":
    unittest.main()
