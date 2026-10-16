import contextlib
import json
from typing import Any, List, Optional, Tuple

import grpc

from timeslice.orchestrator._generated import pb2, pb2_grpc
from timeslice.orchestrator.configs import BackendConfigLike, coerce_backend_config
from timeslice.orchestrator.exceptions import wrap_grpc_error
from timeslice.orchestrator.types import (
    AcquireResult,
    AgentJobState,
    GroupLockState,
    GroupStatus,
    OrchestratorGroupStatus,
    SnapshotAgentJobState,
    YieldResult,
)

DEFAULT_SERVICE_CONFIG = json.dumps(
    {
        "methodConfig": [
            {
                "name": [
                    {"service": "timeslice.orchestrator.TimeSliceOrchestratorService"}
                ],
                "retryPolicy": {
                    "maxAttempts": 5,
                    "initialBackoff": "0.1s",
                    "maxBackoff": "1s",
                    "backoffMultiplier": 2.0,
                    "retryableStatusCodes": [
                        "UNAVAILABLE",
                        "RESOURCE_EXHAUSTED",
                        "ABORTED",
                        "DEADLINE_EXCEEDED",
                    ],
                },
            }
        ]
    }
)

DEFAULT_CHANNEL_OPTIONS: List[Tuple[str, Any]] = [
    ("grpc.service_config", DEFAULT_SERVICE_CONFIG),
]


class TimeSliceOrchestratorClient:
    """Convenient UX wrapper client for the TimeSlice Orchestrator Service."""

    def __init__(
        self,
        target: str,
        job_id: Optional[str] = None,
        group_id: Optional[str] = None,
        channel_options: Optional[List[Tuple[str, Any]]] = None,
        backend_config: BackendConfigLike = None,
    ):
        """Initializes the Orchestrator Client.

        Args:
            target: gRPC server address (e.g., 'localhost:50051').
            job_id: Optional unique identifier of the job.
            group_id: Optional unique identifier of the time-slice group.
            channel_options: Optional gRPC channel options.
            backend_config: Optional snapshot backend for this job, sent on
                every release unless overridden per call. A
                ``JobBackendConfig`` (see ``timeslice.orchestrator.configs``)
                or one of "cuda", "tpu", "app_channel". The orchestrator uses
                it to park the job's context and to restore it on the next
                acquire. None lets the orchestrator use whatever it has
                stored for the job, or the agent's default backend.
        """
        self.target = target
        self.job_id = job_id
        self.group_id = group_id
        self.backend_config = coerce_backend_config(backend_config)

        if channel_options is None:
            channel_options = DEFAULT_CHANNEL_OPTIONS

        # Initialize insecure channel. (Can be extended to secure if needed in future)
        self._channel = grpc.insecure_channel(target, options=channel_options)
        self._stub = pb2_grpc.TimeSliceOrchestratorServiceStub(self._channel)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Closes the underlying gRPC channel."""
        if hasattr(self, "_channel") and self._channel:
            self._channel.close()
            self._channel = None

    def __del__(self):
        self.close()

    def acquire(
        self,
        job_id: Optional[str] = None,
        group_id: Optional[str] = None,
        timeout_sec: Optional[float] = None,
    ) -> AcquireResult:
        """Acquires exclusive access to the time-slice group.

        This call blocks until access is granted or timeout is reached. If
        the job's context is parked, it is restored with the backend the
        job last passed on ``release``.

        Args:
            job_id: Optional job_id to override the constructor value.
            group_id: Optional group_id to override the constructor value.
            timeout_sec: Optional timeout in seconds for the RPC call.

        Returns:
            AcquireResult containing success, waited_ms, and context_restored.

        Raises:
            ValueError: If job_id or group_id is not provided either here or in the constructor.
            OrchestratorError: If the RPC fails.
        """
        resolved_job_id = self._resolve_job_id(job_id)
        resolved_group_id = self._resolve_group_id(group_id)

        request = pb2.AcquireRequest(job_id=resolved_job_id, group_id=resolved_group_id)
        try:
            # Note: timeout in grpc is passed as 'timeout' keyword argument in seconds
            response = self._stub.Acquire(request, timeout=timeout_sec)
            return AcquireResult(
                success=response.success,
                waited_ms=response.waited_ms,
                context_restored=response.context_restored,
            )
        except grpc.RpcError as e:
            raise wrap_grpc_error(e) from e

    def release(
        self,
        job_id: Optional[str] = None,
        group_id: Optional[str] = None,
        timeout_sec: Optional[float] = None,
        backend_config: BackendConfigLike = None,
    ) -> YieldResult:
        """Releases exclusive access to the time-slice group.

        Returns immediately once recorded.

        Args:
            job_id: Optional job_id to override the constructor value.
            group_id: Optional group_id to override the constructor value.
            timeout_sec: Optional timeout in seconds for the RPC call.
            backend_config: Optional snapshot backend to override the
                constructor value for this call. If set, the orchestrator
                stores it for the job and uses it to park the job's context
                and to restore it on the next acquire.

        Returns:
            YieldResult containing success, pending_waiters, and snapshot_deferred.

        Raises:
            ValueError: If job_id or group_id is not provided either here or in the constructor.
            OrchestratorError: If the RPC fails.
        """
        resolved_job_id = self._resolve_job_id(job_id)
        resolved_group_id = self._resolve_group_id(group_id)

        request = pb2.YieldRequest(job_id=resolved_job_id, group_id=resolved_group_id)
        self._set_backend_config(request, backend_config)
        try:
            response = self._stub.Yield(request, timeout=timeout_sec)
            return YieldResult(
                success=response.success,
                pending_waiters=response.pending_waiters,
                snapshot_deferred=response.snapshot_deferred,
            )
        except grpc.RpcError as e:
            raise wrap_grpc_error(e) from e

    def get_status(
        self, group_id: Optional[str] = None, timeout_sec: Optional[float] = None
    ) -> OrchestratorGroupStatus:
        """Returns the detailed status of the time-slice group.

        Args:
            group_id: Optional group_id to override the constructor value.
            timeout_sec: Optional timeout in seconds for the RPC call.

        Returns:
            OrchestratorGroupStatus containing group status and agent states.

        Raises:
            ValueError: If group_id is not provided either here or in the constructor.
            OrchestratorError: If the RPC fails.
        """
        resolved_group_id = self._resolve_group_id(group_id)

        request = pb2.GetGroupStatusRequest(group_id=resolved_group_id)
        try:
            response = self._stub.GetGroupStatus(request, timeout=timeout_sec)

            # Map GroupStatus
            proto_group = response.group
            group_state = self._map_group_state(proto_group.group_state)

            # Convert protobuf timestamp to python datetime
            dt = (
                proto_group.state_timestamp.ToDatetime()
                if proto_group.HasField("state_timestamp")
                else None
            )

            group_status = GroupStatus(
                group_id=proto_group.group_id,
                group_state=group_state,
                state_timestamp=dt,
                locking_job=proto_group.locking_job,
                active_job=proto_group.active_job,
                waiter_queue_depth=proto_group.waiter_queue_depth,
                loaded_job=proto_group.loaded_job,
            )

            # Map Agent States
            agent_states = []
            for proto_agent_state in response.agent_job_states:
                agent_states.append(
                    SnapshotAgentJobState(
                        agent=proto_agent_state.agent,
                        job_id=proto_agent_state.job_id,
                        job_state=self._map_agent_state(proto_agent_state.job_state),
                    )
                )

            return OrchestratorGroupStatus(
                group=group_status,
                agent_job_states=agent_states,
            )
        except grpc.RpcError as e:
            raise wrap_grpc_error(e) from e

    def list_groups(self, timeout_sec: Optional[float] = None) -> List[str]:
        """Lists all active time-slice group IDs.

        Args:
            timeout_sec: Optional timeout in seconds for the RPC call.

        Returns:
            List of group ID strings.

        Raises:
            OrchestratorError: If the RPC fails.
        """
        request = pb2.ListGroupsRequest()
        try:
            response = self._stub.ListGroups(request, timeout=timeout_sec)
            return list(response.group_ids)
        except grpc.RpcError as e:
            raise wrap_grpc_error(e) from e

    @contextlib.contextmanager
    def on_accelerators(
        self,
        job_id: Optional[str] = None,
        group_id: Optional[str] = None,
        timeout_sec: Optional[float] = None,
        backend_config: BackendConfigLike = None,
    ):
        """Context manager/decorator for safe acquire/release flow on accelerators.

        ``backend_config`` is forwarded to release.

        Usage as context manager:
            with client.on_accelerators() as result:
                # exclusive access here, inspect result.waited_ms

        Usage as decorator:
            @client.on_accelerators()
            def my_gpu_function():
                # runs with exclusive access
        """
        result = self.acquire(job_id=job_id, group_id=group_id, timeout_sec=timeout_sec)
        try:
            yield result
        finally:
            self.release(
                job_id=job_id, group_id=group_id, backend_config=backend_config
            )

    def _set_backend_config(self, request, backend_config: BackendConfigLike) -> None:
        """Fills request.backend_config from the per-call value, else the
        constructor default; leaves the field unset when neither is given."""
        cfg = (
            coerce_backend_config(backend_config)
            if backend_config is not None
            else self.backend_config
        )
        if cfg is not None:
            request.backend_config.CopyFrom(cfg)

    def _map_group_state(self, proto_state: int) -> GroupLockState:
        try:
            name = pb2.GroupStatus.State.Name(proto_state)
            friendly_name = name.replace("STATE_", "")
            return GroupLockState[friendly_name]
        except (ValueError, KeyError):
            return GroupLockState.UNSPECIFIED

    def _map_agent_state(self, proto_state: int) -> AgentJobState:
        try:
            name = pb2.SnapshotAgentJobState.State.Name(proto_state)
            friendly_name = name.replace("STATE_", "")
            return AgentJobState[friendly_name]
        except (ValueError, KeyError):
            return AgentJobState.UNSPECIFIED

    def _resolve_job_id(self, job_id: Optional[str]) -> str:
        resolved = job_id or self.job_id
        if not resolved:
            raise ValueError(
                "job_id must be provided either in the constructor or in the method call."
            )
        return resolved

    def _resolve_group_id(self, group_id: Optional[str]) -> str:
        resolved = group_id or self.group_id
        if not resolved:
            raise ValueError(
                "group_id must be provided either in the constructor or in the method call."
            )
        return resolved
