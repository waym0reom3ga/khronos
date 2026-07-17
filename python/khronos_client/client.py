"""KhronosClient — high-level gRPC client for ScheduleService and WorkflowService."""

import logging
from typing import Any

from . import khronos_pb2, khronos_pb2_grpc
from .types import (
    OverlapPolicy,
    ScheduleInfo,
    ScheduleSpec,
    Timeouts,
    WorkflowAction,
    WorkflowInfo,
)

logger = logging.getLogger(__name__)


class KhronosClient:
    """High-level client for the Khronos workflow scheduler.

    Provides methods to manage schedules and workflows over gRPC.

    Args:
        host: Server hostname or IP address.
        port: Server gRPC port (default 7233).
        namespace: Logical namespace for isolation (default "default").
    """

    def __init__(self, host: str = "localhost", port: int = 7233, namespace: str = "default"):
        self._host = host
        self._port = port
        self._namespace = namespace
        self._channel: Any | None = None
        self._schedule_stub: khronos_pb2_grpc.ScheduleServiceStub | None = None
        self._workflow_stub: khronos_pb2_grpc.WorkflowServiceStub | None = None

    def connect(self) -> "KhronosClient":
        """Establish a gRPC channel to the Khronos server.

        Returns:
            Self for method chaining.
        """
        import grpc

        target = f"{self._host}:{self._port}"
        logger.info("Connecting to Khronos at %s", target)
        self._channel = grpc.insecure_channel(target)
        self._schedule_stub = khronos_pb2_grpc.ScheduleServiceStub(self._channel)
        self._workflow_stub = khronos_pb2_grpc.WorkflowServiceStub(self._channel)
        return self

    def close(self):
        """Close the gRPC channel."""
        if self._channel is not None:
            logger.info("Closing Khronos channel")
            self._channel.close()
            self._channel = None
            self._schedule_stub = None
            self._workflow_stub = None

    def __enter__(self) -> "KhronosClient":
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    # ── Schedule operations ────────────────────────────────────────

    def create_schedule(
        self,
        schedule_id: str,
        spec: ScheduleSpec,
        action: WorkflowAction,
        policy: OverlapPolicy = OverlapPolicy.SKIP,
    ) -> str:
        """Create a new scheduled workflow.

        Args:
            schedule_id: Unique identifier for this schedule.
            spec: When to trigger (cron expressions or interval).
            action: What workflow to run on each trigger.
            policy: How to handle overlapping executions.

        Returns:
            The created schedule ID.
        """
        if self._schedule_stub is None:
            raise RuntimeError("Call connect() before using the client")

        request = khronos_pb2.CreateScheduleRequest(
            schedule_id=schedule_id,
            namespace=self._namespace,
            spec=spec.to_proto(),
            action=action.to_proto(),
            policy=khronos_pb2.OverlapPolicy(policy=policy.value),
        )
        response = self._schedule_stub.CreateSchedule(request)
        logger.info("Created schedule %s", response.schedule_id)
        return response.schedule_id

    def update_schedule(
        self,
        schedule_id: str,
        spec: ScheduleSpec,
    ) -> bool:
        """Update the specification of an existing schedule.

        Args:
            schedule_id: ID of the schedule to update.
            spec: New schedule specification.

        Returns:
            True if the update succeeded.
        """
        if self._schedule_stub is None:
            raise RuntimeError("Call connect() before using the client")

        request = khronos_pb2.UpdateScheduleRequest(
            schedule_id=schedule_id,
            namespace=self._namespace,
            spec=spec.to_proto(),
        )
        response = self._schedule_stub.UpdateSchedule(request)
        logger.info("Updated schedule %s: success=%s", schedule_id, response.success)
        return response.success

    def delete_schedule(self, schedule_id: str) -> bool:
        """Delete a scheduled workflow.

        Args:
            schedule_id: ID of the schedule to delete.

        Returns:
            True if deletion succeeded.
        """
        if self._schedule_stub is None:
            raise RuntimeError("Call connect() before using the client")

        request = khronos_pb2.DeleteScheduleRequest(
            schedule_id=schedule_id,
            namespace=self._namespace,
        )
        response = self._schedule_stub.DeleteSchedule(request)
        logger.info("Deleted schedule %s: success=%s", schedule_id, response.success)
        return response.success

    def list_schedules(self) -> list[ScheduleInfo]:
        """List all schedules in the current namespace.

        Returns:
            List of ScheduleInfo objects.
        """
        if self._schedule_stub is None:
            raise RuntimeError("Call connect() before using the client")

        request = khronos_pb2.ListSchedulesRequest(namespace=self._namespace)
        response = self._schedule_stub.ListSchedules(request)
        return [self._proto_to_schedule_info(info) for info in response.schedules]

    # ── Workflow operations ────────────────────────────────────────

    def start_workflow(
        self,
        name: str,
        args: dict[str, str] | None = None,
        workflow_id: str = "",
        task_queue: str = "default",
        timeouts: Timeouts | None = None,
    ) -> str:
        """Start a new workflow execution.

        Args:
            name: Workflow definition name.
            args: Key-value arguments for the workflow.
            workflow_id: Logical ID (reusable across runs). Auto-generated if empty.
            task_queue: Task queue to dispatch tasks to.
            timeouts: Timeout configuration. Defaults to 1h execution timeout.

        Returns:
            The unique workflow run ID.
        """
        if self._workflow_stub is None:
            raise RuntimeError("Call connect() before using the client")

        args = args or {}
        timeouts = timeouts or Timeouts()

        request = khronos_pb2.StartWorkflowRequest(
            workflow_name=name,
            args=args,
            workflow_id=workflow_id,
            task_queue=task_queue,
            timeouts=khronos_pb2.Timeouts(
                execution_timeout_secs=timeouts.execution_timeout_secs,
                run_timeout_secs=timeouts.run_timeout_secs,
                task_timeout_secs=timeouts.task_timeout_secs,
            ),
            namespace=self._namespace,
        )
        response = self._workflow_stub.StartWorkflow(request)
        logger.info("Started workflow %s -> run_id=%s", name, response.workflow_run_id)
        return response.workflow_run_id

    def get_workflow(self, workflow_run_id: str) -> WorkflowInfo | None:
        """Get the current state of a workflow execution.

        Args:
            workflow_run_id: The unique run ID returned by start_workflow().

        Returns:
            WorkflowInfo if found, None otherwise.
        """
        if self._workflow_stub is None:
            raise RuntimeError("Call connect() before using the client")

        request = khronos_pb2.GetWorkflowRequest(
            workflow_run_id=workflow_run_id,
            namespace=self._namespace,
        )
        response = self._workflow_stub.GetWorkflow(request)
        return self._proto_to_workflow_info(response.info)

    def list_workflows(self) -> list[WorkflowInfo]:
        """List all workflows in the current namespace.

        Returns:
            List of WorkflowInfo objects.
        """
        if self._workflow_stub is None:
            raise RuntimeError("Call connect() before using the client")

        request = khronos_pb2.ListWorkflowsRequest(namespace=self._namespace)
        response = self._workflow_stub.ListWorkflows(request)
        return [self._proto_to_workflow_info(info) for info in response.workflows]

    def cancel_workflow(self, workflow_run_id: str) -> bool:
        """Cancel a running or pending workflow.

        Args:
            workflow_run_id: The unique run ID to cancel.

        Returns:
            True if cancellation succeeded.
        """
        if self._workflow_stub is None:
            raise RuntimeError("Call connect() before using the client")

        request = khronos_pb2.CancelWorkflowRequest(
            workflow_run_id=workflow_run_id,
            namespace=self._namespace,
        )
        response = self._workflow_stub.CancelWorkflow(request)
        logger.info("Cancelled workflow %s: success=%s", workflow_run_id, response.success)
        return response.success

    # ── Internal helpers ───────────────────────────────────────────

    @staticmethod
    def _proto_to_schedule_info(info: khronos_pb2.ScheduleInfo) -> ScheduleInfo:
        """Convert a proto ScheduleInfo to our high-level type."""
        spec = None
        if info.HasField("spec"):
            spec = ScheduleSpec(
                cron_expressions=list(info.spec.cron_expressions),
                interval_seconds=info.spec.interval_spec.seconds,
            )

        action = None
        if info.HasField("action"):
            action = WorkflowAction(
                workflow_name=info.action.workflow_name,
                args=dict(info.action.args),
                task_queue=info.action.task_queue,
                id=info.action.id,
                timeouts=Timeouts(
                    execution_timeout_secs=info.action.timeouts.execution_timeout_secs,
                    run_timeout_secs=info.action.timeouts.run_timeout_secs,
                    task_timeout_secs=info.action.timeouts.task_timeout_secs,
                ),
            )

        return ScheduleInfo(
            schedule_id=info.schedule_id,
            namespace=info.namespace,
            spec=spec,
            action=action,
            policy=OverlapPolicy(info.policy.policy) if info.policy.policy else OverlapPolicy.SKIP,
            created_at=info.created_at,
        )

    @staticmethod
    def _proto_to_workflow_info(info: khronos_pb2.WorkflowInfo) -> WorkflowInfo:
        """Convert a proto WorkflowInfo to our high-level type."""
        return WorkflowInfo(
            workflow_run_id=info.workflow_run_id,
            workflow_id=info.workflow_id,
            name=info.name,
            state=info.state,
            args=dict(info.args),
            result_json=info.result_json,
            started_at=info.started_at,
            completed_at=info.completed_at,
        )
