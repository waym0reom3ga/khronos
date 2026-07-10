"""High-level type definitions for Khronos client."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class OverlapPolicy(Enum):
    """Overlap policy for scheduled workflows."""
    SKIP = "skip"
    BUFFER = "buffer"
    TERMINATE = "terminate"


@dataclass(frozen=True)
class Timeouts:
    """Timeout configuration for workflow execution.

    Args:
        execution_timeout_secs: Total time allowed for the entire workflow run,
            including retries and queuing (0 means no limit).
        run_timeout_secs: Max duration of a single workflow attempt (0 means no limit).
        task_timeout_secs: Max duration of a single task processing (0 means no limit).
    """
    execution_timeout_secs: int = 3600
    run_timeout_secs: int = 0
    task_timeout_secs: int = 0


@dataclass(frozen=True)
class RetryPolicy:
    """Retry configuration for activities.

    Args:
        maximum_attempts: Maximum number of retry attempts (1 means no retries).
        initial_interval_secs: Initial backoff interval between retries in seconds.
            Subsequent intervals grow exponentially.
    """
    maximum_attempts: int = 3
    initial_interval_secs: float = 1.0


@dataclass
class ScheduleSpec:
    """Schedule specification — either cron expressions or an interval.

    Args:
        cron_expressions: List of cron expressions (e.g., ["0 */6 * * *"]).
            Mutually exclusive with interval_seconds.
        interval_seconds: Interval in seconds between schedule triggers.
            Mutually exclusive with cron_expressions.
    """
    cron_expressions: list[str] = field(default_factory=list)
    interval_seconds: int = 0

    def to_proto(self):
        from . import khronos_pb2

        spec_type = "cron" if self.cron_expressions else "interval"
        return khronos_pb2.ScheduleSpec(
            spec_type=spec_type,
            cron_expressions=self.cron_expressions,
            interval_spec=(
                khronos_pb2.IntervalSpec(seconds=self.interval_seconds)
                if self.interval_seconds > 0
                else None
            ),
        )


@dataclass
class WorkflowAction:
    """Describes a workflow to be triggered by a schedule.

    Args:
        workflow_name: Name of the workflow definition.
        args: Key-value arguments passed to the workflow.
        task_queue: Task queue where workflow tasks are dispatched.
        id: Unique identifier for this action instance.
        timeouts: Timeout configuration for the workflow.
    """
    workflow_name: str
    args: dict[str, str] = field(default_factory=dict)
    task_queue: str = "default"
    id: str = ""
    timeouts: Timeouts = field(default_factory=Timeouts)

    def to_proto(self):
        from . import khronos_pb2

        return khronos_pb2.WorkflowAction(
            workflow_name=self.workflow_name,
            args=self.args,
            task_queue=self.task_queue,
            id=self.id,
            timeouts=khronos_pb2.Timeouts(
                execution_timeout_secs=self.timeouts.execution_timeout_secs,
                run_timeout_secs=self.timeouts.run_timeout_secs,
                task_timeout_secs=self.timeouts.task_timeout_secs,
            ),
        )


@dataclass(frozen=True)
class ScheduleInfo:
    """Information about an existing schedule.

    Attributes:
        schedule_id: Unique identifier for the schedule.
        namespace: Namespace this schedule belongs to.
        spec: The schedule specification (cron or interval).
        action: The workflow action triggered by this schedule.
        policy: Overlap policy for concurrent executions.
        created_at: ISO 8601 timestamp when the schedule was created.
    """
    schedule_id: str = ""
    namespace: str = ""
    spec: ScheduleSpec | None = None
    action: WorkflowAction | None = None
    policy: OverlapPolicy = OverlapPolicy.SKIP
    created_at: str = ""


@dataclass(frozen=True)
class WorkflowInfo:
    """Information about a workflow execution.

    Attributes:
        workflow_run_id: Unique run identifier for this workflow instance.
        workflow_id: Logical workflow ID (can be reused across runs).
        name: Name of the workflow definition.
        state: Current state — pending, running, completed, failed, cancelled.
        args: Arguments passed to the workflow.
        result_json: JSON-encoded result if completed.
        started_at: ISO 8601 timestamp when execution started.
        completed_at: ISO 8601 timestamp when execution finished (if done).
    """
    workflow_run_id: str = ""
    workflow_id: str = ""
    name: str = ""
    state: str = ""
    args: dict[str, str] = field(default_factory=dict)
    result_json: str = ""
    started_at: str = ""
    completed_at: str = ""


@dataclass(frozen=True)
class ActivityTask:
    """An activity task received from the server.

    Attributes:
        activity_id: Unique identifier for this activity instance.
        step_id: Step within the workflow that owns this activity.
        workflow_run_id: Workflow run that spawned this activity.
        name: Name of the activity type (matches registered handler).
        args: Key-value arguments passed to the activity.
        retry_policy: Retry configuration for this activity.
        heartbeat_timeout_secs: Seconds between required heartbeats.
        start_to_close_timeout_secs: Max time allowed to complete the activity.
    """
    activity_id: str = ""
    step_id: str = ""
    workflow_run_id: str = ""
    name: str = ""
    args: dict[str, str] = field(default_factory=dict)
    retry_policy: RetryPolicy | None = None
    heartbeat_timeout_secs: int = 0
    start_to_close_timeout_secs: int = 0
