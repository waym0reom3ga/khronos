"""KhronosWorker — activity polling and execution."""

import asyncio
import json
import logging
from typing import Any, Callable

from . import khronos_pb2, khronos_pb2_grpc
from .types import ActivityTask, RetryPolicy

logger = logging.getLogger(__name__)


ActivityHandler = Callable[..., Any]
"""Type alias for activity handlers. Can be sync or async callables."""


class KhronosWorker:
    """Worker that polls for and executes activities from the Khronos server.

    Args:
        client: A connected KhronosClient instance (used to get channel info).
        task_queue: The task queue this worker subscribes to.
        activities: Dict mapping activity names to handler callables.
            Handlers receive keyword arguments matching the activity args.
        poll_interval_secs: Seconds between polls when no tasks are available.
    """

    def __init__(
        self,
        client: Any,  # KhronosClient — avoid circular import in type hint
        task_queue: str = "default",
        activities: dict[str, ActivityHandler] | None = None,
        poll_interval_secs: float = 1.0,
    ):
        self._client = client
        self._task_queue = task_queue
        self._activities = activities or {}
        self._poll_interval = poll_interval_secs
        self._channel = getattr(client, "_channel", None)
        self._stub: khronos_pb2_grpc.WorkerServiceStub | None = (
            khronos_pb2_grpc.WorkerServiceStub(self._channel) if self._channel else None
        )
        self._running = False

    @property
    def task_queue(self) -> str:
        return self._task_queue

    @property
    def activities(self) -> dict[str, ActivityHandler]:
        return dict(self._activities)

    async def run(self):
        """Start the poll loop. Blocks until cancelled or stopped.

        Continuously polls for activity tasks and executes them using
        registered handlers. Implements exponential backoff on empty responses.
        """
        if self._stub is None:
            raise RuntimeError("Worker client must be connected before calling run()")

        logger.info(
            "Starting worker on queue '%s' with %d activities",
            self._task_queue,
            len(self._activities),
        )
        self._running = True
        backoff = 0.1  # Start with aggressive polling

        try:
            while self._running:
                task = await self._poll()
                if task is not None:
                    backoff = self._poll_interval  # Reset backoff on success
                    await self._execute(task)
                else:
                    # Exponential backoff, capped at poll_interval * 10
                    await asyncio.sleep(min(backoff, self._poll_interval * 10))
                    backoff = min(backoff * 2, self._poll_interval * 10)
        except asyncio.CancelledError:
            logger.info("Worker cancelled")
        finally:
            self._running = False
            logger.info("Worker stopped")

    def stop(self):
        """Signal the worker to stop after the current iteration."""
        logger.info("Stop requested for worker on '%s'", self._task_queue)
        self._running = False

    async def _poll(self) -> ActivityTask | None:
        """Poll the server for a single activity task.

        Returns:
            ActivityTask if one is available, None otherwise.
        """
        activity_types = list(self._activities.keys())
        request = khronos_pb2.PollActivityRequest(
            task_queue=self._task_queue,
            activity_types=activity_types,
        )

        try:
            response = await asyncio.get_running_loop().run_in_executor(
                None, self._stub.PollActivity, request
            )
        except Exception as exc:
            logger.warning("Poll failed: %s", exc)
            return None

        if not response.has_task:
            return None

        task_proto = response.task
        retry_policy = None
        if task_proto.HasField("retry_policy"):
            retry_policy = RetryPolicy(
                maximum_attempts=task_proto.retry_policy.maximum_attempts,
                initial_interval_secs=task_proto.retry_policy.initial_interval_secs,
            )

        return ActivityTask(
            activity_id=task_proto.activity_id,
            step_id=task_proto.step_id,
            workflow_run_id=task_proto.workflow_run_id,
            name=task_proto.name,
            args=dict(task_proto.args),
            retry_policy=retry_policy,
            heartbeat_timeout_secs=task_proto.heartbeat_timeout_secs,
            start_to_close_timeout_secs=task_proto.start_to_close_timeout_secs,
        )

    async def _execute(self, task: ActivityTask):
        """Execute a single activity task and report the result."""
        handler = self._activities.get(task.name)
        if handler is None:
            logger.error("No handler registered for activity '%s'", task.name)
            await self._report_failure(
                task.activity_id, f"No handler registered for activity '{task.name}'"
            )
            return

        logger.info(
            "Executing activity %s (type=%s, workflow=%s)",
            task.activity_id,
            task.name,
            task.workflow_run_id,
        )

        try:
            # Call handler — supports both sync and async handlers
            result = handler(**task.args)
            if asyncio.iscoroutine(result):
                result = await result

            # Serialize result to JSON for the server
            result_json = json.dumps(result) if result is not None else "null"
            success = await self._report_result(task.activity_id, result_json)
            if success:
                logger.info("Activity %s completed successfully", task.activity_id)
            else:
                logger.warning("Failed to report result for activity %s", task.activity_id)

        except Exception as exc:
            logger.error(
                "Activity %s failed with error: %s", task.activity_id, exc, exc_info=True
            )
            await self._report_failure(task.activity_id, str(exc))

    async def _report_result(self, activity_id: str, result_json: str) -> bool:
        """Report a successful activity result to the server."""
        request = khronos_pb2.ReportActivityResultRequest(
            activity_id=activity_id,
            result_json=result_json,
        )
        try:
            response = await asyncio.get_running_loop().run_in_executor(
                None, self._stub.ReportActivityResult, request  # type: ignore[union-attr]
            )
            return response.success
        except Exception as exc:
            logger.error("Failed to report result for %s: %s", activity_id, exc)
            return False

    async def _report_failure(self, activity_id: str, error_message: str) -> bool:
        """Report an activity failure to the server."""
        request = khronos_pb2.ReportActivityFailureRequest(
            activity_id=activity_id,
            error_message=error_message,
        )
        try:
            response = await asyncio.get_running_loop().run_in_executor(
                None, self._stub.ReportActivityFailure, request  # type: ignore[union-attr]
            )
            return response.success
        except Exception as exc:
            logger.error("Failed to report failure for %s: %s", activity_id, exc)
            return False

    async def heartbeat(self, activity_id: str) -> bool:
        """Send a heartbeat for a long-running activity.

        Args:
            activity_id: The activity to send a heartbeat for.

        Returns:
            True if the heartbeat was accepted.
        """
        request = khronos_pb2.HeartbeatRequest(activity_id=activity_id)
        try:
            response = await asyncio.get_running_loop().run_in_executor(
                None, self._stub.Heartbeat, request  # type: ignore[union-attr]
            )
            return response.success
        except Exception as exc:
            logger.error("Heartbeat failed for %s: %s", activity_id, exc)
            return False
