// Khronos Integration Tests
// Copyright (C) 2025 Technetia Inc.
// This file is part of Khronos.
//
// Khronos is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

//! End-to-end integration tests using gRPC service implementations directly.
//! Tests cover ScheduleService, WorkflowService, and WorkerService APIs.

use std::collections::HashMap;
use std::sync::Arc;

use khronos_db::Database;
use khronos_server::grpc::KhronosState;
use tokio::sync::Mutex;

// Re-exported proto types from the server crate
use khronos_server::*;

// Import service traits so their methods are available on KhronosState
use khronos_server::schedule_service_server::ScheduleService;
use khronos_server::workflow_service_server::WorkflowService;
use khronos_server::worker_service_server::WorkerService;

fn test_state() -> KhronosState {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    // Keep the tempdir alive by storing it in a static — not ideal but works for tests
    std::mem::forget(dir);
    let db = Database::new(&path).unwrap();
    KhronosState::new(db)
}

// ─── ScheduleService Tests ──────────────────────────────────────

#[tokio::test]
async fn test_create_schedule_cron() {
    let state = test_state();

    let request = tonic::Request::new(CreateScheduleRequest {
        schedule_id: "cron-schedule-1".to_string(),
        namespace: "default".to_string(),
        spec: Some(ScheduleSpec {
            spec_type: "cron".to_string(),
            cron_expressions: vec!["0 * * * *".to_string()],
            interval_spec: None,
        }),
        action: Some(WorkflowAction {
            workflow_name: "test_workflow".to_string(),
            args: HashMap::new(),
            task_queue: "default".to_string(),
            id: "action-1".to_string(),
            timeouts: None,
        }),
        policy: Some(OverlapPolicy {
            policy: "skip".to_string(),
        }),
    });

    let response = state.create_schedule(request).await.unwrap();
    let resp = response.into_inner();
    assert_eq!(resp.schedule_id, "cron-schedule-1");
}

#[tokio::test]
async fn test_create_schedule_interval() {
    let state = test_state();

    let request = tonic::Request::new(CreateScheduleRequest {
        schedule_id: "interval-schedule-1".to_string(),
        namespace: "default".to_string(),
        spec: Some(ScheduleSpec {
            spec_type: "interval".to_string(),
            cron_expressions: vec![],
            interval_spec: Some(IntervalSpec { seconds: 300 }),
        }),
        action: Some(WorkflowAction {
            workflow_name: "cleanup_workflow".to_string(),
            args: HashMap::new(),
            task_queue: "maintenance".to_string(),
            id: "action-cleanup".to_string(),
            timeouts: None,
        }),
        policy: Some(OverlapPolicy {
            policy: "buffer".to_string(),
        }),
    });

    let response = state.create_schedule(request).await.unwrap();
    assert_eq!(response.into_inner().schedule_id, "interval-schedule-1");
}

#[tokio::test]
async fn test_create_schedule_missing_spec() {
    let state = test_state();

    let request = tonic::Request::new(CreateScheduleRequest {
        schedule_id: "bad-schedule".to_string(),
        namespace: "default".to_string(),
        spec: None,
        action: Some(WorkflowAction {
            workflow_name: "wf".to_string(),
            args: HashMap::new(),
            task_queue: "default".to_string(),
            id: "".to_string(),
            timeouts: None,
        }),
        policy: None,
    });

    let result = state.create_schedule(request).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_create_schedule_missing_action() {
    let state = test_state();

    let request = tonic::Request::new(CreateScheduleRequest {
        schedule_id: "bad-schedule".to_string(),
        namespace: "default".to_string(),
        spec: Some(ScheduleSpec {
            spec_type: "cron".to_string(),
            cron_expressions: vec!["0 * * * *".to_string()],
            interval_spec: None,
        }),
        action: None,
        policy: None,
    });

    let result = state.create_schedule(request).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_create_schedule_invalid_spec_type() {
    let state = test_state();

    let request = tonic::Request::new(CreateScheduleRequest {
        schedule_id: "bad-schedule".to_string(),
        namespace: "default".to_string(),
        spec: Some(ScheduleSpec {
            spec_type: "invalid_type".to_string(),
            cron_expressions: vec![],
            interval_spec: None,
        }),
        action: Some(WorkflowAction {
            workflow_name: "wf".to_string(),
            args: HashMap::new(),
            task_queue: "default".to_string(),
            id: "".to_string(),
            timeouts: None,
        }),
        policy: None,
    });

    let result = state.create_schedule(request).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_list_schedules_empty() {
    let state = test_state();

    let request = tonic::Request::new(ListSchedulesRequest {
        namespace: "default".to_string(),
    });

    let response = state.list_schedules(request).await.unwrap();
    assert!(response.into_inner().schedules.is_empty());
}

#[tokio::test]
async fn test_list_schedules_after_create() {
    let state = test_state();

    // Create a schedule
    let create_req = tonic::Request::new(CreateScheduleRequest {
        schedule_id: "list-test".to_string(),
        namespace: "default".to_string(),
        spec: Some(ScheduleSpec {
            spec_type: "cron".to_string(),
            cron_expressions: vec!["0 * * * *".to_string()],
            interval_spec: None,
        }),
        action: Some(WorkflowAction {
            workflow_name: "wf".to_string(),
            args: HashMap::new(),
            task_queue: "default".to_string(),
            id: "action-1".to_string(),
            timeouts: None,
        }),
        policy: None,
    });
    state.create_schedule(create_req).await.unwrap();

    // List schedules
    let list_req = tonic::Request::new(ListSchedulesRequest {
        namespace: "default".to_string(),
    });
    let response = state.list_schedules(list_req).await.unwrap();
    assert_eq!(response.into_inner().schedules.len(), 1);
}

#[tokio::test]
async fn test_update_schedule() {
    let state = test_state();

    // Create a schedule first
    let create_req = tonic::Request::new(CreateScheduleRequest {
        schedule_id: "update-test".to_string(),
        namespace: "default".to_string(),
        spec: Some(ScheduleSpec {
            spec_type: "cron".to_string(),
            cron_expressions: vec!["0 * * * *".to_string()],
            interval_spec: None,
        }),
        action: Some(WorkflowAction {
            workflow_name: "wf".to_string(),
            args: HashMap::new(),
            task_queue: "default".to_string(),
            id: "action-1".to_string(),
            timeouts: None,
        }),
        policy: None,
    });
    state.create_schedule(create_req).await.unwrap();

    // Update to interval spec
    let update_req = tonic::Request::new(UpdateScheduleRequest {
        schedule_id: "update-test".to_string(),
        namespace: "default".to_string(),
        spec: Some(ScheduleSpec {
            spec_type: "interval".to_string(),
            cron_expressions: vec![],
            interval_spec: Some(IntervalSpec { seconds: 600 }),
        }),
    });

    let response = state.update_schedule(update_req).await.unwrap();
    assert!(response.into_inner().success);
}

#[tokio::test]
async fn test_update_nonexistent_schedule() {
    let state = test_state();

    let update_req = tonic::Request::new(UpdateScheduleRequest {
        schedule_id: "nonexistent".to_string(),
        namespace: "default".to_string(),
        spec: Some(ScheduleSpec {
            spec_type: "cron".to_string(),
            cron_expressions: vec!["0 * * * *".to_string()],
            interval_spec: None,
        }),
    });

    let result = state.update_schedule(update_req).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_delete_schedule() {
    let state = test_state();

    // Create a schedule first
    let create_req = tonic::Request::new(CreateScheduleRequest {
        schedule_id: "delete-test".to_string(),
        namespace: "default".to_string(),
        spec: Some(ScheduleSpec {
            spec_type: "cron".to_string(),
            cron_expressions: vec!["0 * * * *".to_string()],
            interval_spec: None,
        }),
        action: Some(WorkflowAction {
            workflow_name: "wf".to_string(),
            args: HashMap::new(),
            task_queue: "default".to_string(),
            id: "action-1".to_string(),
            timeouts: None,
        }),
        policy: None,
    });
    state.create_schedule(create_req).await.unwrap();

    // Delete it
    let delete_req = tonic::Request::new(DeleteScheduleRequest {
        schedule_id: "delete-test".to_string(),
        namespace: "default".to_string(),
    });
    let response = state.delete_schedule(delete_req).await.unwrap();
    assert!(response.into_inner().success);

    // Verify it's gone
    let list_req = tonic::Request::new(ListSchedulesRequest {
        namespace: "default".to_string(),
    });
    let list_response = state.list_schedules(list_req).await.unwrap();
    assert!(list_response.into_inner().schedules.is_empty());
}

#[tokio::test]
async fn test_delete_nonexistent_schedule() {
    let state = test_state();

    let delete_req = tonic::Request::new(DeleteScheduleRequest {
        schedule_id: "nonexistent".to_string(),
        namespace: "default".to_string(),
    });
    let response = state.delete_schedule(delete_req).await.unwrap();
    assert!(!response.into_inner().success);
}

// ─── WorkflowService Tests ──────────────────────────────────────

#[tokio::test]
async fn test_start_workflow() {
    let state = test_state();

    let request = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "my_workflow".to_string(),
        args: HashMap::new(),
        workflow_id: "".to_string(),
        task_queue: "default".to_string(),
        timeouts: None,
        namespace: "default".to_string(),
    });

    let response = state.start_workflow(request).await.unwrap();
    let resp = response.into_inner();
    assert!(!resp.workflow_run_id.is_empty());
}

#[tokio::test]
async fn test_start_workflow_with_args() {
    let state = test_state();

    let mut args = HashMap::new();
    args.insert("input".to_string(), "data".to_string());
    args.insert("mode".to_string(), "fast".to_string());

    let request = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "my_workflow".to_string(),
        args,
        workflow_id: "custom-wf-id".to_string(),
        task_queue: "workers".to_string(),
        timeouts: Some(Timeouts {
            execution_timeout_secs: 3600,
            run_timeout_secs: 1800,
            task_timeout_secs: 300,
        }),
        namespace: "default".to_string(),
    });

    let response = state.start_workflow(request).await.unwrap();
    assert!(!response.into_inner().workflow_run_id.is_empty());
}

#[tokio::test]
async fn test_get_workflow() {
    let state = test_state();

    // Start a workflow first
    let start_req = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "my_workflow".to_string(),
        args: HashMap::new(),
        workflow_id: "".to_string(),
        task_queue: "default".to_string(),
        timeouts: None,
        namespace: "default".to_string(),
    });
    let start_resp = state.start_workflow(start_req).await.unwrap();
    let run_id = start_resp.into_inner().workflow_run_id;

    // Get the workflow
    let get_req = tonic::Request::new(GetWorkflowRequest {
        workflow_run_id: run_id.clone(),
        namespace: "default".to_string(),
    });
    let response = state.get_workflow(get_req).await.unwrap();
    let info = response.into_inner().info.expect("Should have workflow info");

    assert_eq!(info.workflow_run_id, run_id);
}

#[tokio::test]
async fn test_get_nonexistent_workflow() {
    let state = test_state();

    let get_req = tonic::Request::new(GetWorkflowRequest {
        workflow_run_id: "00000000-0000-0000-0000-000000000000".to_string(),
        namespace: "default".to_string(),
    });

    let result = state.get_workflow(get_req).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_list_workflows() {
    let state = test_state();

    // Start two workflows
    for _ in 0..2 {
        let start_req = tonic::Request::new(StartWorkflowRequest {
            workflow_name: "my_workflow".to_string(),
            args: HashMap::new(),
            workflow_id: "".to_string(),
            task_queue: "default".to_string(),
            timeouts: None,
            namespace: "default".to_string(),
        });
        state.start_workflow(start_req).await.unwrap();
    }

    let list_req = tonic::Request::new(ListWorkflowsRequest {
        namespace: "default".to_string(),
    });
    let response = state.list_workflows(list_req).await.unwrap();
    assert_eq!(response.into_inner().workflows.len(), 2);
}

#[tokio::test]
async fn test_cancel_workflow() {
    let state = test_state();

    // Start a workflow first
    let start_req = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "my_workflow".to_string(),
        args: HashMap::new(),
        workflow_id: "".to_string(),
        task_queue: "default".to_string(),
        timeouts: None,
        namespace: "default".to_string(),
    });
    let start_resp = state.start_workflow(start_req).await.unwrap();
    let run_id = start_resp.into_inner().workflow_run_id;

    // Cancel it
    let cancel_req = tonic::Request::new(CancelWorkflowRequest {
        workflow_run_id: run_id.clone(),
        namespace: "default".to_string(),
    });
    let response = state.cancel_workflow(cancel_req).await.unwrap();
    assert!(response.into_inner().success);

    // Verify it's cancelled
    let get_req = tonic::Request::new(GetWorkflowRequest {
        workflow_run_id: run_id,
        namespace: "default".to_string(),
    });
    let info = state.get_workflow(get_req).await.unwrap().into_inner().info.expect("Should have info");
    assert_eq!(info.state, "cancelled");
}

#[tokio::test]
async fn test_cancel_nonexistent_workflow() {
    let state = test_state();

    let cancel_req = tonic::Request::new(CancelWorkflowRequest {
        workflow_run_id: "00000000-0000-0000-0000-000000000000".to_string(),
        namespace: "default".to_string(),
    });

    let result = state.cancel_workflow(cancel_req).await;
    assert!(result.is_err());
}

// ─── WorkerService Tests ──────────────────────────────────────

#[tokio::test]
async fn test_poll_activity_no_tasks() {
    let state = test_state();

    let request = tonic::Request::new(PollActivityRequest {
        task_queue: "default".to_string(),
        activity_types: vec!["some_activity".to_string()],
    });

    let response = state.poll_activity(request).await.unwrap();
    assert!(!response.into_inner().has_task);
}

#[tokio::test]
async fn test_poll_activity_returns_matching_task() {
    let state = test_state();

    // Start a workflow with a known activity name
    let start_req = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "my_custom_activity".to_string(),
        args: HashMap::new(),
        workflow_id: "".to_string(),
        task_queue: "default".to_string(),
        timeouts: None,
        namespace: "default".to_string(),
    });
    state.start_workflow(start_req).await.unwrap();

    // Poll for the activity type that matches the workflow name (default single-step)
    let poll_req = tonic::Request::new(PollActivityRequest {
        task_queue: "default".to_string(),
        activity_types: vec!["my_custom_activity".to_string()],
    });

    let response = state.poll_activity(poll_req).await.unwrap();
    assert!(response.into_inner().has_task);
}

#[tokio::test]
async fn test_poll_activity_no_matching_type() {
    let state = test_state();

    // Start a workflow with activity "my_custom_activity"
    let start_req = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "my_custom_activity".to_string(),
        args: HashMap::new(),
        workflow_id: "".to_string(),
        task_queue: "default".to_string(),
        timeouts: None,
        namespace: "default".to_string(),
    });
    state.start_workflow(start_req).await.unwrap();

    // Poll for a different activity type — should not match
    let poll_req = tonic::Request::new(PollActivityRequest {
        task_queue: "default".to_string(),
        activity_types: vec!["different_activity".to_string()],
    });

    let response = state.poll_activity(poll_req).await.unwrap();
    assert!(!response.into_inner().has_task);
}

#[tokio::test]
async fn test_report_activity_result() {
    let state = test_state();

    // Start a workflow and poll for the activity
    let start_req = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "report_test".to_string(),
        args: HashMap::new(),
        workflow_id: "".to_string(),
        task_queue: "default".to_string(),
        timeouts: None,
        namespace: "default".to_string(),
    });
    state.start_workflow(start_req).await.unwrap();

    let poll_req = tonic::Request::new(PollActivityRequest {
        task_queue: "default".to_string(),
        activity_types: vec!["report_test".to_string()],
    });
    let poll_resp = state.poll_activity(poll_req).await.unwrap().into_inner();
    assert!(poll_resp.has_task);
    let task = poll_resp.task.expect("Should have a task");

    // Report success
    let report_req = tonic::Request::new(ReportActivityResultRequest {
        activity_id: task.activity_id,
        result_json: r#"{"output": "success"}"#.to_string(),
    });

    let response = state.report_activity_result(report_req).await.unwrap();
    assert!(response.into_inner().success);
}

#[tokio::test]
async fn test_report_activity_failure() {
    let state = test_state();

    // Start a workflow and poll for the activity
    let start_req = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "failure_test".to_string(),
        args: HashMap::new(),
        workflow_id: "".to_string(),
        task_queue: "default".to_string(),
        timeouts: None,
        namespace: "default".to_string(),
    });
    state.start_workflow(start_req).await.unwrap();

    let poll_req = tonic::Request::new(PollActivityRequest {
        task_queue: "default".to_string(),
        activity_types: vec!["failure_test".to_string()],
    });
    let poll_resp = state.poll_activity(poll_req).await.unwrap().into_inner();
    assert!(poll_resp.has_task);
    let task = poll_resp.task.expect("Should have a task");

    // Report failure
    let report_req = tonic::Request::new(ReportActivityFailureRequest {
        activity_id: task.activity_id,
        error_message: "Something went wrong".to_string(),
    });

    let response = state.report_activity_failure(report_req).await.unwrap();
    assert!(response.into_inner().success);
}

#[tokio::test]
async fn test_heartbeat() {
    let state = test_state();

    // Start a workflow and poll for the activity
    let start_req = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "hb_test".to_string(),
        args: HashMap::new(),
        workflow_id: "".to_string(),
        task_queue: "default".to_string(),
        timeouts: None,
        namespace: "default".to_string(),
    });
    state.start_workflow(start_req).await.unwrap();

    let poll_req = tonic::Request::new(PollActivityRequest {
        task_queue: "default".to_string(),
        activity_types: vec!["hb_test".to_string()],
    });
    let poll_resp = state.poll_activity(poll_req).await.unwrap().into_inner();
    assert!(poll_resp.has_task);
    let task = poll_resp.task.expect("Should have a task");

    // Send heartbeat
    let hb_req = tonic::Request::new(HeartbeatRequest {
        activity_id: task.activity_id,
    });

    let response = state.heartbeat(hb_req).await.unwrap();
    assert!(response.into_inner().success);
}

// ─── End-to-End Integration Tests ──────────────────────────────

#[tokio::test]
async fn test_full_schedule_workflow_lifecycle() {
    let state = test_state();

    // 1. Create a schedule
    let create_req = tonic::Request::new(CreateScheduleRequest {
        schedule_id: "e2e-schedule".to_string(),
        namespace: "default".to_string(),
        spec: Some(ScheduleSpec {
            spec_type: "cron".to_string(),
            cron_expressions: vec!["0 * * * *".to_string()],
            interval_spec: None,
        }),
        action: Some(WorkflowAction {
            workflow_name: "e2e_workflow".to_string(),
            args: HashMap::new(),
            task_queue: "default".to_string(),
            id: "action-e2e".to_string(),
            timeouts: None,
        }),
        policy: Some(OverlapPolicy {
            policy: "skip".to_string(),
        }),
    });
    state.create_schedule(create_req).await.unwrap();

    // 2. Verify schedule appears in list
    let list_req = tonic::Request::new(ListSchedulesRequest {
        namespace: "default".to_string(),
    });
    let list_resp = state.list_schedules(list_req).await.unwrap();
    assert_eq!(list_resp.into_inner().schedules.len(), 1);

    // 3. Start a workflow manually (simulating schedule trigger)
    let start_req = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "e2e_workflow".to_string(),
        args: HashMap::new(),
        workflow_id: "".to_string(),
        task_queue: "default".to_string(),
        timeouts: None,
        namespace: "default".to_string(),
    });
    let start_resp = state.start_workflow(start_req).await.unwrap();
    let run_id = start_resp.into_inner().workflow_run_id;

    // 4. Verify workflow was created and is pending
    let get_req = tonic::Request::new(GetWorkflowRequest {
        workflow_run_id: run_id.clone(),
        namespace: "default".to_string(),
    });
    let wf_info = state.get_workflow(get_req).await.unwrap().into_inner().info.expect("Should have info");
    assert_eq!(wf_info.state, "pending");

    // 5. Poll for activity and complete it
    let poll_req = tonic::Request::new(PollActivityRequest {
        task_queue: "default".to_string(),
        activity_types: vec!["e2e_workflow".to_string()],
    });
    let poll_resp = state.poll_activity(poll_req).await.unwrap().into_inner();
    assert!(poll_resp.has_task);

    // 6. Report result
    let task = poll_resp.task.expect("Should have a task");
    let report_req = tonic::Request::new(ReportActivityResultRequest {
        activity_id: task.activity_id,
        result_json: r#"{"status": "done"}"#.to_string(),
    });
    state.report_activity_result(report_req).await.unwrap();

    // 7. Verify workflow is now running (transitioned when step was picked up)
    let get_req = tonic::Request::new(GetWorkflowRequest {
        workflow_run_id: run_id.clone(),
        namespace: "default".to_string(),
    });
    let wf_info = state.get_workflow(get_req).await.unwrap().into_inner().info.expect("Should have info");
    assert_eq!(wf_info.state, "running");

    // 8. Delete the schedule and verify it's gone
    let delete_req = tonic::Request::new(DeleteScheduleRequest {
        schedule_id: "e2e-schedule".to_string(),
        namespace: "default".to_string(),
    });
    state.delete_schedule(delete_req).await.unwrap();

    let list_req = tonic::Request::new(ListSchedulesRequest {
        namespace: "default".to_string(),
    });
    let list_resp = state.list_schedules(list_req).await.unwrap();
    assert!(list_resp.into_inner().schedules.is_empty());
}

#[tokio::test]
async fn test_multiple_workflows_in_namespace() {
    let state = test_state();

    // Start 3 workflows in the same namespace
    for i in 0..3 {
        let mut args = HashMap::new();
        args.insert("index".to_string(), i.to_string());

        let start_req = tonic::Request::new(StartWorkflowRequest {
            workflow_name: "batch_workflow".to_string(),
            args,
            workflow_id: format!("batch-{}", i),
            task_queue: "default".to_string(),
            timeouts: None,
            namespace: "default".to_string(),
        });
        state.start_workflow(start_req).await.unwrap();
    }

    // List all workflows
    let list_req = tonic::Request::new(ListWorkflowsRequest {
        namespace: "default".to_string(),
    });
    let list_resp = state.list_workflows(list_req).await.unwrap();
    let inner = list_resp.into_inner();
    assert_eq!(inner.workflows.len(), 3);

    // Cancel all of them
    for wf in &inner.workflows {
        let cancel_req = tonic::Request::new(CancelWorkflowRequest {
            workflow_run_id: wf.workflow_run_id.clone(),
            namespace: "default".to_string(),
        });
        state.cancel_workflow(cancel_req).await.unwrap();
    }

    // Verify all are cancelled
    let list_req = tonic::Request::new(ListWorkflowsRequest {
        namespace: "default".to_string(),
    });
    let list_resp = state.list_workflows(list_req).await.unwrap();
    for wf in &list_resp.into_inner().workflows {
        assert_eq!(wf.state, "cancelled");
    }
}

#[tokio::test]
async fn test_cron_job_workflow_definition() {
    let state = test_state();

    // Start the built-in cron_job_workflow which has 5 steps
    let start_req = tonic::Request::new(StartWorkflowRequest {
        workflow_name: "cron_job_workflow".to_string(),
        args: HashMap::new(),
        workflow_id: "".to_string(),
        task_queue: "default".to_string(),
        timeouts: None,
        namespace: "default".to_string(),
    });
    let start_resp = state.start_workflow(start_req).await.unwrap();
    let run_id = start_resp.into_inner().workflow_run_id;

    // Verify workflow was created
    let get_req = tonic::Request::new(GetWorkflowRequest {
        workflow_run_id: run_id.clone(),
        namespace: "default".to_string(),
    });
    let wf_info = state.get_workflow(get_req).await.unwrap().into_inner().info.expect("Should have info");
    assert_eq!(wf_info.name, "cron_job_workflow");

    // Poll for the first activity (execute_cron_job)
    let poll_req = tonic::Request::new(PollActivityRequest {
        task_queue: "default".to_string(),
        activity_types: vec!["execute_cron_job".to_string()],
    });
    let poll_resp = state.poll_activity(poll_req).await.unwrap().into_inner();
    assert!(poll_resp.has_task);
}
