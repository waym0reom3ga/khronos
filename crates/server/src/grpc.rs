//! gRPC service implementations for Khronos.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use khronos_core::{OverlapPolicy as CoreOverlapPolicy, RetryPolicy as CoreRetryPolicy, Schedule, ScheduleSpec as CoreScheduleSpec, Timeouts as CoreTimeouts, WorkflowAction as CoreWorkflowAction, WorkflowInstance, WorkflowState, WorkflowStepInstance};
use khronos_db::activities::{self as db_activities};
use khronos_db::schedules::{self as db_schedules};
use khronos_db::workflows::{self as db_workflows};
use khronos_db::Database;
use tokio::sync::Mutex;

// Re-exported proto types from the parent module (lib.rs)
use super::*;

// Tonic-generated service traits — these live in submodules, not at top level.
use crate::schedule_service_server::{ScheduleService, ScheduleServiceServer};
use crate::workflow_service_server::{WorkflowService, WorkflowServiceServer};
use crate::worker_service_server::{WorkerService, WorkerServiceServer};
use tonic::{Request, Response, Status};

/// Shared server state.
#[derive(Clone)]
pub struct KhronosState {
    pub db: Arc<Mutex<Database>>,
}

impl KhronosState {
    pub fn new(db: Database) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
        }
    }
}

// ─── ScheduleService ──────────────────────────────────────────────

#[async_trait]
impl ScheduleService for KhronosState {
    async fn create_schedule(
        &self,
        request: Request<CreateScheduleRequest>,
    ) -> Result<Response<CreateScheduleResponse>, Status> {
        let req = request.into_inner();
        let spec = req.spec.ok_or_else(|| Status::invalid_argument("spec is required"))?;
        let action = req.action.ok_or_else(|| Status::invalid_argument("action is required"))?;

        // Convert proto ScheduleSpec to core type
        let schedule_spec = match spec.spec_type.as_str() {
            "cron" => CoreScheduleSpec::Cron(spec.cron_expressions),
            "interval" => {
                if let Some(interval) = spec.interval_spec {
                    CoreScheduleSpec::Interval(std::time::Duration::from_secs(interval.seconds))
                } else {
                    return Err(Status::invalid_argument("interval seconds required for interval spec"));
                }
            }
            other => return Err(Status::invalid_argument(format!("unknown spec_type: {}", other))),
        };

        // Convert proto OverlapPolicy to core type
        let policy = match req.policy.map(|p| p.policy).unwrap_or_default().as_str() {
            "skip" | "" => CoreOverlapPolicy::Skip,
            "buffer" => CoreOverlapPolicy::Buffer(1),
            "terminate" => CoreOverlapPolicy::TerminateExisting,
            other => return Err(Status::invalid_argument(format!("unknown policy: {}", other))),
        };

        // Convert proto Timeouts to core type
        let timeouts = if let Some(t) = action.timeouts {
            CoreTimeouts {
                execution_timeout_secs: if t.execution_timeout_secs == 0 { None } else { Some(t.execution_timeout_secs) },
                run_timeout_secs: if t.run_timeout_secs == 0 { None } else { Some(t.run_timeout_secs) },
                task_timeout_secs: if t.task_timeout_secs == 0 { None } else { Some(t.task_timeout_secs) },
            }
        } else {
            CoreTimeouts::default()
        };

        let now = Utc::now();
        let schedule = Schedule {
            schedule_id: req.schedule_id.clone(),
            namespace: req.namespace.clone(),
            spec: schedule_spec,
            action: CoreWorkflowAction {
                workflow_name: action.workflow_name,
                args: action.args.into_iter().collect::<BTreeMap<_, _>>(),
                task_queue: action.task_queue,
                id: action.id,
                timeouts,
            },
            policy,
            created_at: now,
            updated_at: now,
        };

        let db = self.db.lock().await;
        db_schedules::insert_schedule(&&db.connection(), &schedule)
            .map_err(|e| Status::internal(format!("Failed to create schedule: {}", e)))?;

        Ok(Response::new(CreateScheduleResponse {
            schedule_id: req.schedule_id,
        }))
    }

    async fn update_schedule(
        &self,
        request: Request<UpdateScheduleRequest>,
    ) -> Result<Response<UpdateScheduleResponse>, Status> {
        let req = request.into_inner();
        let spec = req.spec.ok_or_else(|| Status::invalid_argument("spec is required"))?;

        // Get existing schedule to preserve action and policy
        let db = self.db.lock().await;
        let existing = db_schedules::get_schedule(&db.connection(), &req.schedule_id)
            .map_err(|e| Status::internal(format!("Failed to get schedule: {}", e)))?
            .ok_or_else(|| Status::not_found("Schedule not found"))?;

        // Convert proto ScheduleSpec to core type
        let new_spec = match spec.spec_type.as_str() {
            "cron" => CoreScheduleSpec::Cron(spec.cron_expressions),
            "interval" => {
                if let Some(interval) = spec.interval_spec {
                    CoreScheduleSpec::Interval(std::time::Duration::from_secs(interval.seconds))
                } else {
                    return Err(Status::invalid_argument("interval seconds required for interval spec"));
                }
            }
            other => return Err(Status::invalid_argument(format!("unknown spec_type: {}", other))),
        };

        let mut updated = existing.clone();
        updated.spec = new_spec;
        updated.updated_at = Utc::now();

        db_schedules::update_schedule_spec(&db.connection(), &req.schedule_id, &req.namespace, &updated)
            .map_err(|e| Status::internal(format!("Failed to update schedule: {}", e)))?;

        Ok(Response::new(UpdateScheduleResponse { success: true }))
    }

    async fn delete_schedule(
        &self,
        request: Request<DeleteScheduleRequest>,
    ) -> Result<Response<DeleteScheduleResponse>, Status> {
        let req = request.into_inner();
        let db = self.db.lock().await;
        let rows = db_schedules::delete_schedule(&db.connection(), &req.schedule_id, &req.namespace)
            .map_err(|e| Status::internal(format!("Failed to delete schedule: {}", e)))?;

        Ok(Response::new(DeleteScheduleResponse {
            success: rows > 0,
        }))
    }

    async fn list_schedules(
        &self,
        request: Request<ListSchedulesRequest>,
    ) -> Result<Response<ListSchedulesResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() { "default".to_string() } else { req.namespace };

        let db = self.db.lock().await;
        let schedules = db_schedules::list_schedules(&db.connection(), &namespace)
            .map_err(|e| Status::internal(format!("Failed to list schedules: {}", e)))?;

        let schedule_infos: Vec<ScheduleInfo> = schedules.into_iter().map(|s| {
            ScheduleInfo {
                schedule_id: s.schedule_id,
                namespace: s.namespace,
                spec: Some(schedule_spec_to_proto(&s.spec)),
                action: Some(workflow_action_to_proto(&s.action)),
                policy: Some(overlap_policy_to_proto(&s.policy)),
                created_at: s.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
            }
        }).collect();

        Ok(Response::new(ListSchedulesResponse {
            schedules: schedule_infos,
        }))
    }
}

// ─── WorkflowService ──────────────────────────────────────────────

#[async_trait]
impl WorkflowService for KhronosState {
    async fn start_workflow(
        &self,
        request: Request<StartWorkflowRequest>,
    ) -> Result<Response<StartWorkflowResponse>, Status> {
        let req = request.into_inner();
        let run_id = uuid::Uuid::new_v4();
        let namespace = if req.namespace.is_empty() { "default".to_string() } else { req.namespace };

        // Convert args map to JSON value
        let args_json: serde_json::Value = if req.args.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::Map::from_iter(req.args.iter().map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))).into()
        };

        // Convert proto Timeouts to core type
        let timeouts = if let Some(t) = req.timeouts {
            CoreTimeouts {
                execution_timeout_secs: if t.execution_timeout_secs == 0 { None } else { Some(t.execution_timeout_secs) },
                run_timeout_secs: if t.run_timeout_secs == 0 { None } else { Some(t.run_timeout_secs) },
                task_timeout_secs: if t.task_timeout_secs == 0 { None } else { Some(t.task_timeout_secs) },
            }
        } else {
            CoreTimeouts::default()
        };

        let workflow_id = if req.workflow_id.is_empty() {
            format!("{}-{}", req.workflow_name, run_id.to_string().chars().take(8).collect::<String>())
        } else {
            req.workflow_id.clone()
        };

        let task_queue = if req.task_queue.is_empty() { "default".to_string() } else { req.task_queue };

        let workflow = WorkflowInstance {
            run_id,
            workflow_id: workflow_id.clone(),
            name: req.workflow_name.clone(),
            task_queue: task_queue.clone(),
            state: WorkflowState::Pending,
            args_json: args_json.clone(),
            result_json: None,
            timeouts,
            started_at: None,
            completed_at: None,
            namespace: namespace.clone(),
        };

        let db = self.db.lock().await;

        // Insert the workflow instance
        db_workflows::insert_workflow(&db.connection(), &workflow)
            .map_err(|e| Status::internal(format!("Failed to create workflow: {}", e)))?;

        // Create workflow steps based on built-in definitions
        let steps = get_workflow_definition(&req.workflow_name);
        for (index, step_def) in steps.iter().enumerate() {
            let step_args_json: serde_json::Value = if req.args.is_empty() {
                serde_json::json!({})
            } else {
                // Merge workflow args with step template
                let mut merged: std::collections::BTreeMap<String, String> = req.args.iter().map(|(k,v)| (k.clone(), v.clone())).collect();
                for (k, v) in &step_def.args_template {
                    merged.insert(k.clone(), v.clone());
                }
                serde_json::Map::from_iter(merged.iter().map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))).into()
            };

            let step = WorkflowStepInstance {
                id: uuid::Uuid::new_v4(),
                workflow_run_id: run_id,
                step_index: index,
                activity_name: step_def.activity_name.clone(),
                args_json: step_args_json,
                retry_policy: step_def.retry_policy.clone(),
                timeout_secs: step_def.timeout_secs,
                heartbeat_timeout_secs: step_def.heartbeat_timeout_secs,
                state: WorkflowState::Pending,
                attempt: 0,
                result_json: None,
                next_retry_at: None,
            };

            db_activities::insert_workflow_step(&db.connection(), &step)
                .map_err(|e| Status::internal(format!("Failed to create step {}: {}", index, e)))?;
        }

        Ok(Response::new(StartWorkflowResponse {
            workflow_run_id: run_id.to_string(),
        }))
    }

    async fn get_workflow(
        &self,
        request: Request<GetWorkflowRequest>,
    ) -> Result<Response<GetWorkflowResponse>, Status> {
        let req = request.into_inner();
        let db = self.db.lock().await;
        let workflow = db_workflows::get_workflow(&db.connection(), &req.workflow_run_id)
            .map_err(|e| Status::internal(format!("Failed to get workflow: {}", e)))?
            .ok_or_else(|| Status::not_found("Workflow not found"))?;

        Ok(Response::new(GetWorkflowResponse {
            info: Some(workflow_info_to_proto(&workflow)),
        }))
    }

    async fn list_workflows(
        &self,
        request: Request<ListWorkflowsRequest>,
    ) -> Result<Response<ListWorkflowsResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() { "default".to_string() } else { req.namespace };

        let db = self.db.lock().await;
        let workflows = db_workflows::list_workflows(&db.connection(), &namespace, None)
            .map_err(|e| Status::internal(format!("Failed to list workflows: {}", e)))?;

        let workflow_infos: Vec<WorkflowInfo> = workflows.into_iter()
            .map(|wf| workflow_info_to_proto(&wf))
            .collect();

        Ok(Response::new(ListWorkflowsResponse {
            workflows: workflow_infos,
        }))
    }

    async fn cancel_workflow(
        &self,
        request: Request<CancelWorkflowRequest>,
    ) -> Result<Response<CancelWorkflowResponse>, Status> {
        let req = request.into_inner();
        let db = self.db.lock().await;
        db_workflows::cancel_workflow(&db.connection(), &req.workflow_run_id)
            .map_err(|e| Status::internal(format!("Failed to cancel workflow: {}", e)))?;

        Ok(Response::new(CancelWorkflowResponse { success: true }))
    }
}

// ─── WorkerService ──────────────────────────────────────────────

#[async_trait]
impl WorkerService for KhronosState {
    async fn poll_activity(
        &self,
        request: Request<PollActivityRequest>,
    ) -> Result<Response<PollActivityResponse>, Status> {
        let req = request.into_inner();

        // Find pending steps that match the worker's activity types
        let db = self.db.lock().await;
        let conn = db.connection();

        // Get all pending/retried steps ready to execute
        let pending_steps = db_activities::get_pending_steps(&conn)
            .map_err(|e| Status::internal(format!("Failed to get pending steps: {}", e)))?;

        // Find a step whose activity_name matches one of the worker's registered types
        for step in &pending_steps {
            if req.activity_types.contains(&step.activity_name) {
                // Create an activity attempt and return it as a task
                let activity_id = uuid::Uuid::new_v4();
                let new_attempt = step.attempt + 1;

                // Update step state to running
                db_activities::update_step_state(
                    &conn,
                    &step.id.to_string(),
                    WorkflowState::Running,
                    Some(new_attempt),
                    None,
                    None,
                ).map_err(|e| Status::internal(format!("Failed to update step state: {}", e)))?;

                // Insert activity attempt record
                db_activities::insert_activity_attempt(
                    &conn,
                    &activity_id.to_string(),
                    &step.id.to_string(),
                    new_attempt,
                ).map_err(|e| Status::internal(format!("Failed to insert activity: {}", e)))?;

                // Also transition the workflow to RUNNING if it's still PENDING
                db_workflows::update_workflow_state(
                    &conn,
                    &step.workflow_run_id.to_string(),
                    WorkflowState::Running,
                    None,
                    Some(Utc::now()),
                ).ok();

                // Convert args_json to HashMap for the proto
                let args: HashMap<String, String> = if let serde_json::Value::Object(map) = &step.args_json {
                    map.iter().filter_map(|(k, v)| v.as_str().map(|sv| (k.clone(), sv.to_string()))).collect()
                } else {
                    HashMap::new()
                };

                let task = ActivityTask {
                    activity_id: activity_id.to_string(),
                    step_id: step.id.to_string(),
                    workflow_run_id: step.workflow_run_id.to_string(),
                    name: step.activity_name.clone(),
                    args,
                    retry_policy: Some(RetryPolicy {
                        maximum_attempts: step.retry_policy.maximum_attempts,
                        initial_interval_secs: step.retry_policy.initial_interval_secs,
                    }),
                    heartbeat_timeout_secs: step.heartbeat_timeout_secs.unwrap_or(30),
                    start_to_close_timeout_secs: step.timeout_secs,
                };

                return Ok(Response::new(PollActivityResponse {
                    task: Some(task),
                    has_task: true,
                }));
            }
        }

        // No matching task found
        Ok(Response::new(PollActivityResponse {
            task: None,
            has_task: false,
        }))
    }

    async fn report_activity_result(
        &self,
        request: Request<ReportActivityResultRequest>,
    ) -> Result<Response<ReportActivityResultResponse>, Status> {
        let req = request.into_inner();

        let db = self.db.lock().await;
        let conn = db.connection();

        // Parse result JSON
        let result_json: serde_json::Value = if req.result_json.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_str(&req.result_json)
                .map_err(|e| Status::invalid_argument(format!("Invalid result JSON: {}", e)))?
        };

        // Update activity state to completed
        db_activities::update_activity_state(
            &conn,
            &req.activity_id,
            khronos_core::ActivityState::Completed,
            Some(&result_json),
            None,
        ).map_err(|e| Status::internal(format!("Failed to update activity: {}", e)))?;

        // Find the step_id for this activity
        let step_id: String = conn.query_row(
            "SELECT step_id FROM activities WHERE id = ?1",
            [req.activity_id.as_str()],
            |row| row.get(0),
        ).map_err(|e| Status::internal(format!("Failed to find activity: {}", e)))?;

        db_activities::update_step_state(
            &conn,
            &step_id,
            WorkflowState::Completed,
            None,
            Some(&result_json),
            None,
        ).map_err(|e| Status::internal(format!("Failed to update step: {}", e)))?;

        Ok(Response::new(ReportActivityResultResponse { success: true }))
    }

    async fn report_activity_failure(
        &self,
        request: Request<ReportActivityFailureRequest>,
    ) -> Result<Response<ReportActivityFailureResponse>, Status> {
        let req = request.into_inner();

        let db = self.db.lock().await;
        let conn = db.connection();

        // Find the activity and its step to check retry policy
        let (step_id, attempt): (String, u32) = conn.query_row(
            "SELECT a.step_id, a.attempt FROM activities a WHERE a.id = ?1",
            [req.activity_id.as_str()],
            |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u32)),
        ).map_err(|e| Status::internal(format!("Failed to find activity: {}", e)))?;

        // Get step retry policy
        let retry_policy_json: String = conn.query_row(
            "SELECT retry_policy_json FROM workflow_steps WHERE id = ?1",
            [step_id.as_str()],
            |row| row.get(0),
        ).map_err(|e| Status::internal(format!("Failed to get retry policy: {}", e)))?;

        let retry_policy: CoreRetryPolicy = serde_json::from_str(&retry_policy_json)
            .map_err(|e| Status::internal(format!("Invalid retry policy JSON: {}", e)))?;

        // Update activity as failed
        db_activities::update_activity_state(
            &conn,
            &req.activity_id,
            khronos_core::ActivityState::Failed,
            None,
            Some(&req.error_message),
        ).map_err(|e| Status::internal(format!("Failed to update activity: {}", e)))?;

        // Check if we should retry
        let current_attempt = attempt + 1;
        if current_attempt < retry_policy.maximum_attempts {
            // Calculate exponential backoff
            let backoff_secs = retry_policy.initial_interval_secs * (2.0_f64.powi((current_attempt - 1) as i32));
            let next_retry_at = Utc::now() + chrono::Duration::seconds(backoff_secs as i64);

            // Mark step as pending for retry with next_retry_at
            db_activities::update_step_state(
                &conn,
                &step_id,
                WorkflowState::Pending,
                Some(current_attempt),
                None,
                Some(next_retry_at),
            ).map_err(|e| Status::internal(format!("Failed to schedule retry: {}", e)))?;

            tracing::info!(
                activity_id = %req.activity_id,
                attempt = current_attempt,
                max_attempts = retry_policy.maximum_attempts,
                next_retry_at = ?next_retry_at,
                "Activity failed, scheduling retry"
            );
        } else {
            // Max retries exceeded - mark step as failed
            db_activities::update_step_state(
                &conn,
                &step_id,
                WorkflowState::Failed,
                Some(current_attempt),
                None,
                None,
            ).map_err(|e| Status::internal(format!("Failed to mark step as failed: {}", e)))?;

            tracing::error!(
                activity_id = %req.activity_id,
                attempt = current_attempt,
                max_attempts = retry_policy.maximum_attempts,
                error = %req.error_message,
                "Activity failed permanently after all retries"
            );
        }

        Ok(Response::new(ReportActivityFailureResponse { success: true }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();

        let db = self.db.lock().await;
        db_activities::heartbeat_update(&db.connection(), &req.activity_id)
            .map_err(|e| Status::internal(format!("Failed to update heartbeat: {}", e)))?;

        Ok(Response::new(HeartbeatResponse { success: true }))
    }
}

// ─── Server runner ──────────────────────────────────────────────

/// Start the gRPC server, spawning scheduler and engine as background tasks.
pub async fn run_server(
    addr: std::net::SocketAddr,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = KhronosState::new(db.clone());

    // Spawn scheduler as background task
    let scheduler_db = db.clone();
    tokio::spawn(async move {
        super::scheduler::Scheduler::new(scheduler_db).run().await;
    });

    // Spawn engine as background task
    let engine_db = db.clone();
    tokio::spawn(async move {
        super::engine::Engine::new(engine_db).run().await;
    });

    let schedule_svc = ScheduleServiceServer::new(state.clone());
    let workflow_svc = WorkflowServiceServer::new(state.clone());
    let worker_svc = WorkerServiceServer::new(state);

    tracing::info!("Starting Khronos gRPC server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(schedule_svc)
        .add_service(workflow_svc)
        .add_service(worker_svc)
        .serve(addr)
        .await?;

    Ok(())
}

// ─── Built-in workflow definitions ──────────────────────────────

/// Get the built-in workflow definition for a given name.
fn get_workflow_definition(name: &str) -> Vec<khronos_core::ActivityStep> {
    match name {
        "cron_job_workflow" => vec![
            khronos_core::ActivityStep {
                activity_name: "execute_cron_job".to_string(),
                args_template: BTreeMap::new(),
                retry_policy: CoreRetryPolicy { maximum_attempts: 3, initial_interval_secs: 2.0 },
                timeout_secs: 300,
                heartbeat_timeout_secs: Some(60),
            },
            khronos_core::ActivityStep {
                activity_name: "save_cron_output".to_string(),
                args_template: BTreeMap::new(),
                retry_policy: CoreRetryPolicy { maximum_attempts: 2, initial_interval_secs: 1.0 },
                timeout_secs: 60,
                heartbeat_timeout_secs: None,
            },
            khronos_core::ActivityStep {
                activity_name: "deliver_cron_result".to_string(),
                args_template: BTreeMap::new(),
                retry_policy: CoreRetryPolicy { maximum_attempts: 3, initial_interval_secs: 2.0 },
                timeout_secs: 120,
                heartbeat_timeout_secs: Some(30),
            },
            khronos_core::ActivityStep {
                activity_name: "mark_job_run_activity".to_string(),
                args_template: BTreeMap::new(),
                retry_policy: CoreRetryPolicy { maximum_attempts: 1, initial_interval_secs: 1.0 },
                timeout_secs: 30,
                heartbeat_timeout_secs: None,
            },
            khronos_core::ActivityStep {
                activity_name: "trigger_on_success_jobs".to_string(),
                args_template: BTreeMap::new(),
                retry_policy: CoreRetryPolicy { maximum_attempts: 2, initial_interval_secs: 1.0 },
                timeout_secs: 60,
                heartbeat_timeout_secs: None,
            },
        ],
        _ => {
            // Default single-step workflow for unknown names
            vec![khronos_core::ActivityStep {
                activity_name: name.to_string(),
                args_template: BTreeMap::new(),
                retry_policy: CoreRetryPolicy { maximum_attempts: 3, initial_interval_secs: 2.0 },
                timeout_secs: 300,
                heartbeat_timeout_secs: Some(60),
            }]
        }
    }
}

// ─── Proto conversion helpers ──────────────────────────────────

fn schedule_spec_to_proto(spec: &CoreScheduleSpec) -> ScheduleSpec {
    match spec {
        CoreScheduleSpec::Cron(exprs) => ScheduleSpec {
            spec_type: "cron".to_string(),
            cron_expressions: exprs.clone(),
            interval_spec: None,
        },
        CoreScheduleSpec::Interval(duration) => ScheduleSpec {
            spec_type: "interval".to_string(),
            cron_expressions: Vec::new(),
            interval_spec: Some(IntervalSpec { seconds: duration.as_secs() }),
        },
    }
}

fn workflow_action_to_proto(action: &CoreWorkflowAction) -> WorkflowAction {
    let timeouts = Timeouts {
        execution_timeout_secs: action.timeouts.execution_timeout_secs.unwrap_or(0),
        run_timeout_secs: action.timeouts.run_timeout_secs.unwrap_or(0),
        task_timeout_secs: action.timeouts.task_timeout_secs.unwrap_or(0),
    };

    WorkflowAction {
        workflow_name: action.workflow_name.clone(),
        args: action.args.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        task_queue: action.task_queue.clone(),
        id: action.id.clone(),
        timeouts: Some(timeouts),
    }
}

fn overlap_policy_to_proto(policy: &CoreOverlapPolicy) -> OverlapPolicy {
    let policy_str = match policy {
        CoreOverlapPolicy::Skip => "skip",
        CoreOverlapPolicy::Buffer(_) => "buffer",
        CoreOverlapPolicy::TerminateExisting => "terminate",
    };

    OverlapPolicy {
        policy: policy_str.to_string(),
    }
}

fn workflow_info_to_proto(wf: &WorkflowInstance) -> WorkflowInfo {
    let args: HashMap<String, String> = if let serde_json::Value::Object(map) = &wf.args_json {
        map.iter().filter_map(|(k, v)| v.as_str().map(|sv| (k.clone(), sv.to_string()))).collect()
    } else {
        HashMap::new()
    };

    WorkflowInfo {
        workflow_run_id: wf.run_id.to_string(),
        workflow_id: wf.workflow_id.clone(),
        name: wf.name.clone(),
        state: match wf.state {
            WorkflowState::Pending => "pending".to_string(),
            WorkflowState::Running => "running".to_string(),
            WorkflowState::Completed => "completed".to_string(),
            WorkflowState::Failed => "failed".to_string(),
            WorkflowState::Cancelled => "cancelled".to_string(),
        },
        args,
        result_json: wf.result_json.as_ref().map(|v| v.to_string()).unwrap_or_default(),
        started_at: wf.started_at.map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()).unwrap_or_default(),
        completed_at: wf.completed_at.map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()).unwrap_or_default(),
    }
}
