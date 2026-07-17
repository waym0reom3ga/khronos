//! gRPC service implementations for Khronos.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use khronos_core::{OverlapPolicy as CoreOverlapPolicy, RetryPolicy as CoreRetryPolicy, Schedule, ScheduleSpec as CoreScheduleSpec, Timeouts as CoreTimeouts, WorkflowAction as CoreWorkflowAction, WorkflowInstance, WorkflowState, WorkflowStepInstance};
use khronos_db::activities::{self as db_activities};
use khronos_db::schedules::{self as db_schedules};

// Temporal WorkflowService trait
use crate::temporal::api::workflowservice::v1::workflow_service_server::WorkflowService as TemporalWorkflowService;
use khronos_db::workflows::{self as db_workflows};
use khronos_db::Database;
use tokio::sync::broadcast;
use tokio::sync::Mutex;

// Re-exported proto types from the parent module (lib.rs)
use super::*;

// Tonic-generated service traits — these live in submodules, not at top level.
use crate::schedule_service_server::{ScheduleService, ScheduleServiceServer};
use crate::workflow_service_server::{WorkflowService, WorkflowServiceServer};
use crate::worker_service_server::{WorkerService, WorkerServiceServer};
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status};

/// Shared server state.
#[derive(Clone)]
pub struct KhronosState {
    pub db: Arc<Mutex<Database>>,
    pub engine_tx: Option<broadcast::Sender<super::engine::EngineEvent>>,
}

impl KhronosState {
    pub fn new(db: Database) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
            engine_tx: None,
        }
    }

    pub fn with_engine_tx(mut self, tx: broadcast::Sender<super::engine::EngineEvent>) -> Self {
        self.engine_tx = Some(tx);
        self
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

        // Notify engine of schedule change
        if let Some(tx) = &self.engine_tx {
            let _ = tx.send(super::engine::EngineEvent::ScheduleChange);
        }

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

        // Notify engine of schedule change
        if let Some(tx) = &self.engine_tx {
            let _ = tx.send(super::engine::EngineEvent::ScheduleChange);
        }

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

        // Notify engine of schedule change
        if let Some(tx) = &self.engine_tx {
            let _ = tx.send(super::engine::EngineEvent::ScheduleChange);
        }

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

        // Notify engine that a new workflow was created
        if let Some(tx) = &self.engine_tx {
            let _ = tx.send(super::engine::EngineEvent::WorkflowCreated {
                run_id: run_id.to_string(),
            });
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

        // Get workflow_run_id for the engine event
        let workflow_run_id: String = conn.query_row(
            "SELECT workflow_run_id FROM workflow_steps WHERE id = ?1",
            [step_id.as_str()],
            |row| row.get(0),
        ).map_err(|e| Status::internal(format!("Failed to find step: {}", e)))?;

        // Notify engine that a step completed
        if let Some(tx) = &self.engine_tx {
            let _ = tx.send(super::engine::EngineEvent::StepCompleted {
                workflow_run_id,
            });
        }

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

// ─── Temporal WorkflowService ──────────────────────────────────────────────

#[async_trait]
impl TemporalWorkflowService for KhronosState {
    async fn register_namespace(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::RegisterNamespaceRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RegisterNamespaceResponse>, Status> {
        let req = request.into_inner();
        let namespace = req.namespace;

        // Create namespace in DB if it doesn't exist
        let db = self.db.lock().await;
        let conn = db.connection();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM namespaces WHERE name = ?1",
            [&namespace],
            |row| row.get(0),
        ).unwrap_or(0);

        if count == 0 {
            conn.execute(
                "INSERT INTO namespaces (name, created_at, updated_at) VALUES (?1, strftime('%s', 'now'), strftime('%s', 'now'))",
                [&namespace],
            ).map_err(|e| Status::internal(format!("Failed to create namespace: {}", e)))?;
        }

        Ok(Response::new(crate::temporal::api::workflowservice::v1::RegisterNamespaceResponse {
            ..Default::default()
        }))
    }

    async fn describe_namespace(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::DescribeNamespaceRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeNamespaceResponse>, Status> {
        let req = request.into_inner();
        let namespace = req.namespace;

        let db = self.db.lock().await;
        let conn = db.connection();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM namespaces WHERE name = ?1",
            [&namespace],
            |row| row.get(0),
        ).unwrap_or(0);

        if count == 0 {
            return Err(Status::not_found(format!("Namespace '{}' not found", namespace)));
        }

        Ok(Response::new(crate::temporal::api::workflowservice::v1::DescribeNamespaceResponse {
            namespace_info: Some(crate::temporal::api::namespace::v1::NamespaceInfo {
                name: namespace,
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    async fn get_system_info(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::GetSystemInfoRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::GetSystemInfoResponse>, Status> {
        Ok(Response::new(crate::temporal::api::workflowservice::v1::GetSystemInfoResponse {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            ..Default::default()
        }))
    }

    async fn start_workflow_execution(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::StartWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::StartWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let run_id = uuid::Uuid::new_v4();
        let namespace = if req.namespace.is_empty() { "default".to_string() } else { req.namespace };

        // Convert args to JSON
        let args_json: serde_json::Value = if let Some(input) = &req.input {
            if input.payloads.is_empty() {
                serde_json::json!({})
            } else {
                serde_json::json!(input.payloads.iter().map(|p| String::from_utf8_lossy(&p.data).to_string()).collect::<Vec<_>>())
            }
        } else {
            serde_json::json!({})
        };

        // Convert timeouts
        let timeouts = CoreTimeouts {
            execution_timeout_secs: req.workflow_execution_timeout.as_ref().map(|t| t.seconds as u64),
            run_timeout_secs: req.workflow_run_timeout.as_ref().map(|t| t.seconds as u64),
            task_timeout_secs: req.workflow_task_timeout.as_ref().map(|t| t.seconds as u64),
        };

        let workflow_name = req.workflow_type.as_ref().map(|wt| wt.name.clone()).unwrap_or_default();
        let task_queue = req.task_queue.as_ref().map(|tq| tq.name.clone()).unwrap_or_else(|| "default".to_string());

        let workflow = WorkflowInstance {
            run_id,
            workflow_id: req.workflow_id.clone(),
            name: workflow_name.clone(),
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
        db_workflows::insert_workflow(&db.connection(), &workflow)
            .map_err(|e| Status::internal(format!("Failed to create workflow: {}", e)))?;

        // Create workflow steps
        let steps = get_workflow_definition(&workflow_name);
        for (index, step_def) in steps.iter().enumerate() {
            let step = WorkflowStepInstance {
                id: uuid::Uuid::new_v4(),
                workflow_run_id: run_id,
                step_index: index,
                activity_name: step_def.activity_name.clone(),
                args_json: args_json.clone(),
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

        Ok(Response::new(crate::temporal::api::workflowservice::v1::StartWorkflowExecutionResponse {
            run_id: run_id.to_string(),
            started: true,
            ..Default::default()
        }))
    }

    async fn poll_activity_task_queue(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::PollActivityTaskQueueRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse>, Status> {
        let req = request.into_inner();
        let task_queue = req.task_queue.as_ref().map(|tq| tq.name.clone()).unwrap_or_default();

        // Find pending steps that match the task queue
        let db = self.db.lock().await;
        let conn = db.connection();

        let pending_steps = db_activities::get_pending_steps(&conn)
            .map_err(|e| Status::internal(format!("Failed to get pending steps: {}", e)))?;

        for step in &pending_steps {
            // Check if this step belongs to a workflow in the same task queue
            let wf_queue: String = conn.query_row(
                "SELECT task_queue FROM workflows WHERE run_id = ?1",
                [&step.workflow_run_id.to_string()],
                |row| row.get(0),
            ).unwrap_or_default();

            if wf_queue == task_queue {
                let activity_id = uuid::Uuid::new_v4();
                let new_attempt = step.attempt + 1;

                db_activities::update_step_state(
                    &conn,
                    &step.id.to_string(),
                    WorkflowState::Running,
                    Some(new_attempt),
                    None,
                    None,
                ).map_err(|e| Status::internal(format!("Failed to update step state: {}", e)))?;

                db_activities::insert_activity_attempt(
                    &conn,
                    &activity_id.to_string(),
                    &step.id.to_string(),
                    new_attempt,
                ).map_err(|e| Status::internal(format!("Failed to insert activity: {}", e)))?;

                db_workflows::update_workflow_state(
                    &conn,
                    &step.workflow_run_id.to_string(),
                    WorkflowState::Running,
                    None,
                    Some(Utc::now()),
                ).ok();

                // Convert args to proto format with proper JSON encoding metadata.
                // args_json stores the job_id as a plain string for cron_job_workflow steps.
                let args: Vec<crate::temporal::api::common::v1::Payload> = if step.args_json.is_null()
                    || matches!(&step.args_json, serde_json::Value::String(s) if s.is_empty()) {
                    vec![]
                } else if let serde_json::Value::String(job_id) = &step.args_json {
                    // Single positional arg: job_id
                    let mut metadata = HashMap::new();
                    metadata.insert("encoding".to_string(), "json/plain".to_string().into_bytes());
                    vec![crate::temporal::api::common::v1::Payload {
                        metadata,
                        data: serde_json::to_string(job_id).unwrap_or_else(|_| "null".to_string()).into_bytes(),
                        external_payloads: vec![],
                    }]
                } else {
                    vec![]
                };

                // Look up the actual namespace from the workflow
                let wf_namespace: String = conn.query_row(
                    "SELECT namespace FROM workflows WHERE run_id = ?1",
                    [&step.workflow_run_id.to_string()],
                    |row| row.get(0),
                ).unwrap_or_else(|_| "default".to_string());

                return Ok(Response::new(crate::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse {
                    task_token: activity_id.to_string().into_bytes(),
                    workflow_namespace: wf_namespace,
                    workflow_execution: Some(crate::temporal::api::common::v1::WorkflowExecution {
                        workflow_id: step.workflow_run_id.to_string(),
                        run_id: step.workflow_run_id.to_string(),
                    }),
                    activity_id: step.activity_name.clone(),
                    activity_type: Some(crate::temporal::api::common::v1::ActivityType {
                        name: step.activity_name.clone(),
                    }),
                    header: None,
                    input: Some(crate::temporal::api::common::v1::Payloads { payloads: args }),
                    heartbeat_timeout: Some(prost_types::Duration {
                        seconds: step.heartbeat_timeout_secs.unwrap_or(30) as i64,
                        nanos: 0,
                    }),
                    schedule_to_close_timeout: Some(prost_types::Duration {
                        seconds: step.timeout_secs as i64,
                        nanos: 0,
                    }),
                    start_to_close_timeout: Some(prost_types::Duration {
                        seconds: step.timeout_secs as i64,
                        nanos: 0,
                    }),
                    ..Default::default()
                }));
            }
        }

        // No task found
        Ok(Response::new(crate::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse {
            ..Default::default()
        }))
    }

    async fn respond_activity_task_completed(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::RespondActivityTaskCompletedRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondActivityTaskCompletedResponse>, Status> {
        let req = request.into_inner();
        let activity_id = String::from_utf8_lossy(&req.task_token).to_string();

        let db = self.db.lock().await;
        let conn = db.connection();

        // Parse result
        let result_json: serde_json::Value = if let Some(result) = req.result {
            if let Some(payload) = result.payloads.first() {
                serde_json::from_slice(&payload.data).unwrap_or(serde_json::json!(String::from_utf8_lossy(&payload.data)))
            } else {
                serde_json::json!({})
            }
        } else {
            serde_json::json!({})
        };

        db_activities::update_activity_state(
            &conn,
            &activity_id,
            khronos_core::ActivityState::Completed,
            Some(&result_json),
            None,
        ).map_err(|e| Status::internal(format!("Failed to update activity: {}", e)))?;

        let step_id: String = conn.query_row(
            "SELECT step_id FROM activities WHERE id = ?1",
            [&activity_id],
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

        Ok(Response::new(crate::temporal::api::workflowservice::v1::RespondActivityTaskCompletedResponse {
            ..Default::default()
        }))
    }

    async fn respond_activity_task_failed(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::RespondActivityTaskFailedRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondActivityTaskFailedResponse>, Status> {
        let req = request.into_inner();
        let activity_id = String::from_utf8_lossy(&req.task_token).to_string();

        let db = self.db.lock().await;
        let conn = db.connection();

        let (step_id, attempt): (String, u32) = conn.query_row(
            "SELECT a.step_id, a.attempt FROM activities a WHERE a.id = ?1",
            [&activity_id],
            |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u32)),
        ).map_err(|e| Status::internal(format!("Failed to find activity: {}", e)))?;

        let retry_policy_json: String = conn.query_row(
            "SELECT retry_policy_json FROM workflow_steps WHERE id = ?1",
            [&step_id],
            |row| row.get(0),
        ).map_err(|e| Status::internal(format!("Failed to get retry policy: {}", e)))?;

        let retry_policy: CoreRetryPolicy = serde_json::from_str(&retry_policy_json)
            .map_err(|e| Status::internal(format!("Invalid retry policy JSON: {}", e)))?;

        let error_message = req.failure.as_ref().map(|f| f.message.clone()).unwrap_or_default();

        db_activities::update_activity_state(
            &conn,
            &activity_id,
            khronos_core::ActivityState::Failed,
            None,
            Some(&error_message),
        ).map_err(|e| Status::internal(format!("Failed to update activity: {}", e)))?;

        let current_attempt = attempt + 1;
        if current_attempt < retry_policy.maximum_attempts {
            let backoff_secs = retry_policy.initial_interval_secs * (2.0_f64.powi((current_attempt - 1) as i32));
            let next_retry_at = Utc::now() + chrono::Duration::seconds(backoff_secs as i64);

            db_activities::update_step_state(
                &conn,
                &step_id,
                WorkflowState::Pending,
                Some(current_attempt),
                None,
                Some(next_retry_at),
            ).map_err(|e| Status::internal(format!("Failed to schedule retry: {}", e)))?;
        } else {
            db_activities::update_step_state(
                &conn,
                &step_id,
                WorkflowState::Failed,
                Some(current_attempt),
                None,
                None,
            ).map_err(|e| Status::internal(format!("Failed to mark step as failed: {}", e)))?;
        }

        Ok(Response::new(crate::temporal::api::workflowservice::v1::RespondActivityTaskFailedResponse {
            ..Default::default()
        }))
    }

    // Stub implementations for required RPCs (not used by autolycus but required by trait)
    async fn get_workflow_execution_history(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn poll_workflow_task_queue(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::PollWorkflowTaskQueueRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn respond_workflow_task_completed(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RespondWorkflowTaskCompletedRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondWorkflowTaskCompletedResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn respond_workflow_task_failed(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RespondWorkflowTaskFailedRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondWorkflowTaskFailedResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn create_schedule(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::CreateScheduleRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::CreateScheduleResponse>, Status> {
        let req = request.into_inner();
        let schedule_id = req.schedule_id.clone();
        let namespace = if req.namespace.is_empty() { "default".to_string() } else { req.namespace };

        let schedule = req.schedule.ok_or_else(|| Status::invalid_argument("schedule is required"))?;
        let spec = schedule.spec.ok_or_else(|| Status::invalid_argument("schedule spec is required"))?;
        let action = schedule.action.ok_or_else(|| Status::invalid_argument("schedule action is required"))?;

        // Parse spec type and data from Temporal ScheduleSpec
        let (spec_type, cron_expressions, interval_seconds) = if !spec.cron_string.is_empty() {
            let cron_exprs = spec.cron_string.join(",");
            (String::from("cron"), cron_exprs, 0)
        } else if !spec.interval.is_empty() {
            let secs = spec.interval.first()
                .and_then(|iv| iv.interval)
                .map(|d| d.seconds)
                .unwrap_or(0);
            (String::from("interval"), String::new(), secs)
        } else {
            return Err(Status::invalid_argument("schedule spec must have cron_string or interval"));
        };

        // Parse action from Temporal ScheduleAction (only start_workflow is supported)
        let (workflow_name, task_queue, action_id, action_args_json) = match action.action {
            Some(crate::temporal::api::schedule::v1::schedule_action::Action::StartWorkflow(sw)) => {
                let wf_name = sw.workflow_type
                    .map(|wt| wt.name)
                    .unwrap_or_default();
                let tq = sw.task_queue
                    .map(|tq| tq.name)
                    .unwrap_or_default();
                let id = sw.workflow_id;
                // Store job_id as plain string for activity arg passthrough.
                // For lycus-cron-* schedules, extract job_id from schedule_id.
                let args_json = if let Some(payloads) = sw.input {
                    // Take first payload as job_id
                    payloads.payloads.first()
                        .map(|p| String::from_utf8_lossy(&p.data).to_string())
                        .unwrap_or_default()
                } else if schedule_id.starts_with("lycus-cron-") {
                    schedule_id.strip_prefix("lycus-cron-")
                        .unwrap_or(&schedule_id)
                        .to_string()
                } else {
                    String::new()
                };
                (wf_name, tq, id, args_json)
            }
            None => {
                return Err(Status::invalid_argument("schedule action start_workflow is required"));
            }
        };

        // Parse overlap policy from Temporal SchedulePolicies
        let overlap_policy = match schedule.policies.map(|p| p.overlap_policy) {
            Some(0) | None => "skip".to_string(),
            Some(1) => "skip".to_string(),
            Some(2) | Some(3) => "buffer".to_string(),
            Some(4) | Some(5) => "terminate".to_string(),
            _ => "skip".to_string(),
        };

        // Create schedule in DB
        let db = self.db.lock().await;
        let conn = db.connection();

        conn.execute(
            "INSERT INTO schedules (schedule_id, namespace, spec_type, cron_expressions, interval_seconds, \
             overlap_policy, action_workflow_name, action_task_queue, action_args_json, action_id, timeouts_json, \
             created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, datetime('now'), datetime('now'))",
            rusqlite::params![
                schedule_id,
                namespace,
                spec_type,
                cron_expressions,
                interval_seconds,
                overlap_policy,
                workflow_name,
                task_queue,
                action_args_json,
                action_id,
                "{}",
            ],
        ).map_err(|e| Status::internal(format!("Failed to create schedule: {}", e)))?;

        Ok(Response::new(crate::temporal::api::workflowservice::v1::CreateScheduleResponse {
            ..Default::default()
        }))
    }

    async fn update_schedule(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::UpdateScheduleRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateScheduleResponse>, Status> {
        let req = request.into_inner();
        let schedule_id = req.schedule_id.clone();
        let namespace = if req.namespace.is_empty() { "default".to_string() } else { req.namespace };

        // Verify schedule exists
        let db = self.db.lock().await;
        let conn = db.connection();
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM schedules WHERE schedule_id = ?1 AND namespace = ?2)",
            rusqlite::params![schedule_id.clone(), namespace.clone()],
            |row| row.get(0),
        ).map_err(|e| Status::internal(format!("Failed to check schedule: {}", e)))?;
        if !exists {
            return Err(Status::not_found("Schedule not found"));
        }

        // Parse update input — Temporal UpdateScheduleRequest has `schedule` directly
        let new_schedule = req.schedule.ok_or_else(|| Status::invalid_argument("schedule is required"))?;

        let mut sets = Vec::new();
        sets.push("updated_at = datetime('now')".to_string());

        // Update spec if provided
        if let Some(spec) = new_schedule.spec {
            if !spec.cron_string.is_empty() {
                sets.push(format!("spec_type = 'cron', cron_expressions = '{}'", spec.cron_string.join(",")));
            } else if !spec.interval.is_empty() {
                let secs = spec.interval.first()
                    .and_then(|iv| iv.interval)
                    .map(|d| d.seconds)
                    .unwrap_or(0);
                sets.push(format!("spec_type = 'interval', interval_seconds = {}", secs));
            }
        }

        // Update action if provided
        if let Some(action) = new_schedule.action {
            if let Some(crate::temporal::api::schedule::v1::schedule_action::Action::StartWorkflow(sw)) = action.action {
                if let Some(wt) = sw.workflow_type {
                    sets.push(format!("action_workflow_name = '{}'", wt.name));
                }
                if let Some(tq) = sw.task_queue {
                    sets.push(format!("action_task_queue = '{}'", tq.name));
                }
                if !sw.workflow_id.is_empty() {
                    sets.push(format!("action_id = '{}'", sw.workflow_id));
                }
            }
        }

        // Update overlap policy if provided
        if let Some(policies) = new_schedule.policies {
            let policy = match policies.overlap_policy {
                0 | 1 => "skip",
                2 | 3 => "buffer",
                 4 | 5 => "terminate",
                _ => "skip",
            };
            sets.push(format!("overlap_policy = '{}'", policy));
        }

        let set_clause = sets.join(", ");
        let sql = format!(
            "UPDATE schedules SET {} WHERE schedule_id = ?1 AND namespace = ?2",
            set_clause
        );

        conn.execute(
            &sql,
            rusqlite::params![schedule_id, namespace],
        ).map_err(|e| Status::internal(format!("Failed to update schedule: {}", e)))?;

        Ok(Response::new(crate::temporal::api::workflowservice::v1::UpdateScheduleResponse {
            ..Default::default()
        }))
    }

    async fn describe_schedule(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::DescribeScheduleRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeScheduleResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() { "default".to_string() } else { req.namespace };

        let db = self.db.lock().await;
        let conn = db.connection();

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM schedules WHERE schedule_id = ?1 AND namespace = ?2",
            rusqlite::params![req.schedule_id, namespace],
            |row| row.get(0),
        ).map_err(|e| Status::internal(format!("Failed to describe schedule: {}", e)))?;

        if count == 0 {
            return Err(Status::not_found("Schedule not found"));
        }

        let row = conn.query_row(
            "SELECT spec_type, overlap_policy, cron_expressions, interval_seconds, \
             action_workflow_name, action_task_queue, action_args_json, action_id, timeouts_json \
             FROM schedules WHERE schedule_id = ?1 AND namespace = ?2",
            rusqlite::params![req.schedule_id, namespace],
            |row| Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
            )),
        ).map_err(|e| Status::internal(format!("Failed to describe schedule: {}", e)));

        let (spec_type, overlap_policy, cron_expressions, interval_seconds, wf_name, task_queue, _args_json, action_id, _timeouts_json) = row?;

        // Build Temporal ScheduleSpec
        let spec = if spec_type == "cron" {
            let exprs: Vec<String> = cron_expressions
                .and_then(|j| serde_json::from_str(&j).ok())
                .unwrap_or_default();
            crate::temporal::api::schedule::v1::ScheduleSpec {
                cron_string: exprs,
                ..Default::default()
            }
        } else if spec_type == "interval" {
            let secs = interval_seconds.unwrap_or(0);
            crate::temporal::api::schedule::v1::ScheduleSpec {
                interval: if secs > 0 {
                    vec![crate::temporal::api::schedule::v1::IntervalSpec {
                        interval: Some(prost_types::Duration { seconds: secs, nanos: 0 }),
                        ..Default::default()
                    }]
                } else {
                    vec![]
                },
                ..Default::default()
            }
        } else {
            Default::default()
        };

        // Build Temporal ScheduleAction
        let action = crate::temporal::api::schedule::v1::schedule_action::Action::StartWorkflow(
            crate::temporal::api::workflow::v1::NewWorkflowExecutionInfo {
                workflow_type: Some(crate::temporal::api::common::v1::WorkflowType {
                    name: wf_name,
                }),
                task_queue: Some(crate::temporal::api::taskqueue::v1::TaskQueue {
                    name: task_queue,
                    kind: 0,
                    normal_name: String::new(),
                }),
                workflow_id: action_id.unwrap_or_default(),
                ..Default::default()
            }
        );

        // Build overlap policy
        let overlap_policy_val = match overlap_policy.as_str() {
            "skip" => 1,
            "buffer" => 2,
            "terminate" => 4,
            _ => 1,
        };

        let schedule = crate::temporal::api::schedule::v1::Schedule {
            spec: Some(spec),
            action: Some(crate::temporal::api::schedule::v1::ScheduleAction {
                action: Some(action),
            }),
            policies: Some(crate::temporal::api::schedule::v1::SchedulePolicies {
                overlap_policy: overlap_policy_val,
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(Response::new(crate::temporal::api::workflowservice::v1::DescribeScheduleResponse {
            schedule: Some(schedule),
            ..Default::default()
        }))
    }

    async fn patch_schedule(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::PatchScheduleRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::PatchScheduleResponse>, Status> {
        let _req = request.into_inner();
        // Stub for now
        Ok(Response::new(Default::default()))
    }

    async fn list_schedules(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListSchedulesRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListSchedulesResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn delete_schedule(
        &self,
        request: Request<crate::temporal::api::workflowservice::v1::DeleteScheduleRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DeleteScheduleResponse>, Status> {
        let req = request.into_inner();
        let schedule_id = req.schedule_id;
        let namespace = if req.namespace.is_empty() { "default".to_string() } else { req.namespace };

        let db = self.db.lock().await;
        let conn = db.connection();

        conn.execute(
            "DELETE FROM schedules WHERE schedule_id = ?1 AND namespace = ?2",
            rusqlite::params![schedule_id, namespace],
        ).map_err(|e| Status::internal(format!("Failed to delete schedule: {}", e)))?;

        Ok(Response::new(crate::temporal::api::workflowservice::v1::DeleteScheduleResponse {
            ..Default::default()
        }))
    }

    async fn describe_workflow_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DescribeWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeWorkflowExecutionResponse>, Status> {
        // Stub for now - returns empty response
        Ok(Response::new(Default::default()))
    }

    // ─── Stub implementations for remaining required RPCs ───────────

    // Namespace management
    async fn list_namespaces(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListNamespacesRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListNamespacesResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn update_namespace(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateNamespaceRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateNamespaceResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn deprecate_namespace(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DeprecateNamespaceRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DeprecateNamespaceResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Workflow execution
    async fn execute_multi_operation(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ExecuteMultiOperationRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ExecuteMultiOperationResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn get_workflow_execution_history_reverse(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryReverseRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryReverseResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn request_cancel_workflow_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RequestCancelWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RequestCancelWorkflowExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn signal_workflow_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::SignalWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::SignalWorkflowExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn signal_with_start_workflow_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::SignalWithStartWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::SignalWithStartWorkflowExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn reset_workflow_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ResetWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ResetWorkflowExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn terminate_workflow_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::TerminateWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::TerminateWorkflowExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn delete_workflow_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DeleteWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DeleteWorkflowExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_open_workflow_executions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListOpenWorkflowExecutionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListOpenWorkflowExecutionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_closed_workflow_executions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListClosedWorkflowExecutionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListClosedWorkflowExecutionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_workflow_executions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListWorkflowExecutionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListWorkflowExecutionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_archived_workflow_executions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListArchivedWorkflowExecutionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListArchivedWorkflowExecutionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn scan_workflow_executions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ScanWorkflowExecutionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ScanWorkflowExecutionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn count_workflow_executions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::CountWorkflowExecutionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::CountWorkflowExecutionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Activity task
    async fn record_activity_task_heartbeat(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn record_activity_task_heartbeat_by_id(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatByIdRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatByIdResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn respond_activity_task_completed_by_id(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RespondActivityTaskCompletedByIdRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondActivityTaskCompletedByIdResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn respond_activity_task_failed_by_id(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RespondActivityTaskFailedByIdRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondActivityTaskFailedByIdResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn respond_activity_task_canceled(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RespondActivityTaskCanceledRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondActivityTaskCanceledResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn respond_activity_task_canceled_by_id(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RespondActivityTaskCanceledByIdRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondActivityTaskCanceledByIdResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Query task
    async fn respond_query_task_completed(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RespondQueryTaskCompletedRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondQueryTaskCompletedResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Task queue
    async fn reset_sticky_task_queue(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ResetStickyTaskQueueRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ResetStickyTaskQueueResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn describe_task_queue(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DescribeTaskQueueRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeTaskQueueResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_task_queue_partitions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListTaskQueuePartitionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListTaskQueuePartitionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Cluster info
    async fn get_cluster_info(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::GetClusterInfoRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::GetClusterInfoResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Search attributes
    async fn get_search_attributes(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::GetSearchAttributesRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::GetSearchAttributesResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Schedule
    async fn list_schedule_matching_times(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListScheduleMatchingTimesRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListScheduleMatchingTimesResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn count_schedules(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::CountSchedulesRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::CountSchedulesResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Worker deployment
    async fn update_worker_build_id_compatibility(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateWorkerBuildIdCompatibilityRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateWorkerBuildIdCompatibilityResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn get_worker_build_id_compatibility(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::GetWorkerBuildIdCompatibilityRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::GetWorkerBuildIdCompatibilityResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn update_worker_versioning_rules(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateWorkerVersioningRulesRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateWorkerVersioningRulesResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn get_worker_versioning_rules(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::GetWorkerVersioningRulesRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::GetWorkerVersioningRulesResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn get_worker_task_reachability(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::GetWorkerTaskReachabilityRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::GetWorkerTaskReachabilityResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn describe_deployment(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DescribeDeploymentRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeDeploymentResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn describe_worker_deployment_version(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DescribeWorkerDeploymentVersionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeWorkerDeploymentVersionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_deployments(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListDeploymentsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListDeploymentsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn get_deployment_reachability(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::GetDeploymentReachabilityRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::GetDeploymentReachabilityResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn get_current_deployment(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::GetCurrentDeploymentRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::GetCurrentDeploymentResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn set_current_deployment(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::SetCurrentDeploymentRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::SetCurrentDeploymentResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn set_worker_deployment_current_version(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::SetWorkerDeploymentCurrentVersionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::SetWorkerDeploymentCurrentVersionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn describe_worker_deployment(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DescribeWorkerDeploymentRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeWorkerDeploymentResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn delete_worker_deployment(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DeleteWorkerDeploymentRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DeleteWorkerDeploymentResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn delete_worker_deployment_version(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DeleteWorkerDeploymentVersionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DeleteWorkerDeploymentVersionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn set_worker_deployment_ramping_version(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::SetWorkerDeploymentRampingVersionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::SetWorkerDeploymentRampingVersionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_worker_deployments(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListWorkerDeploymentsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListWorkerDeploymentsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn create_worker_deployment(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::CreateWorkerDeploymentRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::CreateWorkerDeploymentResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn create_worker_deployment_version(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::CreateWorkerDeploymentVersionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::CreateWorkerDeploymentVersionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn update_worker_deployment_version_compute_config(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateWorkerDeploymentVersionComputeConfigRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateWorkerDeploymentVersionComputeConfigResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn validate_worker_deployment_version_compute_config(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ValidateWorkerDeploymentVersionComputeConfigRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ValidateWorkerDeploymentVersionComputeConfigResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn update_worker_deployment_version_metadata(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateWorkerDeploymentVersionMetadataRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateWorkerDeploymentVersionMetadataResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn set_worker_deployment_manager(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::SetWorkerDeploymentManagerRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::SetWorkerDeploymentManagerResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Workflow execution update
    async fn update_workflow_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateWorkflowExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn poll_workflow_execution_update(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::PollWorkflowExecutionUpdateRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::PollWorkflowExecutionUpdateResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Batch operation
    async fn start_batch_operation(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::StartBatchOperationRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::StartBatchOperationResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn stop_batch_operation(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::StopBatchOperationRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::StopBatchOperationResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn describe_batch_operation(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DescribeBatchOperationRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeBatchOperationResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_batch_operations(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListBatchOperationsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListBatchOperationsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Nexus
    async fn poll_nexus_task_queue(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::PollNexusTaskQueueRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::PollNexusTaskQueueResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn respond_nexus_task_completed(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RespondNexusTaskCompletedRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondNexusTaskCompletedResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn respond_nexus_task_failed(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RespondNexusTaskFailedRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RespondNexusTaskFailedResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Activity options
    async fn update_activity_options(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateActivityOptionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateActivityOptionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn update_workflow_execution_options(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateWorkflowExecutionOptionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateWorkflowExecutionOptionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Activity pause/unpause
    async fn pause_activity(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::PauseActivityRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::PauseActivityResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn unpause_activity(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UnpauseActivityRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UnpauseActivityResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn reset_activity(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ResetActivityRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ResetActivityResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Workflow rules
    async fn create_workflow_rule(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::CreateWorkflowRuleRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::CreateWorkflowRuleResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn describe_workflow_rule(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DescribeWorkflowRuleRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeWorkflowRuleResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn delete_workflow_rule(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DeleteWorkflowRuleRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DeleteWorkflowRuleResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_workflow_rules(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListWorkflowRulesRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListWorkflowRulesResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn trigger_workflow_rule(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::TriggerWorkflowRuleRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::TriggerWorkflowRuleResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Worker
    async fn record_worker_heartbeat(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RecordWorkerHeartbeatRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RecordWorkerHeartbeatResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_workers(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListWorkersRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListWorkersResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn update_task_queue_config(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateTaskQueueConfigRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateTaskQueueConfigResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn fetch_worker_config(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::FetchWorkerConfigRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::FetchWorkerConfigResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn update_worker_config(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateWorkerConfigRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateWorkerConfigResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn describe_worker(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DescribeWorkerRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeWorkerResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Workflow pause/unpause
    async fn pause_workflow_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::PauseWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::PauseWorkflowExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn unpause_workflow_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UnpauseWorkflowExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UnpauseWorkflowExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Activity execution
    async fn start_activity_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::StartActivityExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::StartActivityExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Nexus operation
    async fn start_nexus_operation_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::StartNexusOperationExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::StartNexusOperationExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn describe_activity_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DescribeActivityExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeActivityExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn describe_nexus_operation_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DescribeNexusOperationExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DescribeNexusOperationExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn poll_activity_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::PollActivityExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::PollActivityExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn poll_nexus_operation_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::PollNexusOperationExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::PollNexusOperationExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_activity_executions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListActivityExecutionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListActivityExecutionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn list_nexus_operation_executions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ListNexusOperationExecutionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ListNexusOperationExecutionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn count_activity_executions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::CountActivityExecutionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::CountActivityExecutionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn count_nexus_operation_executions(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::CountNexusOperationExecutionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::CountNexusOperationExecutionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn request_cancel_activity_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RequestCancelActivityExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RequestCancelActivityExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn request_cancel_nexus_operation_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::RequestCancelNexusOperationExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::RequestCancelNexusOperationExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn terminate_activity_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::TerminateActivityExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::TerminateActivityExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn delete_activity_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DeleteActivityExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DeleteActivityExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn pause_activity_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::PauseActivityExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::PauseActivityExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn reset_activity_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ResetActivityExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ResetActivityExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn unpause_activity_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UnpauseActivityExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UnpauseActivityExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn update_activity_execution_options(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::UpdateActivityExecutionOptionsRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::UpdateActivityExecutionOptionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn terminate_nexus_operation_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::TerminateNexusOperationExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::TerminateNexusOperationExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    async fn delete_nexus_operation_execution(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::DeleteNexusOperationExecutionRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::DeleteNexusOperationExecutionResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Worker shutdown
    async fn shutdown_worker(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::ShutdownWorkerRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::ShutdownWorkerResponse>, Status> {
        Ok(Response::new(Default::default()))
    }

    // Query workflow
    async fn query_workflow(
        &self,
        _request: Request<crate::temporal::api::workflowservice::v1::QueryWorkflowRequest>,
    ) -> Result<Response<crate::temporal::api::workflowservice::v1::QueryWorkflowResponse>, Status> {
        Ok(Response::new(Default::default()))
    }
}

// ─── Server runner ──────────────────────────────────────────────

/// Start the gRPC server, spawning scheduler and engine as background tasks.
pub async fn run_server(
    addr: std::net::SocketAddr,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create engine first so we can get its sender for KhronosState
    let engine = super::engine::Engine::new(db.clone());
    let engine_tx = engine.sender();

    let state = KhronosState::new(db.clone()).with_engine_tx(engine_tx);

    // Spawn scheduler as background task
    let scheduler_db = db.clone();
    tokio::spawn(async move {
        super::scheduler::Scheduler::new(scheduler_db).run().await;
    });

    // Spawn engine as background task
    tokio::spawn(async move {
        engine.run().await;
    });

    let schedule_svc = ScheduleServiceServer::new(state.clone())
        .accept_compressed(CompressionEncoding::Gzip);
    let workflow_svc = WorkflowServiceServer::new(state.clone())
        .accept_compressed(CompressionEncoding::Gzip);
    let worker_svc = WorkerServiceServer::new(state.clone())
        .accept_compressed(CompressionEncoding::Gzip);

    // Add Temporal WorkflowService
    use crate::temporal::api::workflowservice::v1::workflow_service_server::WorkflowServiceServer as TemporalWorkflowServiceServer;
    let temporal_svc = TemporalWorkflowServiceServer::new(state)
        .accept_compressed(CompressionEncoding::Gzip);

    tracing::info!("Starting Khronos gRPC server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(schedule_svc)
        .add_service(workflow_svc)
        .add_service(worker_svc)
        .add_service(temporal_svc)
        .serve(addr)
        .await?;

    Ok(())
}

// ─── Built-in workflow definitions ──────────────────────────────

/// Get the built-in workflow definition for a given name.
fn get_workflow_definition(name: &str) -> Vec<khronos_core::ActivityStep> {
    // Normalize: lowercase and insert underscores before capitals (camelCase → snake_case)
    let mut normalized = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            normalized.push('_');
        }
        normalized.push(c.to_lowercase().next().unwrap_or(c));
    }
    match normalized.as_str() {
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
