# Khronos — Lightweight Durable Workflow Orchestration Server

[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

![Khronos](assets/banner.png)

Khronos is a lightweight, durable workflow orchestration server written in Rust. It provides scheduled and ad-hoc workflow execution with built-in retry policies, heartbeat monitoring, overlap control, and SQLite-backed persistence — designed as the **workflow engine** in the Autolycus ecosystem alongside [TotalRecall](https://github.com/waym0reom3ga/TotalRecall) (memory/storage).

## Architecture

```
┌─────────────────┐       gRPC        ┌─────────────────────────────────┐
│  Lycus Gateway  │ ◄───────────────► │         Khronos Server          │
│  (or any client)│                   │                                 │
└─────────────────┘                   │  ┌──────────┐  ┌──────────┐     │
                                      │  │Scheduler │  │ Engine   │     │
                                      │  │(cron/    │  │(workflow │     │
                                      │  │ interval)│  │ execution│     │
                                      │  └────┬─────┘  └────┬─────┘     │
                                      │       │             │           │
                                      │       ▼             ▼           │
                                      │  ┌──────────────────────────┐   │
                                      │  │    SQLite Database       │   │
                                      │  │  schedules | workflows   │   │
                                      │  │  steps     | activities  │   │
                                      │  └──────────────────────────┘   │
                                      └─────────────────────────────────┘

Workers (Python/Rust) ◄── PollActivity / ReportResult ──► Khronos Server
```

## Features

- **Schedule Management** — Cron expressions and fixed-interval triggers with namespace isolation
- **Workflow Orchestration** — Multi-step workflows with ordered activity execution
- **Activity Execution Model** — Workers poll for tasks, report results/failures, send heartbeats
- **Retry Policies** — Configurable maximum attempts per step with exponential backoff
- **Heartbeat Monitoring** — Detect and fail long-running or stalled activities automatically
- **Overlap Policies** — `skip`, `buffer`, or `terminate` when schedules fire while a workflow is running
- **Durable Storage** — All state persisted to SQLite; survives server restarts
- **gRPC Interface** — Three clean services: ScheduleService, WorkflowService, WorkerService
- **Python Client Library** — High-level wrappers with generated gRPC stubs

## Quick Start

### Prerequisites

- Rust 1.75+ (stable)
- Python 3.12+ (for the client library)

### Build & Run

```bash
# Clone and build
git clone https://github.com/waym0reom3ga/khronos.git
cd khronos
cargo build --release

# Start the server (defaults to port 50051, data in ./data/)
./target/release/khronos
```

### Connect with Python Client

```bash
# Install dependencies
cd python
python -m venv .venv && source .venv/bin/activate
pip install grpcio grpcio-tools protobuf

# Use the client
from khronos_client import KhronosClient, ScheduleSpec, WorkflowAction

client = KhronosClient().connect()

# Create a cron schedule
spec = ScheduleSpec(cron_expressions=["0 */6 * * *"])  # Every 6 hours
action = WorkflowAction(
    workflow_name="data-pipeline",
    args={"source": "database"},
    task_queue="default",
)
client.create_schedule("hourly-etl", spec, action)

# Start a workflow manually
run_id = client.start_workflow("data-pipeline", {"source": "api"})
print(f"Started workflow: {run_id}")

# Check status
info = client.get_workflow(run_id)
print(f"State: {info.state}")
```

### Run a Worker

```python
import asyncio
from khronos_client import KhronosClient, KhronosWorker

async def fetch_data(source: str):
    """Example activity handler."""
    return {"status": "ok", "source": source}

client = KhronosClient().connect()
worker = KhronosWorker(
    client=client,
    task_queue="default",
    activities={"fetch_data": fetch_data},
)

asyncio.run(worker.run())
```

## API Reference

Khronos exposes three gRPC services defined in `proto/khronos.proto`:

### ScheduleService

Manage cron and interval-based schedules.

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `CreateSchedule` | `CreateScheduleRequest` | `CreateScheduleResponse` | Create a new schedule with spec, action, and overlap policy |
| `UpdateSchedule` | `UpdateScheduleRequest` | `UpdateScheduleResponse` | Update the trigger spec of an existing schedule |
| `DeleteSchedule` | `DeleteScheduleRequest` | `DeleteScheduleResponse` | Remove a schedule by ID |
| `ListSchedules` | `ListSchedulesRequest` | `ListSchedulesResponse` | List all schedules in a namespace |

### WorkflowService

Start, inspect, and cancel workflow executions.

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `StartWorkflow` | `StartWorkflowRequest` | `StartWorkflowResponse` | Start a new workflow run with args and timeouts |
| `GetWorkflow` | `GetWorkflowRequest` | `GetWorkflowResponse` | Get the current state of a workflow by run ID |
| `ListWorkflows` | `ListWorkflowsRequest` | `ListWorkflowsResponse` | List all workflows in a namespace |
| `CancelWorkflow` | `CancelWorkflowRequest` | `CancelWorkflowResponse` | Cancel a running or pending workflow |

### WorkerService

Worker-side operations for activity polling and execution.

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `PollActivity` | `PollActivityRequest` | `PollActivityResponse` | Poll for the next available activity task |
| `ReportActivityResult` | `ReportActivityResultRequest` | `ReportActivityResultResponse` | Report successful completion of an activity |
| `ReportActivityFailure` | `ReportActivityFailureRequest` | `ReportActivityFailureResponse` | Report a failed activity with error details |
| `Heartbeat` | `HeartbeatRequest` | `HeartbeatResponse` | Send a heartbeat to keep a long-running activity alive |

## Configuration

### CLI Arguments

```bash
khronos [OPTIONS]

Options:
  -p, --port <PORT>          Server port (default: 50051)
  -d, --data-dir <DATA_DIR>  Data directory for SQLite database (default: ./data)
      --log-level <LEVEL>    Log level: trace, debug, info, warn, error (default: info)
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RUST_LOG` | Override log filtering (tracing-subscriber format) | — |

## Project Structure

```
khronos/
├── Cargo.toml              # Workspace manifest
├── proto/
│   └── khronos.proto       # gRPC service definitions
├── crates/
│   ├── core/               # Domain models and types
│   │   ├── src/lib.rs      # Module exports
│   │   ├── src/schedule.rs # ScheduleSpec, OverlapPolicy, Timeouts, WorkflowAction
│   │   ├── src/workflow.rs # WorkflowDefinition, WorkflowInstance, WorkflowStepInstance
│   │   ├── src/activity.rs # ActivityState, RetryPolicy, ActivityResult, ActivityTask
│   │   └── src/task_queue.rs
│   ├── db/                 # SQLite persistence layer
│   │   ├── src/lib.rs      # Database connection and migration entry point
│   │   ├── src/schema.rs   # SQL schema (namespaces, schedules, workflows, steps, activities)
│   │   ├── src/schedules.rs# Schedule CRUD operations
│   │   ├── src/workflows.rs# Workflow CRUD operations
│   │   └── src/activities.rs# Activity state management
│   └── server/             # gRPC server and background tasks
│       ├── Cargo.toml      # Server dependencies (tonic, tokio, etc.)
│       ├── build.rs        # Compiles proto/khronos.proto via prost-build
│       ├── src/lib.rs      # Module exports + re-exports generated types
│       ├── src/main.rs     # CLI entry point (clap)
│       ├── src/grpc.rs     # gRPC service implementations (ScheduleService, WorkflowService, WorkerService)
│       ├── src/scheduler.rs# Background scheduler loop (evaluates schedules every second)
│       └── src/engine.rs   # Workflow execution engine (advances steps, retries, heartbeat checks)
├── python/
│   └── khronos_client/     # Python client library
│       ├── __init__.py     # Package exports
│       ├── client.py       # KhronosClient — high-level ScheduleService + WorkflowService wrapper
│       ├── worker.py       # KhronosWorker — activity polling loop with handler dispatch
│       ├── types.py        # Dataclasses: ScheduleSpec, WorkflowAction, OverlapPolicy, Timeouts, etc.
│       ├── khronos_pb2.py  # Generated protobuf message classes
│       └── khronos_pb2_grpc.py # Generated gRPC stubs and service interfaces
└── README.md               # This file
```

## Database Schema

Khronos uses SQLite with five core tables:

| Table | Purpose |
|-------|---------|
| `namespaces` | Logical isolation boundaries |
| `schedules` | Cron/interval schedule definitions with overlap policies |
| `workflows` | Workflow execution instances (state, args, results, timeouts) |
| `workflow_steps` | Individual steps within a workflow run (retry tracking, heartbeats) |
| `activities` | Concrete activity executions linked to steps (attempt history) |

## License

MIT
