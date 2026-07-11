# Khronos Worker 架构设计文档

## 一、核心概念

Khronos Worker 是一个**活动执行器**。它不运行工作流逻辑，只负责：
1. 向服务器请求活动任务（Poll）
2. 执行活动（脚本/命令）
3. 报告结果或失败给服务器

参考 Temporal.io SDK 的架构模式：**Worker → Poller → TaskManager → ActivityRegistry**。

---

## 二、模块划分（黑盒抽象）

```
crates/worker/src/
├── main.rs          # 入口：启动 Worker，运行事件循环
├── worker.rs        # [黑盒] Worker 结构体 — 协调所有组件
├── poller.rs        # [黑盒] ActivityPoller — 长轮询服务器获取任务
├── executor.rs      # [黑盒] TaskExecutor — 并发执行活动，管理生命周期
├── registry.rs      # [黑盒] ActivityRegistry — 注册表：活动名 → 处理器
├── handler.rs       # [黑盒] ActivityHandler trait + 实现（脚本/Python）
└── client.rs        # [黑盒] gRPCClient — tonic stub 封装
```

### 各模块职责边界

| 模块 | 输入 | 输出 | 不关心 |
|------|------|------|--------|
| `main` | CLI参数 | Worker启动/停止 | 内部逻辑 |
| `worker` | 配置、组件引用 | 协调运行循环 | gRPC细节 |
| `poller` | gRPC客户端、队列名 | 任务流 (Stream) | 任务执行 |
| `executor` | 任务、注册表 | 并发执行、报告结果 | 如何获取任务 |
| `registry` | 活动名+处理器 | 查找处理器 | 执行细节 |
| `handler` | ActivityTask | 执行结果字符串 | gRPC/网络 |
| `client` | 请求对象 | gRPC响应 | 业务逻辑 |

---

## 三、数据流（伪代码）

### 3.1 Worker 启动流程

```
main() {
    // 1. 解析配置
    config = parse_args()          // server_url, task_queue, max_concurrent
    
    // 2. 创建 gRPC 客户端
    client = GrpcClient::new(config.server_url)
    
    // 3. 创建活动注册表并注册处理器
    registry = ActivityRegistry::new()
    registry.register("lycus-memory-condenser", PythonHandler(...))
    registry.register("lycus-cron-notifier", ScriptHandler(...))
    ...
    
    // 4. 创建轮询器
    poller = ActivityPoller::new(client, config.task_queue)
    
    // 5. 创建执行器
    executor = TaskExecutor::new(
        client,           // 用于报告结果
        registry,         // 用于查找处理器
        config.max_concurrent  // 并发限制
    )
    
    // 6. 组装 Worker 并运行
    worker = Worker::new(poller, executor)
    worker.run(shutdown_signal).await
}
```

### 3.2 Worker 运行循环（核心）

```
Worker {
    poller: ActivityPoller,
    executor: TaskExecutor,
    
    async fn run(&self, shutdown: CancellationToken) {
        // 从轮询器获取任务流
        let mut task_stream = self.poller.stream();
        
        loop {
            select! {
                // 收到关闭信号 → 优雅退出
                _ = shutdown.cancelled() => {
                    info!("shutdown signal received");
                    break;
                }
                
                // 收到新任务 → 交给执行器
                Some(task) = task_stream.next() => {
                    self.executor.execute_task(task).await;
                }
            }
        }
        
        // 等待所有进行中的活动完成
        self.executor.wait_pending().await;
    }
}
```

### 3.3 ActivityPoller（轮询器）

```
ActivityPoller {
    client: GrpcClient,
    task_queue: String,
    
    /// 单次轮询 — 阻塞直到有任务或超时
    async fn poll(&self) -> Option<ActivityTask> {
        let request = PollActivityRequest {
            task_queue: self.task_queue.clone(),
            activity_types: vec![],  // 空表示接受所有类型
        };
        
        match self.client.poll_activity(request).await {
            Ok(Some(task)) => Some(task),
            Ok(None)       => None,           // 空响应，重试
            Err(e)         => {               // 连接错误，短暂等待后重试
                warn!(error = %e, "poll failed");
                sleep(Duration::from_secs(1)).await;
                None
            }
        }
    }
    
    /// 将轮询包装为异步流
    fn stream(&self) -> impl Stream<Item = ActivityTask> {
        async_stream! {
            loop {
                match self.poll().await {
                    Some(task) => yield task,
                    None       => {}  // 继续循环重试
                }
            }
        }
    }
}
```

### 3.4 TaskExecutor（执行器）

```
TaskExecutor {
    client: GrpcClient,
    registry: Arc<ActivityRegistry>,
    semaphore: Semaphore,           // 并发控制
    pending_tasks: HashSet<String>, // 进行中的任务ID
    
    async fn execute_task(&self, task: ActivityTask) {
        // 1. 获取并发槽位（阻塞直到有空闲）
        let permit = self.semaphore.acquire().await;
        
        // 2. 从注册表查找处理器
        let handler = match self.registry.get(&task.name) {
            Some(h) => h,
            None => {
                warn!(activity = %task.name, "no handler registered");
                self.client.report_failure(
                    &task.activity_id,
                    "No handler registered for this activity"
                ).await;
                return;
            }
        };
        
        // 3. 在独立任务中执行（不阻塞轮询循环）
        let client = self.client.clone();
        let activity_id = task.activity_id.clone();
        
        tokio::spawn(async move {
            match handler.execute(&task).await {
                Ok(result) => {
                    info!(activity_id = %activity_id, "completed");
                    client.report_result(&activity_id, &result).await;
                }
                Err(e) => {
                    error!(activity_id = %activity_id, error = %e, "failed");
                    client.report_failure(&activity_id, &e.to_string()).await;
                }
            };
            
            drop(permit);  // 释放槽位
        });
    }
    
    async fn wait_pending(&self) {
        // 等待所有 spawn 的任务完成（通过检查 pending_tasks）
    }
}
```

### 3.5 ActivityRegistry（注册表）

```
ActivityRegistry {
    handlers: HashMap<String, Box<dyn ActivityHandler>>,
    
    fn register(&mut self, name: &str, handler: impl ActivityHandler) {
        self.handlers.insert(name.to_string(), Box::new(handler));
    }
    
    fn get(&self, name: &str) -> Option<&dyn ActivityHandler> {
        self.handlers.get(name).map(|b| b.as_ref())
    }
}

/// 活动处理器 trait — 黑盒接口
#[async_trait]
trait ActivityHandler: Send + Sync {
    async fn execute(&self, task: &ActivityTask) -> Result<String, String>;
}
```

### 3.6 Handler 实现（脚本执行器）

```
/// Shell 脚本处理器
struct ScriptHandler {
    command: String,       // "bash -c"
    script_path: PathBuf,
    workdir: PathBuf,
}

impl ActivityHandler for ScriptHandler {
    async fn execute(&self, task: &ActivityTask) -> Result<String, String> {
        let output = tokio::process::Command::new("bash")
            .arg("-c")
            .arg(self.script_path.to_str().unwrap())
            .current_dir(&self.workdir)
            .output()
            .await
            .map_err(|e| e.to_string())?;
        
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).into_owned())
        }
    }
}

/// Python 脚本处理器（类似，只是用 python3 代替 bash）
struct PythonHandler { ... }
```

### 3.7 GrpcClient（gRPC 客户端封装）

```
GrpcClient {
    stub: WorkerServiceClient<Channel>,   // tonic 生成的客户端
    
    async fn new(url: &str) -> Self {
        let channel = Endpoint::from_shared(format!("http://{}", url))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            stub: WorkerServiceClient::new(channel),
        }
    }
    
    async fn poll_activity(&mut self, request: PollActivityRequest) 
        -> Result<Option<ActivityTask>, tonic::Status> 
    {
        let response = self.stub.poll_activity(request).await?;
        let inner = response.into_inner();
        Ok(inner.task)  // has_task && task.is_some() → Some, else None
    }
    
    async fn report_result(&mut self, activity_id: &str, result_json: &str) 
        -> Result<(), tonic::Status> 
    {
        self.stub.report_activity_result(ReportActivityResultRequest {
            activity_id: activity_id.to_string(),
            result_json: result_json.to_string(),
        }).await.map(|_| ())
    }
    
    async fn report_failure(&mut self, activity_id: &str, error_message: &str) 
        -> Result<(), tonic::Status> 
    {
        self.stub.report_activity_failure(ReportActivityFailureRequest {
            activity_id: activity_id.to_string(),
            error_message: error_message.to_string(),
        }).await.map(|_| ())
    }
}
```

---

## 四、关键设计决策

### 4.1 为什么用 Semaphore 而不是 Mutex？

Temporal 使用 `MeteredPermitDealer`（信号量模式）控制并发。我们简化为 `tokio::sync::Semaphore`：
- 每个活动执行前获取一个 permit
- 完成后释放
- 限制最大并发数，防止资源耗尽

### 4.2 为什么用 tokio::spawn？

每个活动在独立任务中运行：
- 不阻塞轮询循环（poller 可以继续拉取新任务）
- 活动超时不影响其他活动
- 自然支持并发执行多个活动

### 4.3 gRPC stub 的共享问题

**关键问题：** tonic 生成的 `WorkerServiceClient<Channel>` 需要 `&mut self`。

**解决方案（参考 Temporal）：** Channel 本身是可克隆的，每次 clone 会复用底层连接池。所以我们：
- **不**用 `Arc<Mutex<Client>>`（锁竞争严重）
- **直接 clone Channel** 创建多个 stub，每个活动用一个独立 stub

```rust
// 正确做法：clone channel，不是 mutex
let channel = Endpoint::from_shared(url)?.connect().await?;
// channel.clone() 复用连接池，开销极小
```

### 4.4 Proto 生成的代码结构

`include!(concat!(env!("OUT_DIR"), "/khronos.rs"))` 在 crate root 展开后：
- **消息类型**直接暴露：`ActivityTask`, `PollActivityRequest` 等 → `crate::ActivityTask`
- **客户端模块**：`worker_service_client::WorkerServiceClient` → `crate::worker_service_client::...`

---

## 五、完整执行流程（端到端）

```
服务器触发 Schedule
    │
    ▼
WorkflowEngine 创建 WorkflowRun + ActivityStep (state=pending)
    │
    ▼
[Worker 侧]
    │
    ▼
ActivityPoller.poll() → gRPC: PollActivityRequest
    │
    ▼
Server 返回 ActivityTask { activity_id, name, args, ... }
    │
    ▼
TaskExecutor.execute_task(task)
    ├── Semaphore.acquire()          // 等待并发槽位
    ├── registry.get("lycus-xxx")   // 查找处理器
    ├── tokio::spawn({              // 异步执行
    │     handler.execute(&task).await
    │     → bash/python 脚本运行
    │     → stdout/stderr 捕获
    │ })
    └── Channel.clone()             // 独立 gRPC stub
            │
            ▼
        client.report_result(activity_id, result_json)
            │
            ▼
[Server 侧]
ActivityStep.state = completed
WorkflowEngine 检查后续步骤 → 继续或完成工作流
```

---

## 六、Cargo.toml 依赖清单

```toml
[package]
edition = "2024"

[dependencies]
tokio = { version = "1", features = ["full"] }
tonic = "0.12"
prost = "0.13"
async-trait = "0.1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
futures-util = "0.3"
anyhow = "1"

[build-dependencies]
tonic-build = "0.12"
```

---

## 七、文件清单与行数预估

| 文件 | 职责 | 预估行数 |
|------|------|----------|
| `main.rs` | CLI入口，组装组件 | ~80 |
| `worker.rs` | Worker结构体 + run循环 | ~60 |
| `poller.rs` | ActivityPoller + stream | ~80 |
| `executor.rs` | TaskExecutor + semaphore | ~120 |
| `registry.rs` | HashMap注册表 | ~40 |
| `handler.rs` | trait + ScriptHandler + PythonHandler | ~100 |
| `client.rs` | GrpcClient封装tonic stub | ~80 |
| **总计** | | **~560行** |

简洁、清晰、可编译。
