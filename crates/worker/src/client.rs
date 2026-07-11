//! gRPC client wrapper — each call gets its own stub from a cloned channel.
//! Channel cloning reuses the underlying connection pool (Temporal pattern).

use tonic::transport::{Channel, Endpoint};

/// Lightweight gRPC client — clones reuse the underlying connection pool.
#[derive(Clone)]
pub struct GrpcClient {
    channel: Channel,
}

impl GrpcClient {
    pub async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = Endpoint::from_shared(format!("http://{}", url))?;
        let channel = endpoint.connect().await?;
        Ok(Self { channel })
    }

    /// Poll for activity tasks. Clones the channel so concurrent polls work.
    pub async fn poll_activity(
        &self,
        task_queue: String,
        activity_types: Vec<String>,
    ) -> Result<Option<crate::ActivityTask>, tonic::Status> {
        let request = crate::PollActivityRequest {
            task_queue,
            activity_types,
        };

        let mut stub = crate::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        let response = stub.poll_activity(request).await?;
        let inner = response.into_inner();
        Ok(inner.task)
    }

    /// Report successful activity result.
    pub async fn report_result(
        &self,
        activity_id: String,
        result_json: String,
    ) -> Result<(), tonic::Status> {
        let request = crate::ReportActivityResultRequest {
            activity_id,
            result_json,
        };

        let mut stub = crate::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        stub.report_activity_result(request).await.map(|_| ())
    }

    /// Report activity failure.
    pub async fn report_failure(
        &self,
        activity_id: String,
        error_message: String,
    ) -> Result<(), tonic::Status> {
        let request = crate::ReportActivityFailureRequest {
            activity_id,
            error_message,
        };

        let mut stub = crate::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        stub.report_activity_failure(request).await.map(|_| ())
    }
}
