//! Khronos gRPC server.

pub mod grpc;
pub mod scheduler;
pub mod engine;

// Re-export generated proto types
mod khronos {
    include!(concat!(env!("OUT_DIR"), "/khronos.rs"));
}

pub use khronos::*;

// Re-export generated Temporal proto types
pub mod temporal {
    pub mod api {
        pub mod activity { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.activity.v1.rs")); } }
        pub mod batch { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.batch.v1.rs")); } }
        pub mod callback { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.callback.v1.rs")); } }
        pub mod command { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.command.v1.rs")); } }
        pub mod common { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.common.v1.rs")); } }
        pub mod compute { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.compute.v1.rs")); } }
        pub mod deployment { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.deployment.v1.rs")); } }
        pub mod enums { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.enums.v1.rs")); } }
        pub mod failure { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.failure.v1.rs")); } }
        pub mod filter { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.filter.v1.rs")); } }
        pub mod history { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.history.v1.rs")); } }
        pub mod namespace { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.namespace.v1.rs")); } }
        pub mod nexus { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.nexus.v1.rs")); } }
        pub mod protocol { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.protocol.v1.rs")); } }
        pub mod protometa { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.protometa.v1.rs")); } }
        pub mod query { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.query.v1.rs")); } }
        pub mod replication { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.replication.v1.rs")); } }
        pub mod rules { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.rules.v1.rs")); } }
        pub mod schedule { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.schedule.v1.rs")); } }
        pub mod sdk { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.sdk.v1.rs")); } }
        pub mod taskqueue { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.taskqueue.v1.rs")); } }
        pub mod update { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.update.v1.rs")); } }
        pub mod version { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.version.v1.rs")); } }
        pub mod worker { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.worker.v1.rs")); } }
        pub mod workflow { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.workflow.v1.rs")); } }
        pub mod workflowservice { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/temporal.api.workflowservice.v1.rs")); } }
    }
}