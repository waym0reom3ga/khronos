//! Khronos gRPC server.

pub mod grpc;
pub mod scheduler;
pub mod engine;

// Re-export generated proto types
mod khronos {
    include!(concat!(env!("OUT_DIR"), "/khronos.rs"));
}

pub use khronos::*;
