//! Build script — generate gRPC stubs from proto.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&["../../proto/khronos.proto"], &["../../proto"])?;
    Ok(())
}
