fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        // Compile existing Khronos proto
        .compile_protos(
            &["../../proto/khronos.proto"],
            &["../../proto"],
        )?;

    // Compile Temporal WorkflowService proto
    // The temporal protos are under proto/temporal/ and reference
    // google/api/*.proto, nexusannotations/v1/*.proto, temporal/api/**/*.proto
    tonic_build::configure()
        .build_server(true)
        .compile_protos(
            &["../../proto/temporal/temporal/api/workflowservice/v1/service.proto"],
            &["../../proto/temporal"],
        )?;

    Ok(())
}
