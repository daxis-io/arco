//! Build script for compiling protobuf definitions.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Proto compilation will be enabled once proto files are added
    // tonic_build::configure()
    //     .build_server(true)
    //     .build_client(true)
    //     .compile(
    //         &[
    //             "../../proto/arco/v1/catalog.proto",
    //             "../../proto/arco/v1/flow.proto",
    //         ],
    //         &["../../proto"],
    //     )?;

    Ok(())
}
