//! Compiles the versioned .proto files with `protox` (pure-Rust protobuf compiler, no
//! system `protoc`) and generates Rust types with `prost-build`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = "../../proto";
    let files = ["scada/driver/v1/driver.proto", "scada/introspect/v1/introspect.proto"];
    for f in &files {
        println!("cargo:rerun-if-changed={root}/{f}");
    }
    let fds = protox::compile(files, [root])?;
    prost_build::Config::new().compile_fds(fds)?;
    Ok(())
}
