// 编译钩子：复用 banqi-core 的 build_hook.rs（注释清理 + rustfmt，幂等）。
#[path = "../../../banqi-core/build_hook.rs"]
mod comment_hook;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../proto/scheduler.proto";
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=../../../banqi-core/build_hook.rs");
    comment_hook::clean(&std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default());
    println!("cargo:rerun-if-changed={proto}");
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&[proto], &["../../proto"])?;
    Ok(())
}
