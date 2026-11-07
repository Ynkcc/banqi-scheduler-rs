// 编译钩子：复用 banqi-core 的 build_hook.rs（注释清理 + rustfmt，幂等）。
#[path = "../../../banqi-core/build_hook.rs"]
mod comment_hook;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../proto/scheduler.proto";
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=../../../banqi-core/build_hook.rs");
    comment_hook::clean(&std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default());
    println!("cargo:rerun-if-changed={proto}");
    // game_record.proto 为对局记录/标签包契约的单一真源（banqi-core/proto/），
    // scheduler.proto 通过 import 引用；include 目录需同时覆盖两处。
    println!("cargo:rerun-if-changed=../../../banqi-core/proto/game_record.proto");
    // bqrecord.* 类型与 bq-record crate 共享同一份生成定义（杜绝重复定义漂移）。
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .extern_path(".bqrecord", "::bq_record::types")
        .compile_protos(&[proto], &["../../proto", "../../../banqi-core/proto"])?;
    Ok(())
}
