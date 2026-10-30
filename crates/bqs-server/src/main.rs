//! 【所有权：edge 成员】
//!
//! 入口：CLI → 配置 → 装配 → 两个监听（gRPC + WebUI HTTP）→ 优雅停机。
//!
//! 参考实现：`banqi-scheduler/cmd/scheduler/main.go`（**TLS 段整体剔除**）。
//!
//! 生命周期约定：
//! - `-c/--config` 必填（不存在拒绝启动）；`--listen / --db / --http-addr /
//!   --bucket` 覆盖文件值；不读环境变量；
//! - SIGINT/SIGTERM → gRPC 优雅停机（tonic serve_with_shutdown）→ WebUI 连接排空
//!   （hyper-util GracefulShutdown），宽限期 `SHUTDOWN_GRACE`；
//! - WebUI 启动失败只记日志，不影响 gRPC 调度；
//! - 传输层把每个 RPC 包进 `tokio::task::spawn_blocking`（内核是同步的，见 grpc.rs）。

mod grpc;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;

use bqs_config::Config;
use bqs_core::ports::{
    EvalStore, EpisodeStore, FeaturePackStore, JobStore, MatchStore, NetworkStore, SettingsStore,
    SystemClock, TrainerRegistryStore, TrainerStore, WorkerStore,
};
use bqs_core::server::{Server, ServerDeps};

/// 优雅关闭宽限：等待在飞请求与日志刷盘的上限（Go: shutdownGrace）。
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// WebUI 读头超时（慢速连接防护；Go: http.Server.ReadHeaderTimeout）。
const HTTP_READ_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Parser)]
#[command(name = "scheduler", about = "banqi 分布式自对弈训练中心调度器（Rust 版）")]
pub struct Args {
    /// 配置文件路径（TOML 风格，未知键拒绝启动）
    #[arg(short, long, default_value = "configs/config.conf")]
    pub config: std::path::PathBuf,
    /// 覆盖 server.listen
    #[arg(long)]
    pub listen: Option<String>,
    /// 覆盖 db.path
    #[arg(long)]
    pub db: Option<String>,
    /// 覆盖 server.http_addr
    #[arg(long)]
    pub http_addr: Option<String>,
    /// 覆盖 storage.bucket（仅首次启动引导 profile 时生效）
    #[arg(long)]
    pub bucket: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    run(args).await
}

/// ":50051" 形式的监听地址 → 全接口 SocketAddr（Go: net.Listen 直接接受 ":port"）。
fn parse_listen(s: &str) -> Result<SocketAddr> {
    let normalized = if s.starts_with(':') { format!("0.0.0.0{s}") } else { s.to_string() };
    normalized
        .parse()
        .with_context(|| format!("监听地址无法解析: {s}"))
}

/// CLI 覆盖项（flag 未传时为空，回退配置文件/默认值）。
fn apply_cli_overrides(cfg: &mut Config, args: &Args) {
    if let Some(v) = &args.listen {
        cfg.server.listen = v.clone();
    }
    if let Some(v) = &args.db {
        cfg.db.path = v.clone();
    }
    if let Some(v) = &args.http_addr {
        cfg.server.http_addr = v.clone();
    }
    if let Some(bucket) = &args.bucket {
        // --bucket 只改 bootstrap 用的 storage 段（首次启动引导 default profile）
        let sec = cfg.storage.get_or_insert_with(Default::default);
        sec.bucket = bucket.clone();
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// SIGINT / SIGTERM 任一到达即返回（Go: signal.NotifyContext）。
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("注册 SIGTERM 处理器");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn run(args: Args) -> Result<()> {
    init_tracing();

    // ---- 配置：文件（强制存在）+ CLI 覆盖 + 启动校验 ----
    let mut cfg = Config::load(&args.config)
        .with_context(|| format!("配置文件 {}", args.config.display()))?;
    apply_cli_overrides(&mut cfg, &args);
    cfg.validate().context("[config]")?;
    if let Some(dir) = Path::new(&cfg.db.path).parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("[config] 创建数据库目录 {}", dir.display()))?;
        }
    }

    // ---- 装配（同步短事务）----
    let store = bqs_store::Store::open(&cfg.db.path).context("store")?;
    // Store 实现全部端口（bqs-store 侧编译期断言）；逐端口强转装配束
    let ports = bqs_core::ports::Ports {
        settings: Arc::clone(&store) as Arc<dyn SettingsStore>,
        networks: Arc::clone(&store) as Arc<dyn NetworkStore>,
        matches: Arc::clone(&store) as Arc<dyn MatchStore>,
        eval: Arc::clone(&store) as Arc<dyn EvalStore>,
        trainers: Arc::clone(&store) as Arc<dyn TrainerStore>,
        episodes: Arc::clone(&store) as Arc<dyn EpisodeStore>,
        workers: Arc::clone(&store) as Arc<dyn WorkerStore>,
        trainer_registry: Arc::clone(&store) as Arc<dyn TrainerRegistryStore>,
        feature_packs: Arc::clone(&store) as Arc<dyn FeaturePackStore>,
        jobs: Arc::clone(&store) as Arc<dyn JobStore>,
    };
    let storage = Arc::new(
        bqs_storage::Manager::new(ports.settings.clone(), cfg.storage.clone())
            .context("storage bootstrap")?,
    );
    let server = Server::assemble(ServerDeps {
        cfg: cfg.clone(),
        signer: storage.signer(),
        clock: Arc::new(SystemClock),
        ports,
    });

    // ---- WebUI HTTP（axum + 静态托管；绑定失败只记日志，不影响 gRPC 调度）----
    let webui_dist: Option<Arc<PathBuf>> = if cfg.server.webui_dist.is_empty() {
        None
    } else {
        Some(Arc::new(PathBuf::from(&cfg.server.webui_dist)))
    };
    let app = bqs_api::router(bqs_api::AppState {
        server: Arc::clone(&server),
        storage: Arc::clone(&storage),
        online_window_secs: cfg.server.worker_online_seconds,
        webui_dist,
    });
    let http_addr = parse_listen(&cfg.server.http_addr)?;
    let (http_tx, http_rx) = tokio::sync::watch::channel(false);
    let webui = tokio::spawn(run_webui(http_addr, app, http_rx));

    // ---- gRPC（tonic；serve_with_shutdown 接优雅停机信号）----
    let grpc_addr = parse_listen(&cfg.server.listen)?;
    let (grpc_tx, grpc_rx) = tokio::sync::watch::channel(false);
    let grpc_shutdown = {
        let mut rx = grpc_rx.clone();
        async move {
            let _ = rx.changed().await;
        }
    };
    let grpc = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(
                bqs_proto::pb::scheduler_service_server::SchedulerServiceServer::new(
                    grpc::SchedulerSvc(Arc::clone(&server)),
                ),
            )
            .serve_with_shutdown(grpc_addr, grpc_shutdown),
    );

    // 启动日志（Go 版同款信息；paused / autoPaused 等运行时值由 WebUI /api/status 暴露）
    let sp = &cfg.selfplay;
    tracing::info!(
        "[scheduler] listening on {} db={} bucket={} sprt(elo0={},elo1={},alpha={},beta={})",
        cfg.server.listen,
        cfg.db.path,
        cfg.storage.as_ref().map(|s| s.bucket.as_str()).unwrap_or(""),
        cfg.sprt.elo0,
        cfg.sprt.elo1,
        cfg.sprt.alpha,
        cfg.sprt.beta,
    );
    tracing::info!(
        "[scheduler] variant={} initial_revealed={} data_kind={} min_client_version={:?}",
        sp.variant,
        sp.initial_revealed,
        sp.data_kind.as_str(),
        cfg.server.min_client_version,
    );
    if cfg.eval.enabled {
        tracing::info!(
            "[eval] enabled opponents={:?} games={} mode={} every={}_promotions no_progress={} eps={:.3}",
            cfg.eval.opponents,
            cfg.eval.games,
            bqs_api::views::eval_search_mode_name(cfg.eval.mcts_sims),
            cfg.eval.every_n_promotions,
            cfg.eval.no_progress_n,
            cfg.eval.no_progress_eps,
        );
    } else {
        tracing::info!("[eval] disabled（eval.enabled=false）");
    }

    // ---- 生命周期：gRPC 先优雅排空，随后 WebUI，宽限 SHUTDOWN_GRACE ----
    tokio::pin!(grpc);
    tokio::select! {
        res = &mut grpc => {
            match res {
                Ok(serve) => serve.context("grpc serve")?,
                Err(e) => return Err(e.into()),
            }
        }
        _ = wait_for_signal() => {
            tracing::info!("[scheduler] 收到退出信号，开始优雅关闭");
            let _ = grpc_tx.send(true);
            // gRPC GracefulStop（tonic 在 shutdown future 触发后排空连接）
            if tokio::time::timeout(SHUTDOWN_GRACE, &mut grpc).await.is_err() {
                tracing::warn!("[scheduler] gRPC 优雅关闭超时（{}s），继续关闭 WebUI", SHUTDOWN_GRACE.as_secs());
                grpc.abort();
            }
            // WebUI Shutdown（hyper-util 连接排空）
            let _ = http_tx.send(true);
            if tokio::time::timeout(SHUTDOWN_GRACE, webui).await.is_err() {
                tracing::warn!("[webui] shutdown 超时");
            }
            tracing::info!("[scheduler] 已退出");
        }
    }
    Ok(())
}

/// WebUI 服务循环：accept → 每连接 hyper-util（HTTP/1 读头超时）+ 优雅排空。
/// 绑定失败只记日志（Go 版语义：WebUI 故障不影响 gRPC 调度）。
async fn run_webui(
    addr: SocketAddr,
    app: axum::Router,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!("[webui] 绑定 {addr} 失败: {e}（gRPC 调度不受影响）");
            return;
        }
    };
    tracing::info!("[webui] listening on {addr}");

    let graceful = hyper_util::server::graceful::GracefulShutdown::new();

    loop {
        tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok((stream, _peer)) => {
                        stream.set_nodelay(true).ok();
                        // Router（tower Service<Request<Body>>）→ hyper Service<Request<Incoming>>：
                        // 逐请求 oneshot 调用 + Incoming → axum Body 转换
                        let app = app.clone();
                        let svc = hyper_util::service::TowerToHyperService::new(
                            tower::service_fn(
                                move |req: axum::http::Request<hyper::body::Incoming>| {
                                    let app = app.clone();
                                    async move {
                                        let (parts, body) = req.into_parts();
                                        let req = axum::http::Request::from_parts(
                                            parts,
                                            axum::body::Body::new(body),
                                        );
                                        use tower::ServiceExt as _;
                                        app.oneshot(req).await
                                    }
                                },
                            ),
                        );
                        // builder 与连接一起整体搬进任务（watcher 为 owned 类型）
                        let watcher = graceful.watcher();
                        tokio::spawn(async move {
                            let mut builder = hyper_util::server::conn::auto::Builder::new(
                                hyper_util::rt::TokioExecutor::new(),
                            );
                            // timer 必须安装：header_read_timeout 依赖它，
                            // 否则 hyper 运行时 panic（timeout set but no timer）
                            builder
                                .http1()
                                .timer(hyper_util::rt::TokioTimer::new())
                                .header_read_timeout(HTTP_READ_HEADER_TIMEOUT);
                            builder.http2().timer(hyper_util::rt::TokioTimer::new());
                            let conn = builder.serve_connection_with_upgrades(
                                hyper_util::rt::TokioIo::new(stream),
                                svc,
                            );
                            if let Err(e) = watcher.watch(conn).await {
                                tracing::warn!("[webui] connection error: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("[webui] accept: {e}");
                        // 短暂退避避免错误风暴；继续服务
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            _ = shutdown.changed() => {
                graceful.shutdown().await;
                tracing::info!("[webui] 已停止");
                return;
            }
        }
    }
}
