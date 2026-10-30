//! 组装根与 gRPC 契约编排门面。
//!
//! **所有权：core-pipeline 成员**（子系统字段与 `assemble` 中其它成员的
//! 构造行除外）。
//!
//! `Server` 是 gRPC 契约编排层：自身无业务状态、无锁，只把 14 个 RPC 编排到
//! 各子系统（每个子系统独立持锁）。业务拒绝走正常回包（accepted=false），
//! `CoreError` 只表示真故障 → gRPC `Status::internal`。
//!
//! 全部方法为同步；调用方（bqs-server 传输层）负责 `spawn_blocking`。
//!
//! 行为真源（非逐行翻译）：`banqi-scheduler/internal/scheduler/` 的
//! `server.go`（GetInfo / Heartbeat）、`tasks.go`（GetTask）、`episodes.go`
//! （ReportEpisode / SignNetworkUpload / ListEpisodes）、`networks.go`
//! （GetNetwork / RegisterNetwork / ReportMatchResult 路由）、`reanalysis.go`
//! （SubmitReanalysis）、`featurepack.go`（ReportFeaturePack /
//! RequestFeaturePack）、`trainer.go`（TrainerHeartbeat）、`trainconfig.go`。

use std::collections::HashMap;
use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::*;

use crate::{
    assembler::{episode_key, network_key, TaskAssembler},
    balance::{BalanceMonitor, TrainerHeartbeatInput},
    control::Control,
    dispatch::TaskDispatcher,
    evalcoord::EvalCoordinator,
    featurepack::{feature_pack_key, DeriveQueue, PendingDerive, DEFAULT_FEATURE_PACK_VERSION},
    gatekeeper::Gatekeeper,
    jobs::{JobManager, JobSpecJson},
    ports::{Clock, ObjectSigner, Ports},
    reanalysis::{PendingReanalysis, ReanaQueue},
    reports::{lookup_report_task, ReportRouter},
    sources::Sources,
    ports::FeaturePackDraft,
    types::EpisodeDraft,
    variants, CoreError, CoreResult,
};

/// 子系统束：api 层（bqs-api）经这些 pub 字段直接读取子系统查询方法。
pub struct Server {
    pub cfg: Arc<Config>,

    // 依赖端口
    pub ports: Ports,
    pub signer: Arc<dyn ObjectSigner>,
    pub clock: Arc<dyn Clock>,

    // 子系统（各自独立持锁；锁序约定见 lib.rs：evalc.mu → disp.mu）
    pub disp: Arc<TaskDispatcher>,
    pub evalc: Arc<EvalCoordinator>,
    pub gate: Arc<Gatekeeper>,
    pub balance: Arc<BalanceMonitor>,
    pub reana: Arc<ReanaQueue>,
    pub derive: Arc<DeriveQueue>,
    pub jobs: Arc<JobManager>,
    pub ctl: Arc<Control>,

    // 任务组装层（无状态读 + 签名）
    pub asm: Arc<TaskAssembler>,
    // 上报聚合路由（evalSink → gateSink → not_gating）
    pub router: Arc<ReportRouter>,
    // GetTask 的任务生产源（按优先级排序，见 sources.rs）
    pub sources: Vec<Box<dyn crate::sources::TaskSource>>,
}

pub struct ServerDeps {
    pub cfg: Config,
    pub ports: Ports,
    pub signer: Arc<dyn ObjectSigner>,
    pub clock: Arc<dyn Clock>,
}

impl Server {
    /// 装配根：构造全部子系统并注入依赖。
    pub fn assemble(deps: ServerDeps) -> Arc<Self> {
        let cfg = Arc::new(deps.cfg);
        let ports = deps.ports;
        let signer = deps.signer;
        let clock = deps.clock;

        // 各子系统构造参数如需扩展，由其所有者直接在此调整（只改自己那一行）。
        // 注：ctl 必须先于 evalc / balance 构造（它们持有 Arc<Control>）；
        // gate 持有 Arc<EvalCoordinator>（晋级后触发 eval 入队）。
        let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), clock.clone()));
        let ctl = Arc::new(Control::new(&ports, cfg.clone()));
        let evalc = Arc::new(EvalCoordinator::new(&ports, cfg.clone(), clock.clone(), ctl.clone(), disp.clone()));
        let gate = Arc::new(Gatekeeper::new(&ports, cfg.clone(), evalc.clone()));
        let balance = Arc::new(BalanceMonitor::new(&ports, cfg.clone(), clock.clone(), ctl.clone()));
        let reana = Arc::new(ReanaQueue::new(&ports, cfg.clone(), clock.clone()));
        let derive = Arc::new(DeriveQueue::new(&ports, cfg.clone(), clock.clone()));
        let jobs = Arc::new(JobManager::new(
            ports.jobs.clone(),
            disp.clone(),
            ports.networks.clone(),
            ctl.clone(),
            cfg.clone(),
            clock.clone(),
        ));
        // evalcoord 是评估轮发起方：作业入队成功时同步创建 JOB_EVAL job（Weak 注入防环）
        evalc.set_job_hook(&jobs);

        let asm = Arc::new(TaskAssembler::new(
            ports.networks.clone(),
            signer.clone(),
            cfg.clone(),
            ctl.clone(),
        ));
        let router = Arc::new(ReportRouter::new(evalc.clone(), gate.clone(), disp.clone(), jobs.clone()));
        let sources = Sources::assemble(
            jobs.clone(),
            evalc.clone(),
            reana.clone(),
            derive.clone(),
            ports.matches.clone(),
            ports.networks.clone(),
            disp.clone(),
            asm.clone(),
            ctl.clone(),
            cfg.clone(),
        );

        // 启动补齐：当前 best 若有未评测的对手（新增评测配置 / 上次被中断）立即入队
        //（Go 版 New() 末尾的 ensureBestEvaluated；restore 已在 EvalCoordinator::new 内完成）。
        evalc.ensure_best_evaluated();
        // 统一 Job 机制引导：确保持续型 selfplay job（total=0）存在；running job
        // 的行已在库（重启恢复重新载入），在飞任务视为丢失（deadline 兜底）。
        // 引导失败 = 存储故障，拒绝启动（与 Control::new 的 panic 约定一致）。
        if let Err(e) = jobs.ensure_selfplay_job() {
            panic!("[jobs] selfplay job 引导失败，拒绝启动: {e}");
        }

        Arc::new(Self {
            cfg,
            ports,
            signer,
            clock,
            disp,
            evalc,
            gate,
            balance,
            reana,
            derive,
            jobs,
            ctl,
            asm,
            router,
            sources,
        })
    }

    fn ttl_secs(&self) -> i64 {
        crate::consts::PRESIGN_TTL.as_secs() as i64
    }

    // -----------------------------------------------------------------------
    // RPC 门面（14 个，签名与 proto 一一对应）
    // -----------------------------------------------------------------------

    /// 变体 + 停机信号（trainer/worker 启动与轮询）。
    pub fn get_info(&self, _req: GetInfoRequest) -> CoreResult<GetInfoReply> {
        // 变体对局元信息随 GetInfo 一并下发（新版训练端据此构建变体描述符；
        // 旧版训练端忽略该字段）。
        let variant = &self.cfg.selfplay.variant;
        let meta = variants::lookup(variant).ok_or_else(|| {
            CoreError::Internal(format!("变体 {variant} 未在 bq-env 变体表登记（应启动即失败）"))
        })?;
        let (should_stop, stop_reason) = self.ctl.should_stop();
        Ok(GetInfoReply {
            variant: variant.clone(),
            variant_meta: Some(VariantMeta {
                id: meta.id.clone(),
                label: meta.label.clone(),
                rows: meta.rows,
                cols: meta.cols,
                piece_counts: meta.piece_counts.clone(),
                piece_values: meta.piece_values.clone(),
                initial_health: meta.initial_health,
                max_consecutive_moves_for_draw: meta.max_consecutive_moves_for_draw,
                symmetries: meta.symmetries.clone(),
            }),
            should_stop,
            stop_reason,
        })
    }

    /// worker 拉任务：前置校验 → 平衡评估 → 逐源尝试（eval → reanalysis → derive → selfplay）。
    pub fn get_task(&self, req: TaskRequest) -> CoreResult<TaskResponse> {
        // 内存任务表的超期回收（按 PRUNE_INTERVAL 节流）
        self.disp.prune();

        let none = |message: &str| TaskResponse {
            kind: TaskKind::TaskNone as i32,
            message: message.to_string(),
            ..Default::default()
        };

        // client_version 管控。**与 Go 版的刻意差异**：Go 用字典序比较
        //（`req.ClientVersion < cfg.MinClientVersion`），0.10.0 会被误判低于
        // 0.9.0；这里改为语义化版本感知的数值比较（见 client_version_too_old）。
        let min = &self.cfg.server.min_client_version;
        if !min.is_empty()
            && !req.client_version.is_empty()
            && client_version_too_old(&req.client_version, min)
        {
            return Ok(none("client_version_too_old"));
        }
        if !req.client_version.is_empty() {
            tracing::info!(
                worker = %req.worker_id, version = %req.client_version,
                threads = req.threads, memory_mb = req.memory_mb, "[task] GetTask"
            );
        }

        // 人工暂停（生效暂停 = manual || auto，自动暂停由 selfplay 源按自动口径回包）
        if self.ctl.manual_paused() {
            return Ok(none("self_play_paused"));
        }

        // 生成-消费平衡：惰性评估 trainer 消费健康度（内部按节流间隔重算）。
        // 自动暂停只拦数据生产（selfplay / 可配的 reanalysis），eval 恒放行——
        // 它不产 episode 且是绝对强度停机判据的来源。
        self.balance.evaluate(self.clock.now());

        for src in &self.sources {
            if let Some(resp) = src.try_task(&req)? {
                return Ok(resp);
            }
        }
        // 不可达（selfplay 源恒处理请求）：防御性兜底，让 worker 空转等待而非报错。
        Ok(none("no_task_available"))
    }

    /// episode 元数据登记 + 签发预签名 PUT（幂等：task_id 唯一，重试重签 URL）。
    pub fn report_episode(&self, meta: EpisodeMeta) -> CoreResult<EpisodeAck> {
        // 公共闸口（lookup + worker 归属校验）。注意 episode 路径不做终态短路：
        // 上报后任务即 Completed，worker 崩溃重试上传时仍需拿到对象键与重签 PUT
        // URL——幂等由下方 DB 唯一索引闸口承担（duplicate_replay）。
        let task = match lookup_report_task(&self.disp, &meta.task_id, &meta.worker_id) {
            Ok(t) => t,
            Err(rejected) => {
                return Ok(EpisodeAck {
                    accepted: rejected.accepted,
                    message: rejected.message,
                    ..Default::default()
                });
            }
        };
        // 归属校验：collect 任务（JOB_COLLECT）无单一主网络（双方棋手各自加载），
        // network_sha 为空即放宽比对；其余任务保持严格一致。
        if !task.network_sha.is_empty() && task.network_sha != meta.network_sha {
            return Ok(EpisodeAck {
                accepted: false,
                message: format!(
                    "network_sha_mismatch task={} got={}",
                    task.network_sha, meta.network_sha
                ),
                ..Default::default()
            });
        }
        // 幂等闸口：同 task_id 已登记过 → 重用对象键 + 重签 PUT URL（worker 续传，
        // 避免产生孤儿对象）
        if let Some(existing) = self.ports.episodes.get_episode_by_task(&meta.task_id)? {
            let url = self.signer.presign_put(&existing.object_key, self.ttl_secs())?;
            tracing::info!(
                "[episode] 幂等重发 worker={} task={} -> 复用 {}",
                meta.worker_id, meta.task_id, existing.object_key
            );
            return Ok(EpisodeAck {
                accepted: true,
                upload_url: url,
                object_key: existing.object_key,
                message: "duplicate_replay".into(),
                ..Default::default()
            });
        }
        let obj_id = crate::sources::new_id();
        let key = episode_key(&meta.network_sha, &obj_id);
        let url = self.signer.presign_put(&key, self.ttl_secs())?;
        let draft = EpisodeDraft {
            worker_id: meta.worker_id.clone(),
            task_id: meta.task_id.clone(),
            network_sha: meta.network_sha.clone(),
            game_count: meta.game_count as i64,
            total_steps: meta.total_steps as i64,
            winner: meta.winner as i64,
            object_key: key.clone(),
            created_at: self.clock.now(),
            kind: meta.kind as i64,
            // 对局双方与对打关联（下发任务时登记在内存任务表；0/空 = 无对手）
            opponent_sha: task.opponent_sha.clone(),
            opponent_spec: task.opponent_spec.clone(),
            match_id: task.match_id,
            // 逐局四分（collector 上报；旧版不填为 0）
            red_wins: meta.red_wins as i64,
            black_wins: meta.black_wins as i64,
            draw_no_capture: meta.draw_no_capture as i64,
            draw_truncation: meta.draw_truncation as i64,
        };
        self.ports.episodes.insert_episode(draft)?;
        tracing::info!(
            "[episode] worker={} task={} network={} opponent={}{} match={} games={} steps={} -> {}",
            meta.worker_id, meta.task_id, meta.network_sha, task.opponent_spec,
            task.opponent_sha, task.match_id, meta.game_count, meta.total_steps, key
        );
        // 状态机：上报即完成（Assigned → Completed）；幂等重发同样标记完成
        self.disp.mark_done(&meta.task_id);
        // 统一 Job 机制：collect / selfplay job 的完成口径聚合（ReportEpisode 的
        // game_count；先落库后更新计数，映射在 job_tasks）。无映射（rating /
        // reanalysis / derive 等）静默忽略。
        if let Err(e) = self.jobs.record_report(&meta.task_id, meta.game_count as i64) {
            tracing::warn!("[job] ⚠️ episode 上报聚合失败 task={}: {e}", meta.task_id);
        }
        Ok(EpisodeAck {
            accepted: true,
            upload_url: url,
            object_key: key,
            ..Default::default()
        })
    }

    /// 对局结果上报：eval 聚合 / gatekeeper GSPRT 聚合（按 task.kind 与 MatchID 路由）。
    /// 公共闸口（lookup + worker 归属校验）与幂等短路由 ReportRouter::route 承担。
    pub fn report_match_result(&self, r: MatchResult) -> CoreResult<MatchResultAck> {
        self.router.route(&r)
    }

    /// worker 心跳：登记 + 回传 best sha 与生效暂停。
    pub fn heartbeat(&self, req: HeartbeatRequest) -> CoreResult<HeartbeatReply> {
        self.ports.workers.touch_worker(
            &req.worker_id,
            req.current_threads as i64,
            req.completed_games as i64,
            &req.client_version,
            req.memory_mb,
            &req.health_status,
            &req.last_error,
        )?;
        if !req.client_version.is_empty() {
            if let Ok(Some(v)) = self.ports.workers.worker_version(&req.worker_id) {
                if !v.is_empty() && v != req.client_version {
                    tracing::info!("[worker] {} version changed {} -> {}", req.worker_id, v, req.client_version);
                }
            }
        }
        let best = self.ports.networks.get_best()?;
        // 生效暂停 = 人工 || 自动（balance）：trainer 消费停滞的自动暂停同样要拦住 worker。
        let mut reply = HeartbeatReply {
            pause_self_play: self.ctl.effective_paused(),
            ..Default::default()
        };
        if let Some(best) = best {
            reply.best_network = best.sha;
        }
        // 统一 Job 机制：在途取消下发（job 收敛/抢占/取消时加入的取消集合；
        // 下发即移除，worker 收到后尽快终止任务，已完成部分照常上报）。
        reply.cancel_task_ids = self.jobs.cancel_task_ids_for_worker(&req.worker_id);
        if !reply.cancel_task_ids.is_empty() {
            tracing::info!(
                "[job] 心跳下发在途取消 worker={} tasks={:?}",
                req.worker_id, reply.cancel_task_ids
            );
        }
        Ok(reply)
    }

    // -----------------------------------------------------------------------
    // 统一 Job 管理（SubmitJob / ListJobs / ControlJob）
    // -----------------------------------------------------------------------

    /// 提交 job（priority=0 = 按 kind 默认；规格校验失败 accepted=false 附原因）。
    /// JOB_EVAL 额外登记 eval_queue 流式作业（聚合与派发统一走 evalcoord），
    /// 登记失败回滚 job 行——否则 job 永远无任务可派发。
    pub fn submit_job(&self, req: SubmitJobRequest) -> CoreResult<SubmitJobReply> {
        let (accepted, message, job_id) = self.jobs.submit(&req)?;
        if accepted && req.kind == JobKind::JobEval as i32 {
            if let Some(e) = &req.eval {
                let sha = if e.network_sha.is_empty() {
                    self.ports
                        .networks
                        .get_best()?
                        .map(|b| b.sha)
                        .unwrap_or_default()
                } else {
                    e.network_sha.clone()
                };
                if let Err(err) =
                    self.evalc
                        .ensure_pending(&sha, &e.opponent_kind, &e.opponent_network_sha)
                {
                    self.jobs.control(&job_id, "cancel")?;
                    return Err(err);
                }
            }
        }
        Ok(SubmitJobReply { accepted, message, job_id })
    }

    /// job 进度列表（completed_games 为上报口径，不含在飞；规格回显）。
    pub fn list_jobs(&self, req: ListJobsRequest) -> CoreResult<ListJobsReply> {
        let kind = req.kind_filter.map(|k| k as i64);
        let rows = self.jobs.list(
            if req.state_filter.is_empty() { None } else { Some(req.state_filter.as_str()) },
            kind,
        )?;
        let jobs = rows.iter().map(|r| self.job_status(r)).collect();
        Ok(ListJobsReply { jobs })
    }

    /// pause / resume / cancel（终态不可再控制）。
    pub fn control_job(&self, req: JobControlRequest) -> CoreResult<JobControlReply> {
        let (accepted, message, state) = self.jobs.control(&req.job_id, &req.control)?;
        Ok(JobControlReply { accepted, message, state })
    }

    /// JobRow → proto JobStatus（规格回显解析失败时留空——落库数据损坏的防御）。
    fn job_status(&self, r: &crate::ports::JobRow) -> JobStatus {
        let kind = JobKind::try_from(r.kind as i32).unwrap_or(JobKind::JobCollect);
        let (collect, selfplay, eval) = match JobSpecJson::parse(&r.spec_json) {
            Some(JobSpecJson::Collect(c)) => (Some(c.to_proto()), None, None),
            Some(JobSpecJson::SelfPlay(s)) => (None, Some(s.to_proto()), None),
            Some(JobSpecJson::Eval(e)) => (None, None, Some(e.to_proto())),
            None => (None, None, None),
        };
        JobStatus {
            job_id: r.job_id.clone(),
            kind: kind as i32,
            priority: r.priority as u32,
            state: r.state.clone(),
            total_games: r.total_games as u64,
            completed_games: r.completed_games as u64,
            dispatched_games: r.dispatched_games as u64,
            created_at: r.created_at,
            updated_at: r.updated_at,
            collect,
            selfplay,
            eval,
        }
    }

    /// sha 或 best → 对象键 + 预签名 GET（空 download_url = 缺席）。
    pub fn get_network(&self, req: NetworkRequest) -> CoreResult<NetworkInfo> {
        let n = if req.sha.is_empty() {
            self.ports.networks.get_best()?
        } else {
            self.ports.networks.get_network(&req.sha)?
        };
        let Some(n) = n else {
            // not found 改业务回包（空 download_url = 网络不存在）：网络刚被清理属
            // 正常态，旧版以 gRPC 错误传播会让 worker 侧按传输错误重试刷屏——
            // 调用方以 download_url == "" 判定缺席，无需重试。
            return Ok(NetworkInfo { sha: req.sha, ..Default::default() });
        };
        let key = network_key(&n.sha, &n.format);
        let url = self.signer.presign_get(&key, self.ttl_secs())?;
        Ok(NetworkInfo {
            sha: n.sha.clone(),
            download_url: url,
            created_at: n.created_at,
            is_best: n.is_best,
            game_data: serde_json::json!({ "parent_sha": n.parent_sha.clone().unwrap_or_default() })
                .to_string(),
            key,
        })
    }

    /// trainer 登记新网络（格式白名单校验 → 首个直接晋级 / 其余创建 gatekeeper 对打）。
    pub fn register_network(&self, req: RegisterNetworkRequest) -> CoreResult<RegisterNetworkAck> {
        let ack = |accepted: bool, message: String| RegisterNetworkAck {
            accepted,
            message,
            match_task_hint: String::new(),
        };
        let best = self.ports.networks.get_best()?;
        if best.as_ref().is_some_and(|b| b.sha == req.sha) {
            return Ok(ack(false, "sha_equals_current_best".into()));
        }
        let format = match normalize_format(&req.format) {
            Ok(f) => f,
            Err(msg) => return Ok(ack(false, msg)),
        };
        let created = self.ports.networks.register_network(
            &req.sha,
            (!req.parent_sha.is_empty()).then_some(req.parent_sha.as_str()),
            (!req.notes.is_empty()).then_some(req.notes.as_str()),
            &format,
        )?;
        if !created {
            return Ok(ack(false, format!("duplicate_sha:{}", req.sha)));
        }
        match best {
            None => {
                // 首个网络直接晋级
                self.ports.networks.promote_best(&req.sha)?;
                tracing::info!("[network] first network promoted sha={}", req.sha);
                // 新 best 入队绝对强度评估（按 every_n_promotions 节流；judgment-core 填充）
                self.evalc.on_new_best(&req.sha)?;
                Ok(ack(true, "first_network_promoted".into()))
            }
            Some(best) => {
                // 自动创建 gatekeeper 对打（对手 = 登记时 best 快照，不动态追 best）
                let match_id = self.ports.matches.create_match(
                    &req.sha,
                    &best.sha,
                    self.cfg.selfplay.gatekeeper_games as i64,
                )?;
                tracing::info!(
                    "[gatekeeper] match={} created candidate={} vs best={} target_pairs={}",
                    match_id, req.sha, best.sha, self.cfg.selfplay.gatekeeper_games
                );
                let mut a = ack(true, String::new());
                a.match_task_hint = format!("gatekeeper match {match_id} vs {}", best.sha);
                Ok(a)
            }
        }
    }

    /// 网络直传预签名 PUT（须声明权重格式，白名单校验）。
    pub fn sign_network_upload(
        &self,
        req: SignNetworkUploadRequest,
    ) -> CoreResult<SignNetworkUploadAck> {
        let ack = |accepted: bool, message: String| SignNetworkUploadAck {
            accepted,
            message,
            upload_url: String::new(),
            object_key: String::new(),
        };
        let sha = req.sha.trim();
        if sha.len() != 64 {
            return Ok(ack(
                false,
                format!("invalid_sha_len={} (want 64 hex chars)", sha.len()),
            ));
        }
        let format = match normalize_format(&req.format) {
            Ok(f) => f,
            Err(msg) => return Ok(ack(false, msg)),
        };
        let key = network_key(sha, &format);
        let url = self.signer.presign_put(&key, self.ttl_secs())?;
        tracing::info!("[network] sign upload trainer={} key={} len={}", req.trainer_id, key, req.content_length);
        Ok(SignNetworkUploadAck {
            accepted: true,
            upload_url: url,
            object_key: key,
            ..Default::default()
        })
    }

    /// trainer 游标分页拉 episode 预签名 GET 列表（可按 kind 过滤）。
    pub fn list_episodes(&self, req: ListEpisodesRequest) -> CoreResult<ListEpisodesReply> {
        let kind = req.kind.map(|k| k as i64);
        let keys = self.ports.episodes.list_episode_keys(&req.after_key, req.limit as i64, kind)?;
        let mut objects = Vec::with_capacity(keys.len());
        for k in &keys {
            let url = self.signer.presign_get(k, self.ttl_secs())?;
            objects.push(EpisodeObject { object_key: k.clone(), download_url: url, size: 0 });
        }
        // 游标语义：client 以最后一个对象键作 after_key 续页（服务端按登记顺序推进）
        let has_more = !keys.is_empty() && keys.len() as i32 == req.limit && req.limit > 0;
        Ok(ListEpisodesReply { objects, has_more })
    }

    /// trainer 提交局面重搜载荷（异步入队；拒绝：未启用 / 变体不符 / 空 / 队列满）。
    pub fn submit_reanalysis(
        &self,
        req: SubmitReanalysisRequest,
    ) -> CoreResult<SubmitReanalysisReply> {
        let reply = |accepted: bool, message: String, queued: u64| SubmitReanalysisReply {
            accepted,
            message,
            queued_positions: queued,
        };
        if !self.reana.enabled() {
            return Ok(reply(
                false,
                "服务端未启用局面重搜（配置 reanalysis.interval_tasks = 0）".into(),
                0,
            ));
        }
        if req.variant != self.cfg.selfplay.variant {
            return Ok(reply(
                false,
                format!(
                    "变体不一致：请求 \"{}\"，服务端 \"{}\"（拒绝跨变体重搜）",
                    req.variant, self.cfg.selfplay.variant
                ),
                0,
            ));
        }
        if req.payload.is_empty() {
            return Ok(reply(false, "重搜载荷为空".into(), 0));
        }
        let item = PendingReanalysis {
            variant: req.variant,
            mcts_sims: req.mcts_sims,
            payload: req.payload,
            positions: req.positions,
            requester: req.requester.clone(),
            created_at: self.clock.now(),
        };
        let (queued, ok) = self.reana.enqueue(item);
        if !ok {
            return Ok(reply(
                false,
                format!(
                    "重搜队列已满（上限 {} 批），本次丢弃：请降低提交频率或提高配置 reanalysis.max_queue",
                    self.cfg.reanalysis.max_queue
                ),
                queued,
            ));
        }
        tracing::info!(
            "[reanalysis] 入队 requester={} positions={} sims={} 队内待重搜位置={}",
            req.requester, req.positions, req.mcts_sims, queued
        );
        Ok(reply(true, String::new(), queued))
    }

    /// 特征包元数据登记 + 签发预签名 PUT（幂等：已登记直接返回既有键）。
    pub fn report_feature_pack(&self, req: FeaturePackMeta) -> CoreResult<FeaturePackAck> {
        let ack = |accepted: bool, message: String| FeaturePackAck {
            accepted,
            message,
            upload_url: String::new(),
            object_key: String::new(),
        };
        let mut version = req.pack_version as i64;
        if version <= 0 {
            version = DEFAULT_FEATURE_PACK_VERSION;
        }
        if req.episode_object_key.is_empty() {
            return Ok(ack(false, "missing_episode_object_key".into()));
        }
        let derived = feature_pack_key(&req.episode_object_key, version);
        // 幂等 get-or-insert：同 (episode_key, version) 冲突时保留首行（R2 对象内容
        // 由 sha256 校验，重复上传无害）
        let key = match self.ports.feature_packs.get_feature_pack(&req.episode_object_key, version)? {
            Some(existing) => existing.object_key,
            None => {
                self.ports.feature_packs.insert_feature_pack(FeaturePackDraft {
                    episode_object_key: req.episode_object_key.clone(),
                    pack_version: version,
                    object_key: derived.clone(),
                    created_at: self.clock.now(),
                })?;
                derived
            }
        };
        let url = self.signer.presign_put(&key, self.ttl_secs())?;
        // 补派任务收尾：标记完成（在线自对弈路径 task_id 属 selfplay 任务，
        // mark_done 对已完成任务幂等）
        self.disp.mark_done(&req.task_id);
        tracing::info!(
            "[feature-pack] worker={} task={} episode={} pack_v{} -> {}",
            req.worker_id, req.task_id, req.episode_object_key, version, key
        );
        Ok(FeaturePackAck { accepted: true, upload_url: url, object_key: key, ..Default::default() })
    }

    /// 特征包 get-or-enqueue（记录在、包缺失时入队补派）。
    pub fn request_feature_pack(&self, req: FeaturePackRequest) -> CoreResult<FeaturePackReply> {
        let mut version = req.pack_version as i64;
        if version <= 0 {
            version = DEFAULT_FEATURE_PACK_VERSION;
        }
        if req.episode_object_key.is_empty() {
            return Ok(FeaturePackReply {
                accepted: false,
                message: "missing_episode_object_key".into(),
                ..Default::default()
            });
        }
        if let Some(existing) = self.ports.feature_packs.get_feature_pack(&req.episode_object_key, version)? {
            let url = self.signer.presign_get(&existing.object_key, self.ttl_secs())?;
            return Ok(FeaturePackReply {
                accepted: true,
                ready: true,
                object_key: existing.object_key,
                download_url: url,
                ..Default::default()
            });
        }
        let ok = self.derive.enqueue(PendingDerive {
            episode_object_key: req.episode_object_key.clone(),
            pack_version: version,
            requester: req.requester.clone(),
            created_at: self.clock.now(),
        });
        if !ok {
            return Ok(FeaturePackReply {
                accepted: false,
                message: format!(
                    "特征包补派队列已满（上限 {}），稍后重试",
                    crate::consts::DERIVE_QUEUE_MAX
                ),
                ..Default::default()
            });
        }
        tracing::info!(
            "[feature-pack] 补派入队 requester={} episode={} pack_v{} 队列={}",
            req.requester, req.episode_object_key, version, self.derive.pending()
        );
        Ok(FeaturePackReply {
            accepted: true,
            ready: false,
            message: "queued_for_derivation".into(),
            ..Default::default()
        })
    }

    /// 可远程调整训练超参：变体不符时 accepted=false；否则下发全部覆盖项。
    /// 编排依赖 judgment-core 的 trainconfig 白名单（Control.train_config_overrides
    /// 由其填充；未填充前运行时 todo!() panic，编译不受影响——集成期启用）。
    pub fn get_train_config(&self, req: GetTrainConfigRequest) -> CoreResult<GetTrainConfigReply> {
        if req.variant != self.cfg.selfplay.variant {
            return Ok(GetTrainConfigReply {
                accepted: false,
                message: format!(
                    "变体不符：请求 \"{}\"，服务端 \"{}\"",
                    req.variant, self.cfg.selfplay.variant
                ),
                ..Default::default()
            });
        }
        // 全量替换语义的当前覆盖集（调度器是权威裁定方）
        let overrides: HashMap<String, String> = self.ctl.train_config_overrides().into_iter().collect();
        Ok(GetTrainConfigReply {
            accepted: true,
            overrides,
            ..Default::default()
        })
    }

    /// trainer 消费心跳：登记水位 → 平衡即时重估 → 回传生效暂停与自动暂停原因。
    /// 业务体（变体校验 / 水位映射 / UPSERT / 进度观测 / 强制重估）在
    /// BalanceMonitor::register_trainer_heartbeat（judgment-core），门面只做
    /// proto ↔ 业务输入转换。
    pub fn trainer_heartbeat(
        &self,
        req: TrainerHeartbeatRequest,
    ) -> CoreResult<TrainerHeartbeatReply> {
        let outcome = self.balance.register_trainer_heartbeat(TrainerHeartbeatInput {
            trainer_id: req.trainer_id,
            variant: req.variant,
            kind: req.kind as i64,
            consumed_key: req.consumed_key,
            samples_consumed: req.samples_consumed,
            backlog: req.backlog as i64,
            buffered: req.buffered as i64,
            health_status: req.health_status,
            last_error: req.last_error,
            progress_json: req.progress_json,
        })?;
        Ok(TrainerHeartbeatReply {
            pause_self_play: self.ctl.effective_paused(),
            auto_paused: self.ctl.auto_paused(),
            auto_pause_reason: self.ctl.auto_pause_reason(),
            message: outcome.message,
        })
    }
}

// ---------------------------------------------------------------------------
// 辅助：client_version 比较与权重格式白名单
// ---------------------------------------------------------------------------

/// client_version 是否低于 min。
///
/// **与 Go 版的刻意差异**：Go 版 GetTask 用字典序比较（`client < min`），这是
/// 已知缺陷——`0.10.0 < 0.9.0` 会被误判为过旧而拒绝派单。这里改为语义化版本
/// 感知：按 `.` 分段解析为数值逐段比较（缺失段补 0，`0.10.0 > 0.9.0`）；
/// 任一段非纯数字（如 `v1.2-rc`）时回退字典序比较，保证不 panic。
fn client_version_too_old(client: &str, min: &str) -> bool {
    let parse = |s: &str| -> Option<Vec<Option<u64>>> {
        let mut parts = Vec::new();
        for seg in s.split('.') {
            match seg.parse::<u64>() {
                Ok(n) => parts.push(Some(n)),
                Err(_) => return None, // 含非数字段 → 回退字典序
            }
        }
        Some(parts)
    };
    match (parse(client), parse(min)) {
        (Some(c), Some(m)) => {
            let len = c.len().max(m.len());
            for i in 0..len {
                let cv = c.get(i).copied().flatten().unwrap_or(0);
                let mv = m.get(i).copied().flatten().unwrap_or(0);
                if cv != mv {
                    return cv < mv;
                }
            }
            false
        }
        // 任一版本非纯数字分段 → 保持 Go 版字典序行为
        _ => client < min,
    }
}

/// 归一化并校验上传/登记声明的权重格式（白名单 `WEIGHT_FORMATS`）。
fn normalize_format(format: &str) -> Result<String, String> {
    let f = format.trim().to_ascii_lowercase();
    if crate::consts::WEIGHT_FORMATS.contains(&f.as_str()) {
        Ok(f)
    } else {
        Err(format!("unsupported_format=\"{format}\" (want onnx/pt/nnue)"))
    }
}

// ===========================================================================
// 测试
// ===========================================================================

#[cfg(test)]
mod tests;
