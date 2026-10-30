//! 【所有权：core-pipeline 成员】
//!
//! 上报路由链（reportSink）：ReportMatchResult 的「公共闸口 → 逐 sink 尝试 →
//! 链尾兜底」编排。与 taskSource 对称。
//!
//! 参考实现：`banqi-scheduler/internal/scheduler/reports.go`。
//! **保序铁律：先 DB 落库，后置终态**（rollback_report 可回滚）。

use std::sync::Arc;

use bqs_config::Config;
use bqs_judgment::{judge, new_bounds, Hexanomial, Verdict};
use bqs_proto::{MatchResult, MatchResultAck, TaskKind};

use crate::{
    dispatch::{RunningTask, TaskDispatcher, TaskState},
    evalcoord::EvalCoordinator,
    gatekeeper::Gatekeeper,
    jobs::{JobManager, RatingSpecJson},
    ports::{MatchStore, NetworkStore},
    CoreError, CoreResult,
};

/// 公共闸口：任务查找 + worker 归属校验（ReportEpisode / ReportMatchResult 两条
/// 上报路径共用）。校验失败时返回 `Err(rejection ack)`（业务回包，不报 gRPC
/// 错误，worker 据 ack.message 处置）。
pub fn lookup_report_task(
    disp: &TaskDispatcher,
    task_id: &str,
    worker_id: &str,
) -> Result<RunningTask, MatchResultAck> {
    let Some(task) = disp.lookup(task_id) else {
        return Err(MatchResultAck {
            accepted: false,
            message: format!("unknown_task_id:{task_id}"),
            ..Default::default()
        });
    };
    if task.worker_id != worker_id {
        return Err(MatchResultAck {
            accepted: false,
            message: format!("worker_mismatch task_owner={} got={}", task.worker_id, worker_id),
            ..Default::default()
        });
    }
    Ok(task)
}

/// 上报聚合路由：evalSink（TASK_EVAL，只落 eval_results）→ gateSink（GSPRT 聚合，
/// rating job 走专属判停分支）→ 链尾兜底 task_is_not_gating。
pub struct ReportRouter {
    pub evalc: Arc<EvalCoordinator>,
    pub gate: Arc<Gatekeeper>,
    pub disp: Arc<TaskDispatcher>,
    /// 统一 Job 机制：eval / rating 上报的 job 完成口径聚合（completed += games）。
    pub jobs: Arc<JobManager>,
    /// rating job 聚合载体（matches 六桶累加 + 条件结转/取消）。
    pub matches: Arc<dyn MatchStore>,
    pub nets: Arc<dyn NetworkStore>,
    pub cfg: Arc<Config>,
}

impl ReportRouter {
    pub fn new(
        evalc: Arc<EvalCoordinator>,
        gate: Arc<Gatekeeper>,
        disp: Arc<TaskDispatcher>,
        jobs: Arc<JobManager>,
        matches: Arc<dyn MatchStore>,
        nets: Arc<dyn NetworkStore>,
        cfg: Arc<Config>,
    ) -> Self {
        Self { evalc, gate, disp, jobs, matches, nets, cfg }
    }

    fn already_reported(task_id: &str) -> MatchResultAck {
        MatchResultAck {
            accepted: true,
            message: format!("already_reported:{task_id}"),
            ..Default::default()
        }
    }

    /// evalSink：TASK_EVAL 上报聚合，只落 eval_results 并更新停机判据，不参与
    /// GSPRT 与晋级。eval 在飞窗口过期靠重下发兜底（attempts 计数），迟到上报
    /// 不聚合（任务已非 Assigned 态）。
    fn eval_sink(&self, task: &RunningTask, req: &MatchResult) -> CoreResult<Option<MatchResultAck>> {
        if task.kind != TaskKind::TaskEval {
            return Ok(None);
        }
        if task.state != TaskState::Assigned {
            return Ok(Some(Self::already_reported(&req.task_id)));
        }
        let ack = self.evalc.report_eval_result(task, req)?;
        // 统一 Job 机制：eval job 的完成口径聚合（按 job_tasks 映射；无映射的
        // 非 job 任务静默忽略）。先聚合（eval_queue 累加已在 report_eval_result
        // 内落库），失败不回滚任务终态（job 计数行缺失时映射行已删，无双计）。
        if ack.accepted {
            if let Err(e) = self.jobs.record_report(&req.task_id, req.games as i64) {
                tracing::warn!("[job] ⚠️ eval 上报聚合失败 task={}: {e}", req.task_id);
            }
        }
        Ok(Some(ack))
    }

    /// gateSink：参与 gatekeeper 聚合的两种任务——显式 TASK_RATING（滚动升级
    /// 期间在飞的旧任务仍可正常结算）与混入模式 TASK_SELFPLAY（MatchID != 0）。
    /// 普通纯自对弈 selfplay（MatchID == 0）没有五项计数，不消费（交链尾）。
    ///
    /// rating job 派发的 TASK_RATING（task.job_id 有映射）走 rating 专属分支：
    /// SPRT 参数来自 job spec（0 回落 [sprt] 配置），纯相对强度测量 + SPRT
    /// 判停（不晋级、不标 rejected——晋级只属于 gatekeeper 既有判据链）。
    fn gate_sink(&self, task: &RunningTask, req: &MatchResult) -> CoreResult<Option<MatchResultAck>> {
        let is_gating = task.kind == TaskKind::TaskRating
            || (task.kind == TaskKind::TaskSelfplay && task.match_id != 0);
        if !is_gating {
            return Ok(None);
        }
        // CAS 抢占上报权（含 Expired 迟到上报）：失败 = 已有并发/前序上报完成
        if !self.disp.claim_report(&req.task_id) {
            return Ok(Some(Self::already_reported(&req.task_id)));
        }
        // 六桶成对计数（换色配对，互斥对型 [LL, LD, DD, WL, DW, WW]，与 proto
        // pair_* 字段一一对应）
        let delta = Hexanomial([
            req.pair_ll as i64,
            req.pair_ld as i64,
            req.pair_dd as i64,
            req.pair_wl as i64,
            req.pair_dw as i64,
            req.pair_ww as i64,
        ]);
        if task.kind == TaskKind::TaskRating {
            if let Some(spec) = self.jobs.rating_spec(&task.job_id)? {
                return match self.rating_report(task, req, delta, &spec) {
                    Ok(ack) => Ok(Some(ack)),
                    Err(e) => {
                        // F4-P1-3：落库成功才保持终态；失败回滚，worker 重试可重新抢占
                        self.disp.rollback_report(&req.task_id);
                        Err(e)
                    }
                };
            }
            // 无 job 映射：旧版显式 TASK_RATING（滚动升级期间在飞的遗留任务），
            // 走 gatekeeper 既有判据链。
        }
        match self.gate.accept_report(task.match_id, delta, req.games as i64) {
            Ok(d) => Ok(Some(MatchResultAck {
                accepted: d.accepted,
                message: d.message,
                match_concluded: d.concluded,
                promoted: d.promoted,
                best_sha: d.best_sha,
            })),
            Err(e) => {
                // F4-P1-3：落库成功才保持终态；失败回滚，worker 重试可重新抢占
                self.disp.rollback_report(&req.task_id);
                Err(e)
            }
        }
    }

    /// rating job 聚合分支：六桶累加进对发行（多 worker 并发贡献，SQL 原子）→
    /// 每报必跑 SPRT（打满局数上限由 total_games 截停，不用 judge_match 的
    /// 预算强制拒绝——rating 的停机口径是「SPRT 出结论 或 局数达上限」）→
    /// 出结论即结转（仅判停收尾）。
    ///
    /// 手动 SPRT/job 对打**永不触发晋级判定**（promoted 恒 false，不调用
    /// promote/eval 入队，也不标 rejected）——晋级只属于训练闭环自身的
    /// gatekeeper 流程（无 job 映射的旧版 TASK_RATING 走 gate.accept_report
    /// 既有判据链，行为不变）。rating job = 纯相对强度测量 + SPRT 动态判停，
    /// 无论对手是否为当前 best 均仅观测统计。
    ///
    /// 判停/达上限即收敛 job（completed + 取消在飞）；对打已 cancelled（job 被
    /// cancel）的上报经 add_match_games 的 running 闸门拒绝。
    fn rating_report(
        &self,
        task: &RunningTask,
        req: &MatchResult,
        delta: Hexanomial,
        spec: &RatingSpecJson,
    ) -> CoreResult<MatchResultAck> {
        let reject = |message: &str| MatchResultAck {
            accepted: false,
            message: message.to_string(),
            ..Default::default()
        };
        let Some(m) = self.matches.get_match(task.match_id)? else {
            return Ok(reject(&format!("match_{}_not_found", task.match_id)));
        };
        let (agg, running) = self.matches.add_match_games(m.id, delta, req.games as i64)?;
        if !running {
            return Ok(reject(&format!("match_{}_not_running", m.id)));
        }
        // job 完成口径聚合（total>0 达上限时内部会置 completed + 取消在飞）。
        if let Err(e) = self.jobs.record_report(&req.task_id, req.games as i64) {
            tracing::warn!("[job] ⚠️ rating 上报聚合失败 task={}: {e}", req.task_id);
        }
        // SPRT 参数：job spec 非 0 覆盖，否则回落 [sprt] 配置。
        let sp = &self.cfg.sprt;
        let elo0 = if spec.sprt_elo0 != 0.0 { spec.sprt_elo0 } else { sp.elo0 };
        let elo1 = if spec.sprt_elo1 != 0.0 { spec.sprt_elo1 } else { sp.elo1 };
        let alpha = if spec.sprt_alpha != 0.0 { spec.sprt_alpha } else { sp.alpha };
        let beta = if spec.sprt_beta != 0.0 { spec.sprt_beta } else { sp.beta };
        let (verdict, llr) = judge(agg.pairs, elo0, elo1, new_bounds(alpha, beta))
            .map_err(|e| CoreError::Internal(format!("rating sprt judge match={} : {e}", m.id)))?;
        let cap_reached = spec.total_games > 0 && agg.num_games >= spec.total_games as i64;
        let mut concluded = false;
        // 手动 SPRT/job 对打不参与晋级：accept/reject 仅判停结转（观测统计），
        // promoted 恒 false；best_sha 回显当前值。
        let promoted = false;
        let best_sha = self.best_sha()?;
        if verdict != Verdict::Continue || cap_reached {
            // 结转（CAS）：并发双判停只有一个调用方执行结转副作用。
            concluded = self.matches.conclude_match(m.id)?;
        }
        if concluded {
            self.jobs.complete_rating_job(&task.job_id)?;
        }
        tracing::info!(
            "[rating] match={} job={} verdict={} llr={:.3} cap={} concluded={concluded} promoted={promoted} pairs={}",
            m.id, task.job_id, verdict.as_str(), llr, cap_reached, agg.pairs.labeled()
        );
        Ok(MatchResultAck {
            accepted: true,
            message: String::new(),
            match_concluded: concluded,
            promoted,
            best_sha,
        })
    }

    fn best_sha(&self) -> CoreResult<String> {
        Ok(self.nets.get_best()?.map(|b| b.sha).unwrap_or_default())
    }

    /// 完整路由：公共闸口 → 幂等快速短路 → 逐 sink 尝试 → 链尾兜底。
    pub fn route(&self, req: &MatchResult) -> CoreResult<MatchResultAck> {
        let task = match lookup_report_task(&self.disp, &req.task_id, &req.worker_id) {
            Ok(t) => t,
            Err(rejected) => return Ok(rejected),
        };
        // 幂等快速短路：终态 Completed 的重复上报直接返回（权威闸口是各 sink 的 CAS）
        if task.state == TaskState::Completed {
            return Ok(Self::already_reported(&req.task_id));
        }
        if let Some(ack) = self.eval_sink(&task, req)? {
            return Ok(ack);
        }
        if let Some(ack) = self.gate_sink(&task, req)? {
            return Ok(ack);
        }
        Ok(MatchResultAck {
            accepted: false,
            message: "task_is_not_gating".into(),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use bqs_proto::{JobKind, RatingJobSpec, SubmitJobRequest, TaskKind as ProtoTaskKind};

    use super::*;
    use crate::test_support::Fakes;
    use std::sync::Arc;

    struct Env {
        f: Fakes,
        disp: Arc<TaskDispatcher>,
        router: ReportRouter,
        jobs: Arc<JobManager>,
    }

    const CFG: &str = "[sprt]\nelo0 = 0.0\nelo1 = 15.0\nalpha = 0.05\nbeta = 0.05\n";

    fn env() -> Env {
        let f = Fakes::new();
        let cfg = Arc::new(Config::parse(CFG, "test").unwrap());
        let ports = f.ports();
        let ctl = Arc::new(crate::control::Control::new(&ports, cfg.clone()));
        let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), f.clock.clone()));
        let evalc = Arc::new(EvalCoordinator::new(
            &ports,
            cfg.clone(),
            f.clock.clone(),
            ctl.clone(),
            disp.clone(),
        ));
        let gate = Arc::new(Gatekeeper::new(&ports, cfg.clone(), evalc.clone()));
        let jobs = Arc::new(JobManager::new(
            ports.jobs.clone(),
            disp.clone(),
            ports.networks.clone(),
            ports.matches.clone(),
            ctl,
            cfg.clone(),
            f.clock.clone(),
        ));
        let router = ReportRouter::new(
            evalc.clone(),
            gate,
            disp.clone(),
            jobs.clone(),
            ports.matches.clone(),
            ports.networks.clone(),
            cfg,
        );
        Env { f, disp, router, jobs }
    }

    /// 预置谱系并提交一个 rating job（对手空 = 当前 best），返回 (job_id, match_id)。
    fn seed_rating_job(e: &Env, total: u64, opponent: &str) -> (String, i64) {
        e.f.networks.seed_network("best", None, "onnx");
        e.f.networks.promote_best("best").unwrap();
        e.f.networks.seed_network("cand", None, "onnx");
        e.f.networks.seed_network("fixed", None, "onnx");
        let req = SubmitJobRequest {
            requester: "test".into(),
            kind: JobKind::JobRating as i32,
            priority: 0,
            rating: Some(RatingJobSpec {
                variant: String::new(),
                candidate_sha: "cand".into(),
                opponent_sha: opponent.into(),
                total_games: total,
                ..Default::default()
            }),
            ..Default::default()
        };
        let (ok, msg, job_id) = e.jobs.submit(&req).unwrap();
        assert!(ok, "{msg}");
        let row = e.jobs.get(&job_id).unwrap().unwrap();
        let Some(crate::jobs::JobSpecJson::Rating(spec)) =
            crate::jobs::JobSpecJson::parse(&row.spec_json)
        else {
            panic!("rating spec json");
        };
        (job_id, spec.match_id)
    }

    /// 派发一个在飞 rating 任务（模拟 JobSource::dispatch_rating 的记账）。
    fn assign_rating_task(e: &Env, task_id: &str, job_id: &str, match_id: i64, worker: &str) {
        let mut t = RunningTask::new(ProtoTaskKind::TaskRating);
        t.worker_id = worker.into();
        t.network_sha = "cand".into();
        t.opponent_sha = "best".into();
        t.games = 4;
        t.match_id = match_id;
        t.job_id = job_id.into();
        e.disp.register(task_id, t);
        e.jobs.note_dispatch(task_id, job_id, 4, worker, ProtoTaskKind::TaskRating, "cand", "best", "", match_id, None).unwrap();
    }

    fn rating_result(worker: &str, task_id: &str, pairs: [i64; 6], games: i64) -> MatchResult {
        MatchResult {
            worker_id: worker.into(),
            task_id: task_id.into(),
            kind: ProtoTaskKind::TaskRating as i32,
            network_sha: "cand".into(),
            opponent_sha: "best".into(),
            games: games as i32,
            pair_ll: pairs[0] as i32,
            pair_ld: pairs[1] as i32,
            pair_dd: pairs[2] as i32,
            pair_wl: pairs[3] as i32,
            pair_dw: pairs[4] as i32,
            pair_ww: pairs[5] as i32,
            ..Default::default()
        }
    }

    #[test]
    fn rating_accept_never_promotes_even_vs_best() {
        let e = env();
        let (job_id, match_id) = seed_rating_job(&e, 0, "");
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");
        // 100 对全 WW → LLR 大幅上穿接受界 → 判停结转，但不晋级（手动 SPRT/job
        // 对打永不触发晋级判定；晋级只属于 gatekeeper 既有判据链）
        // 另一个 worker 的在飞任务先登记（上报任务自身已完成，不在取消集合）
        assign_rating_task(&e, "rt0", &job_id, match_id, "w2");
        let ack = e.router.route(&rating_result("w1", "rt1", [0, 0, 0, 0, 0, 100], 200)).unwrap();
        assert!(ack.accepted && ack.match_concluded);
        assert!(!ack.promoted, "rating job 永不晋级");
        assert_eq!(ack.best_sha, "best", "best 不变");
        assert_eq!(e.f.networks.best_sha().as_deref(), Some("best"));
        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("candidate"), "候选不被标记");
        // job 收敛 + 在飞取消（上报任务自身已 Completed，仅其余在飞被取消）
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
        assert_eq!(e.jobs.cancel_task_ids_for_worker("w2"), vec!["rt0".to_string()]);
        assert!(e.jobs.cancel_task_ids_for_worker("w1").is_empty());
        // 对发行已结转：迟到上报被拒
        assign_rating_task(&e, "rt2", &job_id, match_id, "w2");
        let ack2 = e.router.route(&rating_result("w2", "rt2", [1, 0, 0, 0, 0, 0], 2)).unwrap();
        assert!(!ack2.accepted);
        assert!(ack2.message.contains("not_running"), "{}", ack2.message);
    }

    #[test]
    fn rating_reject_observes_only_never_marks() {
        let e = env();
        let (job_id, match_id) = seed_rating_job(&e, 0, "");
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");
        // 100 对全 LL → 显著劣势 → 仅判停结转，不标 rejected、不晋级（观测统计）
        let ack = e.router.route(&rating_result("w1", "rt1", [100, 0, 0, 0, 0, 0], 200)).unwrap();
        assert!(ack.accepted && ack.match_concluded && !ack.promoted);
        assert_eq!(e.f.networks.best_sha().as_deref(), Some("best"));
        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("candidate"));
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
    }

    #[test]
    fn rating_fixed_opponent_observes_only() {
        let e = env();
        // 对手 = 指定网络（非 best）：AcceptH1 也只观测，不晋级不标 rejected
        let (job_id, match_id) = seed_rating_job(&e, 0, "fixed");
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");
        let ack = e.router.route(&rating_result("w1", "rt1", [0, 0, 0, 0, 0, 100], 200)).unwrap();
        assert!(ack.accepted && ack.match_concluded && !ack.promoted);
        assert_eq!(e.f.networks.best_sha().as_deref(), Some("best"));
        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("candidate"));
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
    }

    #[test]
    fn rating_total_games_cap_stops_without_verdict() {
        let e = env();
        // total=20 局：打满后仍在判定带内 → 结转但无 SPRT 结论、不晋级
        let (job_id, match_id) = seed_rating_job(&e, 20, "");
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");
        // 先报 16 局（约均衡）：LLR 带内，未达上限
        let ack = e.router.route(&rating_result("w1", "rt1", [0, 0, 8, 0, 0, 0], 16)).unwrap();
        assert!(ack.accepted && !ack.match_concluded);
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "running");
        // 再报 4 局：达 20 局上限 → 判停收敛
        assign_rating_task(&e, "rt2", &job_id, match_id, "w1");
        let ack = e.router.route(&rating_result("w1", "rt2", [0, 0, 2, 0, 0, 0], 4)).unwrap();
        assert!(ack.accepted && ack.match_concluded && !ack.promoted);
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
        assert_eq!(e.f.matches.get_match(match_id).unwrap().unwrap().num_games, 20);
        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("candidate"));
    }

    #[test]
    fn rating_job_cancel_rejects_late_reports() {
        let e = env();
        let (job_id, match_id) = seed_rating_job(&e, 0, "");
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");
        // cancel：对打置 cancelled
        assert!(e.jobs.control(&job_id, "cancel").unwrap().0);
        let ack = e.router.route(&rating_result("w1", "rt1", [0, 0, 2, 0, 0, 0], 4)).unwrap();
        assert!(!ack.accepted);
        assert!(ack.message.contains("not_running"), "{}", ack.message);
        // 候选不被污染
        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("candidate"));
    }

    #[test]
    fn rating_sprt_params_from_spec_override_config() {
        let e = env();
        // spec 带 elo1=1.0 的紧判定带：少量优势即 AcceptH1（配置 elo1=15 不生效）
        e.f.networks.seed_network("best", None, "onnx");
        e.f.networks.promote_best("best").unwrap();
        e.f.networks.seed_network("cand", None, "onnx");
        e.f.networks.seed_network("fixed", None, "onnx");
        let (ok, msg, job_id) = e
            .jobs
            .submit(&SubmitJobRequest {
                requester: "test".into(),
                kind: JobKind::JobRating as i32,
                priority: 0,
                rating: Some(RatingJobSpec {
                    candidate_sha: "cand".into(),
                    opponent_sha: "fixed".into(),
                    total_games: 0,
                    // elo1=100 的宽 H1：少量样本即越上界；配置 elo1=15 同样本仍带内
                    sprt_elo1: 100.0,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap();
        assert!(ok, "{msg}");
        let row = e.jobs.get(&job_id).unwrap().unwrap();
        let Some(crate::jobs::JobSpecJson::Rating(spec)) =
            crate::jobs::JobSpecJson::parse(&row.spec_json)
        else {
            panic!("rating spec json");
        };
        assign_rating_task(&e, "rt1", &job_id, spec.match_id, "w1");
        // 6 对全 WW（12 局）：spec 的 elo1=100 下 LLR 越上界判停（配置 elo1=15
        // 同样本仍在带内 → 覆盖生效）
        let ack = e.router.route(&rating_result("w1", "rt1", [0, 0, 0, 0, 0, 6], 12)).unwrap();
        assert!(ack.accepted && ack.match_concluded, "spec SPRT 覆盖应提前判停");
        // 对照：同样本按配置参数（elo1=15）走 gatekeeper 判据链不判停
        //（带 target 的对打：12 局 < 2×10 对且 6 对 < 10 对 → Continue）
        e.f.networks.seed_network("cand2", None, "onnx");
        let match_id2 = e.f.matches.seed_match("cand2", "fixed", 10);
        let mut t2 = RunningTask::new(ProtoTaskKind::TaskRating);
        t2.worker_id = "w3".into();
        t2.match_id = match_id2;
        t2.games = 12;
        e.disp.register("rt2", t2);
        let ack2 = e.router.route(&rating_result("w3", "rt2", [0, 0, 0, 0, 0, 6], 12)).unwrap();
        assert!(ack2.accepted && !ack2.match_concluded);
    }

    #[test]
    fn legacy_rating_task_without_job_mapping_uses_gatekeeper_chain() {
        let e = env();
        // 无 job 映射的旧版 TASK_RATING：走 gatekeeper 既有判据链（judge_match
        // 的预算强制拒绝语义）
        e.f.networks.seed_network("best", None, "onnx");
        e.f.networks.promote_best("best").unwrap();
        e.f.networks.seed_network("cand", None, "onnx");
        let match_id = e.f.matches.seed_match("cand", "best", 10);
        let mut t = RunningTask::new(ProtoTaskKind::TaskRating);
        t.worker_id = "w1".into();
        t.match_id = match_id;
        t.games = 20;
        e.disp.register("lt1", t);
        // 打满 20 局全 DD ≈ 带内 → gatekeeper 强制 reject + 标记 rejected
        let ack = e.router.route(&rating_result("w1", "lt1", [0, 0, 10, 0, 0, 0], 20)).unwrap();
        assert!(ack.accepted && ack.match_concluded && !ack.promoted);
        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("rejected"));
    }
}
