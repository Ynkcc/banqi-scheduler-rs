use std::sync::Arc;

use bqs_config::Config;
use bqs_judgment::{Hexanomial, Verdict, judge, new_bounds};
use bqs_proto::{MatchResult, MatchResultAck, TaskKind};

use crate::{
    CoreError, CoreResult,
    dispatch::{RunningTask, TaskDispatcher, TaskState},
    jobs::{JobManager, RatingSpecJson},
    ports::MatchStore,
};

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
            message: format!(
                "worker_mismatch task_owner={} got={}",
                task.worker_id, worker_id
            ),
            ..Default::default()
        });
    }
    Ok(task)
}

pub struct ReportRouter {
    pub disp: Arc<TaskDispatcher>,

    pub jobs: Arc<JobManager>,

    pub matches: Arc<dyn MatchStore>,
    pub cfg: Arc<Config>,
}

impl ReportRouter {
    pub fn new(
        disp: Arc<TaskDispatcher>,
        jobs: Arc<JobManager>,
        matches: Arc<dyn MatchStore>,
        cfg: Arc<Config>,
    ) -> Self {
        Self {
            disp,
            jobs,
            matches,
            cfg,
        }
    }

    fn already_reported(task_id: &str) -> MatchResultAck {
        MatchResultAck {
            accepted: true,
            message: format!("already_reported:{task_id}"),
            ..Default::default()
        }
    }

    fn rating_sink(
        &self,
        task: &RunningTask,
        req: &MatchResult,
    ) -> CoreResult<Option<MatchResultAck>> {
        if task.kind != TaskKind::TaskRating {
            return Ok(None);
        }
        let Some(spec) = self.jobs.rating_spec(&task.job_id)? else {
            return Ok(None);
        };

        if !self.disp.claim_report(&req.task_id) {
            return Ok(Some(Self::already_reported(&req.task_id)));
        }

        let delta = Hexanomial([
            req.pair_ll as i64,
            req.pair_ld as i64,
            req.pair_dd as i64,
            req.pair_wl as i64,
            req.pair_dw as i64,
            req.pair_ww as i64,
        ]);
        match self.rating_report(task, req, delta, &spec) {
            Ok(ack) => Ok(Some(ack)),
            Err(e) => {
                self.disp.rollback_report(&req.task_id);
                Err(e)
            }
        }
    }

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
        let (agg, running) = self
            .matches
            .add_match_games(m.id, delta, req.games as i64)?;
        if !running {
            return Ok(reject(&format!("match_{}_not_running", m.id)));
        }

        if let Err(e) = self.jobs.record_report(&req.task_id, req.games as i64) {
            tracing::warn!("[job] ⚠️ rating 上报聚合失败 task={}: {e}", req.task_id);
        }

        if spec.disable_sprt {
            let cap_reached = spec.total_games > 0 && agg.num_games >= spec.total_games as i64;
            let mut concluded = false;
            if cap_reached {
                concluded = self.matches.conclude_match_with_stats(
                    m.id,
                    crate::types::MatchConclusion {
                        verdict: "cap".to_string(),
                        llr: 0.0,
                        sprt_elo0: spec.sprt_elo0,
                        sprt_elo1: spec.sprt_elo1,
                        sprt_alpha: spec.sprt_alpha,
                        sprt_beta: spec.sprt_beta,
                    },
                )?;
            }
            if concluded {
                self.jobs.complete_rating_job(&task.job_id)?;
            }
            tracing::info!(
                "[rating] match={} job={} sprt=disabled cap={} concluded={concluded} pairs={}",
                m.id,
                task.job_id,
                cap_reached,
                agg.pairs.labeled()
            );
            return Ok(MatchResultAck {
                accepted: true,
                message: String::new(),
                match_concluded: concluded,
                ..Default::default()
            });
        }

        if spec.sprt_elo1 <= spec.sprt_elo0
            || !(0.0 < spec.sprt_alpha && spec.sprt_alpha < 1.0)
            || !(0.0 < spec.sprt_beta && spec.sprt_beta < 1.0)
        {
            tracing::warn!(
                "[rating] match={} job={} 旧格式 spec（sprt 参数退化 elo0={} elo1={} alpha={} beta={}），取消任务：请以显式 SPRT 参数重新提交 rating job",
                m.id,
                task.job_id,
                spec.sprt_elo0,
                spec.sprt_elo1,
                spec.sprt_alpha,
                spec.sprt_beta
            );
            let _ = self.matches.cancel_match(m.id)?;
            let _ = self.jobs.control(&task.job_id, "cancel")?;
            return Ok(reject(&format!("match_{}_legacy_sprt_params", m.id)));
        }
        let elo0 = spec.sprt_elo0;
        let elo1 = spec.sprt_elo1;
        let alpha = spec.sprt_alpha;
        let beta = spec.sprt_beta;
        let (verdict, llr) = judge(agg.pairs, elo0, elo1, new_bounds(alpha, beta))
            .map_err(|e| CoreError::Internal(format!("rating sprt judge match={} : {e}", m.id)))?;
        let cap_reached = spec.total_games > 0 && agg.num_games >= spec.total_games as i64;
        let mut concluded = false;

        if verdict != Verdict::Continue || cap_reached {
            concluded = self.matches.conclude_match_with_stats(
                m.id,
                crate::types::MatchConclusion {
                    verdict: verdict.as_str().to_string(),
                    llr,
                    sprt_elo0: elo0,
                    sprt_elo1: elo1,
                    sprt_alpha: alpha,
                    sprt_beta: beta,
                },
            )?;
        }
        if concluded {
            self.jobs.complete_rating_job(&task.job_id)?;
        }
        tracing::info!(
            "[rating] match={} job={} verdict={} llr={:.3} cap={} concluded={concluded} pairs={}",
            m.id,
            task.job_id,
            verdict.as_str(),
            llr,
            cap_reached,
            agg.pairs.labeled()
        );
        Ok(MatchResultAck {
            accepted: true,
            message: String::new(),
            match_concluded: concluded,
            ..Default::default()
        })
    }

    pub fn route(&self, req: &MatchResult) -> CoreResult<MatchResultAck> {
        let task = match lookup_report_task(&self.disp, &req.task_id, &req.worker_id) {
            Ok(t) => t,
            Err(rejected) => return Ok(rejected),
        };

        if task.state == TaskState::Completed {
            return Ok(Self::already_reported(&req.task_id));
        }
        if let Some(ack) = self.rating_sink(&task, req)? {
            return Ok(ack);
        }

        if !self.disp.claim_report(&req.task_id) {
            return Ok(Self::already_reported(&req.task_id));
        }
        self.disp.complete_in_flight(&req.task_id);
        tracing::info!(
            "[report] non-gating 对局统计收讫 task={} games={} kind={:?}",
            req.task_id,
            req.games,
            task.kind
        );
        Ok(MatchResultAck {
            accepted: true,
            message: "non_gating_stats_ack".into(),
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

    const CFG: &str = "";

    fn env() -> Env {
        let f = Fakes::new();
        let cfg = Arc::new(Config::parse(CFG, "test").unwrap());
        let ports = f.ports();
        let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), f.clock.clone()));
        let jobs = Arc::new(JobManager::new(
            ports.jobs.clone(),
            disp.clone(),
            ports.networks.clone(),
            ports.matches.clone(),
            cfg.clone(),
            f.clock.clone(),
        ));
        let router = ReportRouter::new(disp.clone(), jobs.clone(), ports.matches.clone(), cfg);
        Env {
            f,
            disp,
            router,
            jobs,
        }
    }

    fn seed_rating_job(e: &Env, total: u64) -> (String, i64) {
        seed_rating_job_opts(e, total, false)
    }

    fn seed_rating_job_opts(e: &Env, total: u64, disable_sprt: bool) -> (String, i64) {
        e.f.networks.seed_network("cand", None, "onnx");
        e.f.networks.seed_network("fixed", None, "onnx");
        let req = SubmitJobRequest {
            requester: "test".into(),
            kind: JobKind::JobRating as i32,
            priority: 0,
            rating: Some(RatingJobSpec {
                variant: String::new(),
                candidate_sha: "cand".into(),
                opponent_sha: "fixed".into(),
                total_games: total,
                sprt_elo0: 0.0,
                sprt_elo1: 15.0,
                sprt_alpha: 0.05,
                sprt_beta: 0.05,
                disable_sprt,
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

    fn assign_rating_task(e: &Env, task_id: &str, job_id: &str, match_id: i64, worker: &str) {
        let mut t = RunningTask::new(ProtoTaskKind::TaskRating);
        t.worker_id = worker.into();
        t.network_sha = "cand".into();
        t.opponent_sha = "fixed".into();
        t.games = 4;
        t.match_id = match_id;
        t.gating = true;
        t.job_id = job_id.into();
        e.disp.register(task_id, t);
        e.jobs
            .note_dispatch(
                task_id,
                job_id,
                4,
                worker,
                ProtoTaskKind::TaskRating,
                "cand",
                "fixed",
                "",
                match_id,
                None,
            )
            .unwrap();
    }

    fn rating_result(worker: &str, task_id: &str, pairs: [i64; 6], games: i64) -> MatchResult {
        MatchResult {
            worker_id: worker.into(),
            task_id: task_id.into(),
            network_sha: "cand".into(),
            opponent_sha: "fixed".into(),
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
    fn rating_accept_never_promotes() {
        let e = env();
        let (job_id, match_id) = seed_rating_job(&e, 0);
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");

        assign_rating_task(&e, "rt0", &job_id, match_id, "w2");
        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [0, 0, 0, 0, 0, 100], 200))
            .unwrap();
        assert!(ack.accepted && ack.match_concluded);

        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
        assert_eq!(
            e.jobs.cancel_task_ids_for_worker("w2"),
            vec!["rt0".to_string()]
        );
        assert!(e.jobs.cancel_task_ids_for_worker("w1").is_empty());

        assign_rating_task(&e, "rt2", &job_id, match_id, "w2");
        let ack2 = e
            .router
            .route(&rating_result("w2", "rt2", [1, 0, 0, 0, 0, 0], 2))
            .unwrap();
        assert!(!ack2.accepted);
        assert!(ack2.message.contains("not_running"), "{}", ack2.message);
    }

    #[test]
    fn rating_reject_observes_only_never_marks() {
        let e = env();
        let (job_id, match_id) = seed_rating_job(&e, 0);
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");

        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [100, 0, 0, 0, 0, 0], 200))
            .unwrap();
        assert!(ack.accepted && ack.match_concluded);
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
    }

    #[test]
    fn rating_total_games_cap_stops_without_verdict() {
        let e = env();
        let (job_id, match_id) = seed_rating_job(&e, 20);
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");

        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [0, 0, 8, 0, 0, 0], 16))
            .unwrap();
        assert!(ack.accepted && !ack.match_concluded);
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "running");

        assign_rating_task(&e, "rt2", &job_id, match_id, "w1");
        let ack = e
            .router
            .route(&rating_result("w1", "rt2", [0, 0, 2, 0, 0, 0], 4))
            .unwrap();
        assert!(ack.accepted && ack.match_concluded);
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
        assert_eq!(
            e.f.matches.get_match(match_id).unwrap().unwrap().num_games,
            20
        );
    }

    #[test]
    fn rating_job_cancel_rejects_late_reports() {
        let e = env();
        let (job_id, match_id) = seed_rating_job(&e, 0);
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");

        assert!(e.jobs.control(&job_id, "cancel").unwrap().0);
        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [0, 0, 2, 0, 0, 0], 4))
            .unwrap();
        assert!(!ack.accepted);
        assert!(ack.message.contains("not_running"), "{}", ack.message);
    }

    #[test]
    fn rating_disable_sprt_runs_to_cap() {
        let e = env();
        let (job_id, match_id) = seed_rating_job_opts(&e, 20, true);
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");

        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [0, 0, 0, 0, 0, 8], 16))
            .unwrap();
        assert!(ack.accepted && !ack.match_concluded);
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "running");

        assign_rating_task(&e, "rt2", &job_id, match_id, "w1");
        let ack = e
            .router
            .route(&rating_result("w1", "rt2", [0, 0, 0, 0, 0, 2], 4))
            .unwrap();
        assert!(ack.accepted && ack.match_concluded);
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
        let m = e.f.matches.get_match(match_id).unwrap().unwrap();
        assert_eq!(m.num_games, 20);
        let conclusion = m.conclusion.expect("cap conclusion");
        assert_eq!(conclusion.verdict, "cap");
    }

    #[test]
    fn rating_disable_sprt_requires_total_games() {
        let e = env();
        e.f.networks.seed_network("cand", None, "onnx");
        e.f.networks.seed_network("fixed", None, "onnx");
        let (ok, msg, _job_id) = e
            .jobs
            .submit(&SubmitJobRequest {
                requester: "test".into(),
                kind: JobKind::JobRating as i32,
                priority: 0,
                rating: Some(RatingJobSpec {
                    candidate_sha: "cand".into(),
                    opponent_sha: "fixed".into(),
                    total_games: 0,
                    disable_sprt: true,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap();
        assert!(!ok, "禁用 SPRT 且 total_games=0 应拒绝提交");
        assert!(msg.contains("total_games"), "{}", msg);
    }

    #[test]
    fn rating_sprt_params_from_spec() {
        let e = env();
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
                    sprt_elo0: 0.0,
                    sprt_elo1: 100.0,
                    sprt_alpha: 0.05,
                    sprt_beta: 0.05,
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

        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [0, 0, 0, 0, 0, 6], 12))
            .unwrap();
        assert!(
            ack.accepted && ack.match_concluded,
            "spec SPRT 覆盖应提前判停"
        );
    }

    #[test]
    fn non_gating_stats_ack_and_completed_shortcircuit() {
        let e = env();
        let mut t0 = RunningTask::new(ProtoTaskKind::TaskCollect);
        t0.worker_id = "w0".into();
        t0.games = 4;
        e.disp.register("plain", t0);
        let ack = e
            .router
            .route(&rating_result("w0", "plain", [0, 0, 2, 0, 0, 0], 4))
            .unwrap();
        assert!(ack.accepted, "非 gating 统计按登记表收讫，不拒绝");
        assert_eq!(ack.message, "non_gating_stats_ack");
        assert_eq!(
            e.disp.lookup("plain").unwrap().state,
            TaskState::Completed,
            "收讫后任务落终态，重复上报走 already_reported 短路"
        );

        let ack2 = e
            .router
            .route(&rating_result("w0", "plain", [0, 0, 2, 0, 0, 0], 4))
            .unwrap();
        assert!(ack2.accepted);
        assert!(
            ack2.message.contains("already_reported"),
            "{}",
            ack2.message
        );
    }
}
