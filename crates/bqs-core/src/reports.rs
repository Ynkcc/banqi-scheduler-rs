use std::sync::Arc;

use bqs_config::Config;
use bqs_judgment::{judge, new_bounds, Hexanomial, Verdict};
use bqs_proto::{MatchResult, MatchResultAck, TaskKind};

use crate::{
    dispatch::{RunningTask, TaskDispatcher, TaskState},
    gatekeeper::Gatekeeper,
    jobs::{JobManager, RatingSpecJson},
    ports::{MatchStore, NetworkStore},
    CoreError, CoreResult,
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
    pub gate: Arc<Gatekeeper>,
    pub disp: Arc<TaskDispatcher>,

    pub jobs: Arc<JobManager>,

    pub matches: Arc<dyn MatchStore>,
    pub nets: Arc<dyn NetworkStore>,
    pub cfg: Arc<Config>,
}

impl ReportRouter {
    pub fn new(
        gate: Arc<Gatekeeper>,
        disp: Arc<TaskDispatcher>,
        jobs: Arc<JobManager>,
        matches: Arc<dyn MatchStore>,
        nets: Arc<dyn NetworkStore>,
        cfg: Arc<Config>,
    ) -> Self {
        Self {
            gate,
            disp,
            jobs,
            matches,
            nets,
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

    fn gate_sink(
        &self,
        task: &RunningTask,
        req: &MatchResult,
    ) -> CoreResult<Option<MatchResultAck>> {
        let is_gating = task.gating;
        if !is_gating {
            return Ok(None);
        }

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
        if task.kind == TaskKind::TaskRating {
            if let Some(spec) = self.jobs.rating_spec(&task.job_id)? {
                return match self.rating_report(task, req, delta, &spec) {
                    Ok(ack) => Ok(Some(ack)),
                    Err(e) => {
                        self.disp.rollback_report(&req.task_id);
                        Err(e)
                    }
                };
            }
        }
        match self
            .gate
            .accept_report(task.match_id, delta, req.games as i64)
        {
            Ok(d) => Ok(Some(MatchResultAck {
                accepted: d.accepted,
                message: d.message,
                match_concluded: d.concluded,
                promoted: d.promoted,
                best_sha: d.best_sha,
            })),
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

        let sp = &self.cfg.sprt;
        let elo0 = if spec.sprt_elo0 != 0.0 {
            spec.sprt_elo0
        } else {
            sp.elo0
        };
        let elo1 = if spec.sprt_elo1 != 0.0 {
            spec.sprt_elo1
        } else {
            sp.elo1
        };
        let alpha = if spec.sprt_alpha != 0.0 {
            spec.sprt_alpha
        } else {
            sp.alpha
        };
        let beta = if spec.sprt_beta != 0.0 {
            spec.sprt_beta
        } else {
            sp.beta
        };
        let (verdict, llr) = judge(agg.pairs, elo0, elo1, new_bounds(alpha, beta))
            .map_err(|e| CoreError::Internal(format!("rating sprt judge match={} : {e}", m.id)))?;
        let cap_reached = spec.total_games > 0 && agg.num_games >= spec.total_games as i64;
        let mut concluded = false;

        let promoted = false;
        let best_sha = self.best_sha()?;
        if verdict != Verdict::Continue || cap_reached {
            concluded = self.matches.conclude_match(m.id)?;
        }
        if concluded {
            self.jobs.complete_rating_job(&task.job_id)?;
        }
        tracing::info!(
            "[rating] match={} job={} verdict={} llr={:.3} cap={} concluded={concluded} promoted={promoted} pairs={}",
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
            promoted,
            best_sha,
        })
    }

    fn best_sha(&self) -> CoreResult<String> {
        Ok(self.nets.get_best()?.map(|b| b.sha).unwrap_or_default())
    }

    pub fn route(&self, req: &MatchResult) -> CoreResult<MatchResultAck> {
        let task = match lookup_report_task(&self.disp, &req.task_id, &req.worker_id) {
            Ok(t) => t,
            Err(rejected) => return Ok(rejected),
        };

        if task.state == TaskState::Completed {
            return Ok(Self::already_reported(&req.task_id));
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
        let gate = Arc::new(Gatekeeper::new(&ports, cfg.clone()));
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
            gate,
            disp.clone(),
            jobs.clone(),
            ports.matches.clone(),
            ports.networks.clone(),
            cfg,
        );
        Env {
            f,
            disp,
            router,
            jobs,
        }
    }

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

    fn assign_rating_task(e: &Env, task_id: &str, job_id: &str, match_id: i64, worker: &str) {
        let mut t = RunningTask::new(ProtoTaskKind::TaskRating);
        t.worker_id = worker.into();
        t.network_sha = "cand".into();
        t.opponent_sha = "best".into();
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
                "best",
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

        assign_rating_task(&e, "rt0", &job_id, match_id, "w2");
        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [0, 0, 0, 0, 0, 100], 200))
            .unwrap();
        assert!(ack.accepted && ack.match_concluded);
        assert!(!ack.promoted, "rating job 永不晋级");
        assert_eq!(ack.best_sha, "best", "best 不变");
        assert_eq!(e.f.networks.best_sha().as_deref(), Some("best"));
        assert_eq!(
            e.f.networks.status_of("cand").as_deref(),
            Some("candidate"),
            "候选不被标记"
        );

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
        let (job_id, match_id) = seed_rating_job(&e, 0, "");
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");

        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [100, 0, 0, 0, 0, 0], 200))
            .unwrap();
        assert!(ack.accepted && ack.match_concluded && !ack.promoted);
        assert_eq!(e.f.networks.best_sha().as_deref(), Some("best"));
        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("candidate"));
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
    }

    #[test]
    fn rating_fixed_opponent_observes_only() {
        let e = env();

        let (job_id, match_id) = seed_rating_job(&e, 0, "fixed");
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");
        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [0, 0, 0, 0, 0, 100], 200))
            .unwrap();
        assert!(ack.accepted && ack.match_concluded && !ack.promoted);
        assert_eq!(e.f.networks.best_sha().as_deref(), Some("best"));
        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("candidate"));
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
    }

    #[test]
    fn rating_total_games_cap_stops_without_verdict() {
        let e = env();

        let (job_id, match_id) = seed_rating_job(&e, 20, "");
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
        assert!(ack.accepted && ack.match_concluded && !ack.promoted);
        assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
        assert_eq!(
            e.f.matches.get_match(match_id).unwrap().unwrap().num_games,
            20
        );
        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("candidate"));
    }

    #[test]
    fn rating_job_cancel_rejects_late_reports() {
        let e = env();
        let (job_id, match_id) = seed_rating_job(&e, 0, "");
        assign_rating_task(&e, "rt1", &job_id, match_id, "w1");

        assert!(e.jobs.control(&job_id, "cancel").unwrap().0);
        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [0, 0, 2, 0, 0, 0], 4))
            .unwrap();
        assert!(!ack.accepted);
        assert!(ack.message.contains("not_running"), "{}", ack.message);

        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("candidate"));
    }

    #[test]
    fn rating_sprt_params_from_spec_override_config() {
        let e = env();

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

        let ack = e
            .router
            .route(&rating_result("w1", "rt1", [0, 0, 0, 0, 0, 6], 12))
            .unwrap();
        assert!(
            ack.accepted && ack.match_concluded,
            "spec SPRT 覆盖应提前判停"
        );

        e.f.networks.seed_network("cand2", None, "onnx");
        let match_id2 = e.f.matches.seed_match("cand2", "fixed", 10);
        let mut t2 = RunningTask::new(ProtoTaskKind::TaskRating);
        t2.worker_id = "w3".into();
        t2.match_id = match_id2;
        t2.gating = true;
        t2.games = 12;
        e.disp.register("rt2", t2);
        let ack2 = e
            .router
            .route(&rating_result("w3", "rt2", [0, 0, 0, 0, 0, 6], 12))
            .unwrap();
        assert!(ack2.accepted && !ack2.match_concluded);
    }

    #[test]
    fn gating_is_predetermined_by_registry_not_request() {
        let e = env();
        e.f.networks.seed_network("best", None, "onnx");
        e.f.networks.promote_best("best").unwrap();
        e.f.networks.seed_network("cand", None, "onnx");
        let match_id = e.f.matches.seed_match("cand", "best", 10);

        let mut t0 = RunningTask::new(ProtoTaskKind::TaskSelfplay);
        t0.worker_id = "w0".into();
        t0.games = 4;
        e.disp.register("plain", t0);
        let ack = e
            .router
            .route(&rating_result("w0", "plain", [0, 0, 2, 0, 0, 0], 4))
            .unwrap();
        assert!(!ack.accepted);
        assert_eq!(ack.message, "task_is_not_gating");

        let mut t1 = RunningTask::new(ProtoTaskKind::TaskSelfplay);
        t1.worker_id = "w1".into();
        t1.games = 4;
        t1.match_id = match_id;
        t1.gating = true;
        e.disp.register("mix1", t1);
        let ack = e
            .router
            .route(&rating_result("w1", "mix1", [0, 0, 2, 0, 0, 0], 4))
            .unwrap();
        assert!(ack.accepted, "{}", ack.message);
        assert_eq!(
            e.f.matches.get_match(match_id).unwrap().unwrap().num_games,
            4
        );
    }

    #[test]
    fn legacy_rating_task_without_job_mapping_uses_gatekeeper_chain() {
        let e = env();

        e.f.networks.seed_network("best", None, "onnx");
        e.f.networks.promote_best("best").unwrap();
        e.f.networks.seed_network("cand", None, "onnx");
        let match_id = e.f.matches.seed_match("cand", "best", 10);
        let mut t = RunningTask::new(ProtoTaskKind::TaskRating);
        t.worker_id = "w1".into();
        t.match_id = match_id;
        t.gating = true;
        t.games = 20;
        e.disp.register("lt1", t);

        let ack = e
            .router
            .route(&rating_result("w1", "lt1", [0, 0, 10, 0, 0, 0], 20))
            .unwrap();
        assert!(ack.accepted && ack.match_concluded && !ack.promoted);
        assert_eq!(e.f.networks.status_of("cand").as_deref(), Some("rejected"));
    }
}
