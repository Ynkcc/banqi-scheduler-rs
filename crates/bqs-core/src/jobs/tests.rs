use bq_record::DataKind;
use bqs_proto::{
    CollectPlayerSpec, CollectionJobSpec, JobKind, RatingJobSpec, SeedStrategy, SeedStrategyKind,
    SubmitJobRequest, TaskKind,
};

use super::*;
use crate::consts;
use crate::ports::JobRow;
use crate::test_support::Fakes;
use std::sync::Arc;

struct Env {
    f: Fakes,
    disp: Arc<TaskDispatcher>,
    jobs: JobManager,
}

fn env(config_extra: &str) -> Env {
    let f = Fakes::new();
    let cfg = Arc::new(Config::parse(config_extra, "test").unwrap());
    let ports = f.ports();
    let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), f.clock.clone()));
    let jobs = JobManager::new(
        ports.jobs.clone(),
        disp.clone(),
        ports.networks.clone(),
        ports.matches.clone(),
        cfg,
        f.clock.clone(),
    );
    Env { f, disp, jobs }
}

fn collect_req(
    variant: &str,
    total: u64,
    sha_a: &str,
    kind_a: &str,
    kind_b: &str,
) -> SubmitJobRequest {
    SubmitJobRequest {
        requester: "test".into(),
        kind: JobKind::JobCollect as i32,
        priority: 0,
        collect: Some(CollectionJobSpec {
            variant: variant.into(),
            scenario_json: String::new(),
            seed_strategy: Some(SeedStrategy {
                kind: SeedStrategyKind::SeedIncremental as i32,
                base_seed: 1000,
                ..Default::default()
            }),
            player_a: Some(CollectPlayerSpec {
                kind: kind_a.into(),
                network_sha: sha_a.into(),
                ..Default::default()
            }),
            player_b: Some(CollectPlayerSpec {
                kind: kind_b.into(),
                ..Default::default()
            }),
            data_kind: DataKind::DataResnet as i32,
            total_games: total,
            pairs_parallel: None,
            pair_seed_refresh: None,
            start_fens: Vec::new(),
        }),
        ..Default::default()
    }
}

fn seed_collect_job(env: &Env, priority: i64, total: i64, state: &str) -> String {
    let job_id = format!("j{}", env.f.jobs.next_id());
    env.f.jobs.seed_job(JobRow {
        job_id: job_id.clone(),
        kind: JobKind::JobCollect as i64,
        priority,
        state: state.into(),
        total_games: total,
        completed_games: 0,
        dispatched_games: 0,
        spec_json: JobSpecJson::Collect(CollectSpecJson::default()).to_json(),
        created_at: 1,
        updated_at: 1,
        next_seq: 0,
    });
    job_id
}

fn register_in_flight(env: &Env, task_id: &str, job_id: &str, worker: &str, games: i64) {
    let mut t = crate::dispatch::RunningTask::new(TaskKind::TaskCollect);
    t.worker_id = worker.into();
    t.games = games;
    t.job_id = job_id.into();
    env.disp.register(task_id, t);

    env.f.jobs.insert_job_task(task_id, job_id, games).unwrap();
}

#[test]
fn submit_rejects_invalid_variant_and_totals() {
    let e = env("");
    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req("9x9", 16, "", "random", "rule:capture_first"))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("invalid_variant"), "{msg}");

    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req("4x8", 3, "", "random", "rule:capture_first"))
        .unwrap();
    assert!(!ok, "奇数局数应拒绝");
    assert!(msg.contains("偶数"), "{msg}");

    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req("4x8", 1, "", "random", "rule:capture_first"))
        .unwrap();
    assert!(!ok, "total<2 应拒绝");
    assert!(msg.contains("偶数"), "{msg}");

    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req("4x8", 16, "", "mcts:quantum", "random"))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("invalid_player_kind"), "{msg}");
}

#[test]
fn submit_resolves_short_sha_prefix() {
    let e = env("");
    let full_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let full_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let full_c = format!("ab{}", "c".repeat(62));
    e.f.networks.seed_network(full_a, None, "onnx");
    e.f.networks.seed_network(full_b, None, "onnx");
    e.f.networks.seed_network(&full_c, None, "onnx");

    let (ok, msg, id) = e
        .jobs
        .submit(&collect_req("4x8", 16, "aaaa", "mcts_resnet", "random"))
        .unwrap();
    assert!(ok, "{msg}");
    let row = e.jobs.get(&id).unwrap().unwrap();
    let JobSpecJson::Collect(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("collect spec json");
    };
    assert_eq!(spec.player_a.network_sha, full_a);

    let (ok, msg, id) = e.jobs.submit(&rating_req(20, "bbbb", full_a)).unwrap();
    assert!(ok, "{msg}");
    let row = e.jobs.get(&id).unwrap().unwrap();
    let JobSpecJson::Rating(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("rating spec json");
    };
    assert_eq!(spec.candidate_sha, full_b);
    assert_eq!(spec.side_a.network_sha, full_b);

    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req("4x8", 16, "a", "mcts_resnet", "random"))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("歧义"), "{msg}");

    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req("4x8", 16, "ffff", "mcts_resnet", "random"))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("network_not_registered"), "{msg}");
}

#[test]
fn submit_rejects_unregistered_model_network_and_requires_sha() {
    let e = env("");

    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req("4x8", 16, "ghost", "mcts_resnet", "random"))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("network_not_registered"), "{msg}");

    e.f.networks.seed_network("n1", None, "onnx");
    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req("4x8", 16, "n1", "mcts_resnet", "random"))
        .unwrap();
    assert!(ok, "{msg}");

    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req("4x8", 16, "", "random", "rule:capture_first"))
        .unwrap();
    assert!(ok, "{msg}");
    let mut r = collect_req("4x8", 16, "", "random", "rule:capture_first");
    r.collect
        .as_mut()
        .unwrap()
        .player_b
        .as_mut()
        .unwrap()
        .network_sha = "n1".into();
    let (ok, msg, _) = e.jobs.submit(&r).unwrap();
    assert!(!ok, "非模型类棋手不得携带 network_sha");
    assert!(msg.contains("必须为空"), "{msg}");
}

#[test]
fn submit_priority_zero_uses_kind_default() {
    let e = env("[jobs]\npriority_collect = 77\n");
    e.f.networks.seed_network("n1", None, "onnx");
    let (_, _, id) = e
        .jobs
        .submit(&collect_req("4x8", 16, "n1", "mcts_resnet", "random"))
        .unwrap();
    let row = e.jobs.get(&id).unwrap().unwrap();
    assert_eq!(row.priority, 77);
}

#[test]
fn seed_resolution_is_deterministic_per_seq() {
    let fixed = SeedStrategyJson {
        kind: SeedStrategyKind::SeedFixed as i32,
        fixed_seed: 42,
        ..Default::default()
    };
    assert_eq!(resolve_seed(&fixed, 0), 42);
    assert_eq!(resolve_seed(&fixed, 999), 42);

    let inc = SeedStrategyJson {
        kind: SeedStrategyKind::SeedIncremental as i32,
        base_seed: 1000,
        ..Default::default()
    };
    assert_eq!(resolve_seed(&inc, 0), 1000);
    assert_eq!(resolve_seed(&inc, 7), 1007);

    let rnd = SeedStrategyJson {
        kind: SeedStrategyKind::SeedRandom as i32,
        base_seed: 7,
        ..Default::default()
    };
    assert_eq!(
        resolve_seed(&rnd, 3),
        resolve_seed(&rnd, 3),
        "同 seq 恒同种子"
    );
    assert_ne!(
        resolve_seed(&rnd, 3),
        resolve_seed(&rnd, 4),
        "不同 seq 应派生不同种子"
    );

    let range = SeedStrategyJson {
        kind: SeedStrategyKind::SeedRange as i32,
        base_seed: 5,
        min_seed: 10,
        max_seed: 20,
        ..Default::default()
    };
    for seq in 0..50 {
        let s = resolve_seed(&range, seq);
        assert!((10..=20).contains(&s), "RANGE 种子应在区间内: {s}");
    }

    let point = SeedStrategyJson {
        kind: SeedStrategyKind::SeedRange as i32,
        min_seed: 11,
        max_seed: 11,
        ..Default::default()
    };
    assert_eq!(resolve_seed(&point, 5), 11);
}

#[test]
fn record_report_aggregates_and_concludes_with_cancellation() {
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 100, "running");

    register_in_flight(&e, "t1", &job_id, "w1", 32);
    register_in_flight(&e, "t2", &job_id, "w2", 32);
    e.jobs
        .note_dispatch(
            "t3",
            &job_id,
            32,
            "w3",
            TaskKind::TaskCollect,
            "",
            "",
            "",
            0,
            None,
        )
        .unwrap();

    e.jobs.record_report("unknown_task", 16).unwrap();

    e.jobs.record_report("t1", 32).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 32);
    assert_eq!(row.state, "running");

    e.jobs.record_report("t2", 32).unwrap();

    e.jobs.record_report("t3", 32).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 96);
    assert_eq!(row.state, "running");
    assert!(
        e.jobs.cancel_task_ids_for_worker("w1").is_empty(),
        "未收敛不取消"
    );

    e.jobs
        .note_dispatch(
            "t4",
            &job_id,
            16,
            "w1",
            TaskKind::TaskCollect,
            "",
            "",
            "",
            0,
            None,
        )
        .unwrap();
    e.jobs.record_report("t4", 16).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.state, "completed");

    let mut got = e.jobs.cancel_task_ids_for_worker("w1");
    got.sort();
    assert_eq!(
        got,
        vec!["t1".to_string(), "t4".to_string()],
        "w1 名下在飞应被取消"
    );
    assert_eq!(
        e.jobs.cancel_task_ids_for_worker("w2"),
        vec!["t2".to_string()]
    );

    assert!(e.jobs.cancel_task_ids_for_worker("w1").is_empty());
    assert!(e.jobs.cancel_task_ids_for_worker("w2").is_empty());
}

#[test]
fn record_report_accepts_late_report_after_restart_gap() {
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 30, "running");
    e.jobs
        .note_dispatch(
            "t1",
            &job_id,
            32,
            "w1",
            TaskKind::TaskCollect,
            "",
            "",
            "",
            0,
            None,
        )
        .unwrap();

    e.disp.delete("t1");
    e.jobs.record_report("t1", 32).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 32);
    assert_eq!(row.state, "completed", "32 ≥ total 30 → 收敛");

    e.jobs.record_report("t1", 32).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 32);
    assert_eq!(row.state, "completed");

    assert!(e.jobs.cancel_task_ids_for_worker("w1").is_empty());
}

#[test]
fn zero_games_report_ignored() {
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 50, "running");
    e.jobs
        .note_dispatch(
            "t1",
            &job_id,
            16,
            "w1",
            TaskKind::TaskCollect,
            "",
            "",
            "",
            0,
            None,
        )
        .unwrap();
    e.jobs.record_report("t1", 0).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 0, "0 局上报不计聚合");
    assert_eq!(row.state, "running");
}

#[test]
fn preemption_cancels_lower_priority_in_flight_on_submit() {
    let e = env("");
    let low = seed_collect_job(&e, 10, 1000, "running");
    register_in_flight(&e, "low-t1", &low, "w1", 32);
    register_in_flight(&e, "low-t2", &low, "w2", 32);

    e.f.networks.seed_network("n1", None, "onnx");
    let (_, _, high) = e
        .jobs
        .submit(&collect_req("4x8", 16, "n1", "mcts_resnet", "random"))
        .unwrap();
    assert_eq!(
        e.jobs.cancel_task_ids_for_worker("w1"),
        vec!["low-t1".to_string()]
    );
    assert_eq!(
        e.jobs.cancel_task_ids_for_worker("w2"),
        vec!["low-t2".to_string()]
    );

    assert_eq!(e.jobs.get(&low).unwrap().unwrap().state, "running");
    assert_eq!(e.jobs.get(&high).unwrap().unwrap().state, "running");

    register_in_flight(&e, "high-t1", &high, "w3", 8);
    e.jobs.preemption_sweep(true);
    assert!(
        e.jobs.cancel_task_ids_for_worker("w3").is_empty(),
        "最高优先级的在飞不取消"
    );
}

#[test]
fn preemption_skips_paused_high_priority() {
    let e = env("");
    let low = seed_collect_job(&e, 10, 1000, "running");
    register_in_flight(&e, "p-t1", &low, "w1", 32);

    seed_collect_job(&e, 100, 100, "paused");
    e.jobs.preemption_sweep(true);
    assert!(
        e.jobs.cancel_task_ids_for_worker("w1").is_empty(),
        "无可派发高优先级 job 不抢占"
    );
}

#[test]
fn preemption_resume_cancels_lower_priority_in_flight() {
    let e = env("");
    let low = seed_collect_job(&e, 10, 1000, "running");
    register_in_flight(&e, "low-t1", &low, "w1", 32);
    let high = seed_collect_job(&e, 100, 100, "paused");

    let (ok, msg, _) = e.jobs.control(&high, "resume").unwrap();
    assert!(ok, "{msg}");
    assert_eq!(
        e.jobs.cancel_task_ids_for_worker("w1"),
        vec!["low-t1".to_string()]
    );
}

#[test]
fn completion_only_concludes_when_total_reached_not_persistent() {
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 0, "running");
    e.jobs
        .note_dispatch(
            "t1",
            &job_id,
            16,
            "w1",
            TaskKind::TaskCollect,
            "",
            "",
            "",
            0,
            None,
        )
        .unwrap();
    e.jobs.record_report("t1", 16).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 16);
    assert_eq!(row.state, "running", "持续型 job 不收敛");
}

#[test]
fn pause_resume_cancel_state_machine() {
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 100, "running");
    register_in_flight(&e, "t1", &job_id, "w1", 32);

    let (ok, msg, st) = e.jobs.control(&job_id, "pause").unwrap();
    assert!(ok, "{msg}");
    assert_eq!(st, "paused");

    let (ok, msg, _) = e.jobs.control(&job_id, "pause").unwrap();
    assert!(!ok);
    assert!(msg.contains("pause_requires_running"), "{msg}");

    let (ok, _, st) = e.jobs.control(&job_id, "resume").unwrap();
    assert!(ok);
    assert_eq!(st, "running");

    let (ok, msg, _) = e.jobs.control(&job_id, "resume").unwrap();
    assert!(!ok);
    assert!(msg.contains("resume_requires_paused"), "{msg}");

    let (ok, _, st) = e.jobs.control(&job_id, "cancel").unwrap();
    assert!(ok);
    assert_eq!(st, "cancelled");
    assert_eq!(
        e.jobs.cancel_task_ids_for_worker("w1"),
        vec!["t1".to_string()]
    );

    for action in ["pause", "resume", "cancel"] {
        let (ok, msg, _) = e.jobs.control(&job_id, action).unwrap();
        assert!(!ok, "{action} 于终态应拒绝");
        assert!(
            msg.contains("terminal") || msg.contains("requires"),
            "{msg}"
        );
    }

    let (ok, msg, _) = e.jobs.control("nope", "pause").unwrap();
    assert!(!ok && msg.contains("not_found"), "{msg}");
    let (ok, msg, _) = e.jobs.control(&job_id, "explode").unwrap();
    assert!(!ok && msg.contains("invalid_control"), "{msg}");
}

#[test]
fn cancelled_late_report_still_counts() {
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 100, "running");
    e.jobs
        .note_dispatch(
            "t1",
            &job_id,
            32,
            "w1",
            TaskKind::TaskCollect,
            "",
            "",
            "",
            0,
            None,
        )
        .unwrap();
    e.jobs.control(&job_id, "cancel").unwrap();
    e.jobs.record_report("t1", 32).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 32, "取消后迟到上报仍聚合");
    assert_eq!(row.state, "cancelled", "终态不被聚合改写");
}

#[test]
fn dispatchable_filters_state_and_completion() {
    let e = env("");
    let running = seed_collect_job(&e, 50, 100, "running");
    seed_collect_job(&e, 50, 100, "paused");

    assert_eq!(e.jobs.dispatchable(JobKind::JobCollect).unwrap().len(), 1);

    e.jobs
        .note_dispatch(
            "t1",
            &running,
            100,
            "w1",
            TaskKind::TaskCollect,
            "",
            "",
            "",
            0,
            None,
        )
        .unwrap();
    e.jobs.record_report("t1", 100).unwrap();
    assert!(e.jobs.dispatchable(JobKind::JobCollect).unwrap().is_empty());

    e.f.jobs.seed_job(JobRow {
        job_id: "persistent".into(),
        kind: JobKind::JobCollect as i64,
        priority: 50,
        state: "running".into(),
        total_games: 0,
        completed_games: 9999,
        dispatched_games: 9999,
        spec_json: JobSpecJson::Collect(CollectSpecJson::default()).to_json(),
        created_at: 1,
        updated_at: 1,
        next_seq: 0,
    });
    assert_eq!(e.jobs.dispatchable(JobKind::JobCollect).unwrap().len(), 1);
}

#[test]
fn fake_clock_preemption_throttled() {
    let e = env("");
    let low = seed_collect_job(&e, 10, 1000, "running");
    register_in_flight(&e, "low-t1", &low, "w1", 32);
    let _high = seed_collect_job(&e, 100, 100, "running");

    e.jobs.preemption_sweep(false);
    assert_eq!(
        e.jobs.cancel_task_ids_for_worker("w1"),
        vec!["low-t1".to_string()]
    );
    register_in_flight(&e, "low-t2", &low, "w1", 32);
    e.jobs.preemption_sweep(false);
    assert!(
        e.jobs.cancel_task_ids_for_worker("w1").is_empty(),
        "节流窗口内不重复取消"
    );
    e.f.clock
        .advance(consts::JOB_PREEMPT_INTERVAL.as_secs() as i64);
    e.jobs.preemption_sweep(false);

    let mut got = e.jobs.cancel_task_ids_for_worker("w1");
    got.sort();
    assert_eq!(got, vec!["low-t1".to_string(), "low-t2".to_string()]);
}

#[test]
fn submit_timestamps_from_injected_clock() {
    let e = env("");
    e.f.networks.seed_network("n1", None, "onnx");
    let (_, _, id) = e
        .jobs
        .submit(&collect_req("4x8", 16, "n1", "mcts_resnet", "random"))
        .unwrap();
    let row = e.jobs.get(&id).unwrap().unwrap();
    assert_eq!(row.created_at, 1000);
    assert_eq!(row.updated_at, row.created_at);
}

fn rating_req(total: u64, candidate: &str, opponent: &str) -> SubmitJobRequest {
    SubmitJobRequest {
        requester: "test".into(),
        kind: JobKind::JobRating as i32,
        priority: 0,
        rating: Some(RatingJobSpec {
            variant: String::new(),
            candidate_sha: candidate.into(),
            opponent_sha: opponent.into(),
            total_games: total,
            sprt_elo0: 0.0,
            sprt_elo1: 15.0,
            sprt_alpha: 0.05,
            sprt_beta: 0.05,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn rating_sprt_params_unconditionally_validated() {
    let e = env("");
    e.f.networks.seed_network("cand", None, "onnx");
    e.f.networks.seed_network("fixed", None, "onnx");

    let mut req = rating_req(0, "cand", "fixed");
    req.rating.as_mut().unwrap().sprt_elo1 = 0.0;
    let (ok, msg, _) = e.jobs.submit(&req).unwrap();
    assert!(!ok, "{msg}");
    assert!(msg.contains("elo1"), "{msg}");

    let mut req = rating_req(0, "cand", "fixed");
    req.rating.as_mut().unwrap().sprt_alpha = 1.5;
    let (ok, msg, _) = e.jobs.submit(&req).unwrap();
    assert!(!ok, "{msg}");
    assert!(msg.contains("alpha"), "{msg}");
}

fn seed_rating_job(e: &Env, match_id: i64, total: u64, state: &str) -> String {
    let job_id = format!("j{}", e.f.jobs.next_id());
    e.f.jobs.seed_job(JobRow {
        job_id: job_id.clone(),
        kind: JobKind::JobRating as i64,
        priority: 80,
        state: state.into(),
        total_games: total as i64,
        completed_games: 0,
        dispatched_games: 0,
        spec_json: JobSpecJson::Rating(RatingSpecJson {
            variant: "4x8".into(),
            candidate_sha: "cand".into(),
            opponent_sha: "best".into(),
            total_games: total,
            match_id,
            ..Default::default()
        })
        .to_json(),
        created_at: 1,
        updated_at: 1,
        next_seq: 0,
    });
    job_id
}

#[test]
fn rating_submit_validates_and_creates_match() {
    let e = env("");

    let (ok, msg, _) = e.jobs.submit(&rating_req(20, "ghost", "best")).unwrap();
    assert!(!ok);
    assert!(msg.contains("network_not_registered"), "{msg}");

    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.seed_network("cand", None, "onnx");

    let (ok, msg, _) = e.jobs.submit(&rating_req(20, "cand", "")).unwrap();
    assert!(!ok, "best 语义已移除，对手必须显式指定");
    assert!(msg.contains("opponent_sha"), "{msg}");

    for total in [1u64, 3] {
        let (ok, msg, _) = e.jobs.submit(&rating_req(total, "cand", "best")).unwrap();
        assert!(!ok, "total={total} 应拒绝");
        assert!(msg.contains("total_games"), "{msg}");
    }

    let (ok, msg, _) = e.jobs.submit(&rating_req(20, "cand", "ghost")).unwrap();
    assert!(!ok);
    assert!(msg.contains("network_not_registered"), "{msg}");

    let (ok, msg, id) = e.jobs.submit(&rating_req(20, "cand", "best")).unwrap();
    assert!(ok, "{msg}");
    let row = e.jobs.get(&id).unwrap().unwrap();
    assert_eq!(row.priority, 80, "rating 默认优先级");
    assert_eq!(row.total_games, 20);
    let JobSpecJson::Rating(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("rating spec json");
    };
    assert!(spec.match_id != 0, "提交即建对发行");
    let m = e.f.matches.get_match(spec.match_id).unwrap().unwrap();
    assert_eq!(m.candidate, "cand");
    assert_eq!(m.opponent, "best");
    assert_eq!(m.target_games, 10, "对打目标（对数）= 局数上限一半");

    let mut bad = rating_req(20, "cand", "best");
    bad.rating.as_mut().unwrap().variant = "9x9".into();
    let (ok, msg, _) = e.jobs.submit(&bad).unwrap();
    assert!(!ok);
    assert!(msg.contains("invalid_variant"), "{msg}");

    let mut bad = rating_req(20, "cand", "best");
    bad.rating.as_mut().unwrap().sprt_alpha = 1.5;
    let (ok, msg, _) = e.jobs.submit(&bad).unwrap();
    assert!(!ok);
    assert!(msg.contains("sprt_alpha"), "{msg}");
}

#[test]
fn rating_cancel_stops_match_and_in_flight() {
    let e = env("");
    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.seed_network("cand", None, "onnx");
    let (_, _, id) = e.jobs.submit(&rating_req(20, "cand", "best")).unwrap();
    let row = e.jobs.get(&id).unwrap().unwrap();
    let JobSpecJson::Rating(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("rating spec json");
    };
    let match_id = spec.match_id;

    e.jobs
        .note_dispatch(
            "rt1",
            &id,
            4,
            "w1",
            TaskKind::TaskRating,
            "cand",
            "best",
            "",
            match_id,
            None,
        )
        .unwrap();

    let (ok, msg, st) = e.jobs.control(&id, "cancel").unwrap();
    assert!(ok, "{msg}");
    assert_eq!(st, "cancelled");
    let m = e.f.matches.get_match(match_id).unwrap().unwrap();
    assert_eq!(
        m.status,
        crate::types::match_status::CANCELLED,
        "对打应置 cancelled"
    );
    assert_eq!(
        e.jobs.cancel_task_ids_for_worker("w1"),
        vec!["rt1".to_string()]
    );

    let (_, ok2) =
        e.f.matches
            .add_match_games(match_id, bqs_judgment::Hexanomial([1, 0, 0, 0, 0, 0]), 2)
            .unwrap();
    assert!(!ok2);
}

#[test]
fn rating_complete_job_on_verdict_is_idempotent() {
    let e = env("");
    let match_id = e.f.matches.seed_match("cand", "best", 10);
    let job_id = seed_rating_job(&e, match_id, 0, "running");
    e.jobs
        .note_dispatch(
            "rt1",
            &job_id,
            4,
            "w1",
            TaskKind::TaskRating,
            "cand",
            "best",
            "",
            match_id,
            None,
        )
        .unwrap();

    e.jobs.complete_rating_job(&job_id).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.state, "completed");
    assert_eq!(
        e.jobs.cancel_task_ids_for_worker("w1"),
        vec!["rt1".to_string()]
    );
    e.jobs.complete_rating_job(&job_id).unwrap();
    assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");

    assert!(e.jobs.dispatchable(JobKind::JobRating).unwrap().is_empty());
}

fn sides_rating_req(
    variant_sides: Option<(CollectPlayerSpec, CollectPlayerSpec)>,
    total: u64,
) -> SubmitJobRequest {
    let (side_a, side_b) = match variant_sides {
        Some(pair) => pair,
        None => (nnue_side(""), nnue_side("")),
    };
    SubmitJobRequest {
        requester: "test".into(),
        kind: JobKind::JobRating as i32,
        priority: 0,
        rating: Some(RatingJobSpec {
            side_a: Some(side_a),
            side_b: Some(side_b),
            total_games: total,
            sprt_elo0: 0.0,
            sprt_elo1: 15.0,
            sprt_alpha: 0.05,
            sprt_beta: 0.05,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn nnue_side(sha: &str) -> CollectPlayerSpec {
    CollectPlayerSpec {
        kind: "expectimax_nnue".into(),
        network_sha: sha.into(),
        search: None,
    }
}

#[test]
fn rating_sides_submit_normalizes_and_freezes() {
    let e = env("");
    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.seed_network("cand", None, "onnx");

    let req = sides_rating_req(
        Some((
            CollectPlayerSpec {
                kind: "expectimax_nnue".into(),
                network_sha: "best".into(),
                search: Some(bqs_proto::CollectSearchParams {
                    expectimax_node_budget: 4000,
                    ..Default::default()
                }),
            },
            CollectPlayerSpec {
                kind: "expectimax_rule".into(),
                network_sha: String::new(),
                search: Some(bqs_proto::CollectSearchParams {
                    expectimax_node_budget: 1000,
                    expectimax_chance_outcome_depth: Some(2),
                    ..Default::default()
                }),
            },
        )),
        20,
    );
    let (ok, msg, id) = e.jobs.submit(&req).unwrap();
    assert!(ok, "{msg}");
    let row = e.jobs.get(&id).unwrap().unwrap();
    let JobSpecJson::Rating(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("rating spec json");
    };

    assert_eq!(spec.side_a.kind, "expectimax_nnue");
    assert_eq!(spec.side_a.network_sha, "best", "模型类 sha 必须显式指定");
    assert_eq!(spec.side_b.kind, "expectimax_rule");
    assert_eq!(spec.side_b.network_sha, "", "非网络类 sha 为空");

    assert_eq!(
        spec.side_a.search.as_ref().unwrap().expectimax_node_budget,
        4000
    );
    assert_eq!(
        spec.side_a
            .search
            .as_ref()
            .unwrap()
            .expectimax_chance_outcome_depth,
        None,
        "submit 未指定 k 时 spec 冻结应保持 None（派发时由 assembler 补默认）"
    );
    assert_eq!(
        spec.side_b.search.as_ref().unwrap().expectimax_node_budget,
        1000
    );
    assert_eq!(
        spec.side_b
            .search
            .as_ref()
            .unwrap()
            .expectimax_chance_outcome_depth,
        Some(2),
        "显式指定的 k 应随 spec 冻结透传"
    );

    let m = e.f.matches.get_match(spec.match_id).unwrap().unwrap();
    assert_eq!(m.candidate, "best");
    assert_eq!(m.opponent, "");
}

#[test]
fn rating_sides_submit_freezes_chance_budget_ratio() {
    let e = env("");
    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.seed_network("cand", None, "onnx");

    let req = sides_rating_req(
        Some((
            CollectPlayerSpec {
                kind: "expectimax_nnue".into(),
                network_sha: "best".into(),
                search: Some(bqs_proto::CollectSearchParams {
                    expectimax_node_budget: 4000,
                    expectimax_chance_budget_ratio: Some(0.4),
                    ..Default::default()
                }),
            },
            CollectPlayerSpec {
                kind: "expectimax_nnue".into(),
                network_sha: "cand".into(),
                search: Some(bqs_proto::CollectSearchParams {
                    expectimax_node_budget: 1000,
                    expectimax_chance_budget_ratio: Some(0.0),
                    ..Default::default()
                }),
            },
        )),
        20,
    );
    let (ok, msg, id) = e.jobs.submit(&req).unwrap();
    assert!(ok, "{msg}");
    let row = e.jobs.get(&id).unwrap().unwrap();
    let JobSpecJson::Rating(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("rating spec json");
    };

    let ratio_a = spec
        .side_a
        .search
        .as_ref()
        .unwrap()
        .expectimax_chance_budget_ratio
        .expect("显式 ratio 应随 spec 冻结透传");
    assert!(
        (ratio_a - 0.4).abs() < 1e-6,
        "显式 ratio 应随 spec 冻结透传（f32 往返误差可忽略）"
    );
    let ratio_b = spec
        .side_b
        .search
        .as_ref()
        .unwrap()
        .expectimax_chance_budget_ratio
        .expect("显式 ratio=0.0 应被尊重（0 预算隔离语义）");
    assert_eq!(ratio_b, 0.0, "显式 ratio=0.0 应被尊重（0 预算隔离语义）");

    let req = sides_rating_req(
        Some((
            CollectPlayerSpec {
                kind: "expectimax_nnue".into(),
                network_sha: "best".into(),
                search: Some(bqs_proto::CollectSearchParams {
                    expectimax_node_budget: 4000,
                    ..Default::default()
                }),
            },
            CollectPlayerSpec {
                kind: "expectimax_nnue".into(),
                network_sha: "cand".into(),
                search: Some(bqs_proto::CollectSearchParams {
                    expectimax_node_budget: 1000,
                    ..Default::default()
                }),
            },
        )),
        20,
    );
    let (ok, msg, id) = e.jobs.submit(&req).unwrap();
    assert!(ok, "{msg}");
    let row = e.jobs.get(&id).unwrap().unwrap();
    let JobSpecJson::Rating(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("rating spec json");
    };
    assert_eq!(
        spec.side_a
            .search
            .as_ref()
            .and_then(|s| s.expectimax_chance_budget_ratio),
        None,
        "submit 未指定 ratio 时 spec 冻结应保持 None（派发时由 assembler 补默认）"
    );
}

#[test]
fn rating_sides_validation_rejects() {
    let e = env("");
    e.f.networks.seed_network("cand", None, "onnx");

    let mut req = sides_rating_req(None, 20);
    req.rating.as_mut().unwrap().side_a = Some(nnue_side("cand"));
    req.rating.as_mut().unwrap().side_b = None;
    let (ok, msg, _) = e.jobs.submit(&req).unwrap();
    assert!(!ok);
    assert!(msg.contains("成对填写"), "{msg}");

    let mut bad = nnue_side("cand");
    bad.kind = "chess_engine".into();
    let (ok, msg, _) = e
        .jobs
        .submit(&sides_rating_req(Some((bad, nnue_side(""))), 20))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("invalid_player_kind"), "{msg}");

    let (ok, msg, _) = e
        .jobs
        .submit(&sides_rating_req(
            Some((nnue_side("ghost"), nnue_side(""))),
            20,
        ))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("network_not_registered"), "{msg}");

    let mut rule = nnue_side("");
    rule.kind = "random".into();
    rule.network_sha = "cand".into();
    let (ok, msg, _) = e
        .jobs
        .submit(&sides_rating_req(Some((nnue_side("cand"), rule)), 20))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("network_sha 必须为空"), "{msg}");

    let (ok, msg, _) = e
        .jobs
        .submit(&sides_rating_req(Some((nnue_side(""), nnue_side(""))), 20))
        .unwrap();
    assert!(!ok);
    assert!(
        msg.contains("network_sha 必填"),
        "模型类空 sha 不再回退 best: {msg}"
    );

    let (ok, msg, _) = e
        .jobs
        .submit(&sides_rating_req(
            Some((nnue_side("cand"), nnue_side(""))),
            3,
        ))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("total_games"), "{msg}");
}

#[test]
fn collect_spec_json_roundtrip_pair_overrides() {
    let spec: CollectSpecJson = serde_json::from_str(
        r#"{"variant":"4x8","totalGames":4,"pairsParallel":false,"pairSeedRefresh":false}"#,
    )
    .unwrap();
    assert_eq!(spec.pairs_parallel, Some(false));
    assert_eq!(spec.pair_seed_refresh, Some(false));

    let out = serde_json::to_value(&spec).unwrap();
    assert_eq!(out["pairsParallel"], serde_json::json!(false));
    assert_eq!(out["pairSeedRefresh"], serde_json::json!(false));
    let back = CollectSpecJson::from_proto(spec.to_proto());
    assert_eq!(back.pairs_parallel, Some(false));
    assert_eq!(back.pair_seed_refresh, Some(false));

    let empty: CollectSpecJson = serde_json::from_str(r#"{"variant":"4x8"}"#).unwrap();
    assert_eq!(empty.pairs_parallel, None);
    assert_eq!(empty.pair_seed_refresh, None);
    let back = CollectSpecJson::from_proto(empty.to_proto());
    assert_eq!(back.pairs_parallel, None);
    assert_eq!(back.pair_seed_refresh, None);
}

fn valid_fen(variant: bq_env::Variant) -> String {
    let cfg = variant.config();
    let bag = |p: bq_env::Player| -> String {
        (0..bq_env::NUM_PIECE_TYPES_MAX)
            .filter(|&t| cfg.piece_counts[t] > 0)
            .map(|t| {
                format!(
                    "{}{}",
                    bq_env::piece_letter(bq_env::PieceType::from_index(t), p),
                    cfg.piece_counts[t]
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };
    let rows = (0..cfg.rows)
        .map(|_| "*".repeat(cfg.cols))
        .collect::<Vec<_>>()
        .join("/");
    format!(
        "{} {rows} {}/{} r",
        variant.as_str(),
        bag(bq_env::Player::Red),
        bag(bq_env::Player::Black)
    )
}

fn collect_req_with_fens(variant: &str, fens: Vec<String>) -> SubmitJobRequest {
    let mut r = collect_req(variant, 16, "", "random", "rule:capture_first");
    r.collect.as_mut().unwrap().start_fens = fens;
    r
}

#[test]
fn collect_start_fens_validation() {
    let e = env("");
    e.f.networks.seed_network("n1", None, "onnx");

    let good = valid_fen(bq_env::Variant::DarkChess4x8);

    let (ok, msg, id) = e
        .jobs
        .submit(&collect_req_with_fens(
            "4x8",
            vec![good.clone(), good.clone()],
        ))
        .unwrap();
    assert!(ok, "{msg}");
    let row = e.jobs.get(&id).unwrap().unwrap();
    let JobSpecJson::Collect(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("collect spec json");
    };
    assert_eq!(spec.start_fens, vec![good.clone(), good.clone()]);
    let proto = spec.to_proto();
    assert_eq!(proto.start_fens, vec![good.clone(), good.clone()]);

    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req_with_fens(
            "4x8",
            vec![format!("{good} 垃圾字段")],
        ))
        .unwrap();
    assert!(!ok, "字段数非法应拒绝");
    assert!(msg.contains("start_fens[0]"), "{msg}");

    let wrong = valid_fen(bq_env::Variant::DarkChess4x2);
    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req_with_fens("4x8", vec![good.clone(), wrong]))
        .unwrap();
    assert!(!ok, "变体不匹配应拒绝");
    assert!(msg.contains("start_fens[1]"), "{msg}");
    assert!(msg.contains("变体不匹配"), "{msg}");

    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req_with_fens("4x8", vec![String::new()]))
        .unwrap();
    assert!(!ok, "空串应拒绝");
    assert!(msg.contains("为空串"), "{msg}");

    let over = vec![good.clone(); 1025];
    let (ok, msg, _) = e.jobs.submit(&collect_req_with_fens("4x8", over)).unwrap();
    assert!(!ok, "超限应拒绝");
    assert!(msg.contains("超限"), "{msg}");

    let (ok, msg, _) = e
        .jobs
        .submit(&collect_req_with_fens("4x8", vec![]))
        .unwrap();
    assert!(ok, "空 start_fens 应走既有路径: {msg}");
}

#[test]
fn rating_start_fens_validation() {
    let e = env("");
    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.seed_network("cand", None, "onnx");

    let good = valid_fen(bq_env::Variant::DarkChess4x8);
    let mut req = rating_req(20, "cand", "best");
    req.rating.as_mut().unwrap().start_fens = vec![good.clone()];
    let (ok, msg, id) = e.jobs.submit(&req).unwrap();
    assert!(ok, "{msg}");
    let row = e.jobs.get(&id).unwrap().unwrap();
    let JobSpecJson::Rating(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("rating spec json");
    };
    assert_eq!(spec.start_fens, vec![good.clone()]);

    let mut req = rating_req(20, "cand", "best");
    req.rating.as_mut().unwrap().variant = "4x2".into();
    req.rating.as_mut().unwrap().start_fens = vec![good.clone()];
    let (ok, msg, _) = e.jobs.submit(&req).unwrap();
    assert!(!ok, "rating 变体与 FEN 不匹配应拒绝");
    assert!(msg.contains("变体不匹配"), "{msg}");

    let bad = valid_fen(bq_env::Variant::DarkChess4x2);
    let e2 = env("");
    e2.f.networks.seed_network("best", None, "onnx");
    e2.f.networks.seed_network("cand", None, "onnx");
    let mut req = rating_req(20, "cand", "best");
    req.rating.as_mut().unwrap().start_fens = vec![bad];
    let (ok, msg, _) = e2.jobs.submit(&req).unwrap();
    assert!(!ok, "rating variant 空时按 cfg.selfplay.variant 回落校验");
    assert!(msg.contains("变体不匹配"), "{msg}");
}

#[test]
fn rating_scenario_validation() {
    let e = env("");
    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.seed_network("cand", None, "onnx");

    let mut req = rating_req(20, "cand", "best");
    req.rating.as_mut().unwrap().scenario_json = r#"{"kind":"all_revealed"}"#.into();
    let (ok, msg, id) = e.jobs.submit(&req).unwrap();
    assert!(ok, "{msg}");
    let row = e.jobs.get(&id).unwrap().unwrap();
    let JobSpecJson::Rating(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("rating spec json");
    };
    assert_eq!(
        spec.scenario_json, r#"{"kind":"all_revealed"}"#,
        "场景 JSON 须落库并在查询时回显"
    );

    let mut req = rating_req(20, "cand", "best");
    req.rating.as_mut().unwrap().scenario_json = r#"{"kind":"no_such_scenario"}"#.into();
    let (ok, msg, _) = e.jobs.submit(&req).unwrap();
    assert!(!ok, "rating 未知场景应拒绝");
    assert!(msg.contains("scenario_json"), "{msg}");

    let mut req = rating_req(20, "cand", "best");
    req.rating.as_mut().unwrap().variant = "4x4".into();
    req.rating.as_mut().unwrap().scenario_json = r#"{"kind":"random_reveals","count":99}"#.into();
    let (ok, msg, _) = e.jobs.submit(&req).unwrap();
    assert!(!ok, "rating 越界场景参数应拒绝");
    assert!(msg.contains("scenario"), "{msg}");
}

#[test]
fn collect_scenario_passthrough_validation() {
    let e = env("");
    e.f.networks.seed_network("n1", None, "onnx");

    for (input, want) in [
        ("all_revealed", r#"{"kind":"all_revealed"}"#),
        (
            r#"{"kind":"random_reveals","count":3}"#,
            r#"{"kind":"random_reveals","count":3}"#,
        ),
    ] {
        let mut req = collect_req("4x8", 4, "", "random", "rule:capture_first");
        req.collect.as_mut().unwrap().scenario_json = input.into();
        let (ok, msg, id) = e.jobs.submit(&req).unwrap();
        assert!(ok, "场景 {input} 应接受: {msg}");
        let row = e.jobs.get(&id).unwrap().unwrap();
        let JobSpecJson::Collect(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
            panic!("collect spec json");
        };
        assert_eq!(spec.scenario_json, want, "场景应规范化透传落库");
    }

    let mut req = collect_req("4x8", 4, "", "random", "rule:capture_first");
    req.collect.as_mut().unwrap().scenario_json = r#"{"kind":"no_such"}"#.into();
    let (ok, msg, _) = e.jobs.submit(&req).unwrap();
    assert!(!ok, "collect 未知场景应拒绝");
    assert!(msg.contains("scenario_json"), "{msg}");
}
