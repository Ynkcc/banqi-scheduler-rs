//! JobManager 单测：提交校验 / 种子确定性 / 完成口径聚合（含迟到上报）/
//! 完成收敛取消在飞 / 优先级抢占取消在飞 / 暂停恢复取消状态机。

use bqs_proto::{
    CollectionJobSpec, CollectPlayerSpec, DataKind, EvalJobSpec, JobKind, RatingJobSpec,
    SeedStrategy, SeedStrategyKind, SubmitJobRequest, TaskKind,
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
    let ctl = Arc::new(Control::new(&ports, cfg.clone()));
    let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), f.clock.clone()));
    let jobs = JobManager::new(
        ports.jobs.clone(),
        disp.clone(),
        ports.networks.clone(),
        ports.matches.clone(),
        ctl,
        cfg,
        f.clock.clone(),
    );
    Env { f, disp, jobs }
}

fn collect_req(variant: &str, total: u64, sha_a: &str, kind_a: &str, kind_b: &str) -> SubmitJobRequest {
    SubmitJobRequest {
        requester: "test".into(),
        kind: JobKind::JobCollect as i32,
        priority: 0,
        collect: Some(CollectionJobSpec {
            variant: variant.into(),
            scenario: 0,
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

fn seed_eval_job(env: &Env, priority: i64, total: i64, state: &str) -> String {
    let job_id = format!("j{}", env.f.jobs.next_id());
    env.f.jobs.seed_job(JobRow {
        job_id: job_id.clone(),
        kind: JobKind::JobEval as i64,
        priority,
        state: state.into(),
        total_games: total,
        completed_games: 0,
        dispatched_games: 0,
        spec_json: JobSpecJson::Eval(EvalSpecJson {
            variant: "4x8".into(),
            network_sha: "n1".into(),
            opponent_kind: "random".into(),
            ..Default::default()
        })
        .to_json(),
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
    // 派发记账（真实路径在 JobSource::dispatch_collect；测试直接补映射）
    env.f.jobs.insert_job_task(task_id, job_id, games).unwrap();
}

// ---- 提交校验 -------------------------------------------------------------

#[test]
fn submit_rejects_invalid_variant_and_totals() {
    let e = env("");
    let (ok, msg, _) = e.jobs.submit(&collect_req("9x9", 16, "", "random", "rule:capture_first")).unwrap();
    assert!(!ok);
    assert!(msg.contains("invalid_variant"), "{msg}");

    let (ok, msg, _) = e.jobs.submit(&collect_req("4x8", 3, "", "random", "rule:capture_first")).unwrap();
    assert!(!ok, "奇数局数应拒绝");
    assert!(msg.contains("偶数"), "{msg}");

    let (ok, msg, _) = e.jobs.submit(&collect_req("4x8", 1, "", "random", "rule:capture_first")).unwrap();
    assert!(!ok, "total<2 应拒绝");
    assert!(msg.contains("偶数"), "{msg}");

    let (ok, msg, _) = e.jobs.submit(&collect_req("4x8", 16, "", "mcts:quantum", "random")).unwrap();
    assert!(!ok);
    assert!(msg.contains("invalid_player_kind"), "{msg}");
}

#[test]
fn submit_rejects_unregistered_model_network_and_requires_sha() {
    let e = env("");
    // 模型类棋手未登记网络 → 拒绝
    let (ok, msg, _) = e.jobs.submit(&collect_req("4x8", 16, "ghost", "mcts_resnet", "random")).unwrap();
    assert!(!ok);
    assert!(msg.contains("network_not_registered"), "{msg}");
    // 登记后接受
    e.f.networks.seed_network("n1", None, "onnx");
    let (ok, msg, _) = e.jobs.submit(&collect_req("4x8", 16, "n1", "mcts_resnet", "random")).unwrap();
    assert!(ok, "{msg}");
    // 非模型类棋手（空 sha）→ 接受
    let (ok, msg, _) = e.jobs.submit(&collect_req("4x8", 16, "", "random", "rule:capture_first")).unwrap();
    assert!(ok, "{msg}");
    let mut r = collect_req("4x8", 16, "", "random", "rule:capture_first");
    r.collect.as_mut().unwrap().player_b.as_mut().unwrap().network_sha = "n1".into();
    let (ok, msg, _) = e.jobs.submit(&r).unwrap();
    assert!(!ok, "非模型类棋手不得携带 network_sha");
    assert!(msg.contains("必须为空"), "{msg}");
}

#[test]
fn submit_eval_rejects_odd_total_and_missing_opponent_network() {
    let e = env("");
    let req = |total: u64, opp_sha: &str| SubmitJobRequest {
        requester: "test".into(),
        kind: JobKind::JobEval as i32,
        priority: 0,
        eval: Some(EvalJobSpec {
            variant: "4x8".into(),
            network_sha: String::new(),
            opponent_kind: "expectimax_nnue".into(),
            opponent_network_sha: opp_sha.into(),
            total_games: total,
            ..Default::default()
        }),
        ..Default::default()
    };
    let (ok, msg, _) = e.jobs.submit(&req(3, "")).unwrap();
    assert!(!ok);
    assert!(msg.contains("偶数"), "{msg}");
    let (ok, msg, _) = e.jobs.submit(&req(10, "ghost")).unwrap();
    assert!(!ok);
    assert!(msg.contains("network_not_registered"), "{msg}");
    e.f.networks.seed_network("opp", None, "nnue");
    let (ok, _, id) = e.jobs.submit(&req(10, "opp")).unwrap();
    assert!(ok);
    assert!(!id.is_empty());
}

#[test]
fn submit_priority_zero_uses_kind_default() {
    let e = env("[jobs]\npriority_collect = 77\n");
    e.f.networks.seed_network("n1", None, "onnx");
    let (_, _, id) = e.jobs.submit(&collect_req("4x8", 16, "n1", "mcts_resnet", "random")).unwrap();
    let row = e.jobs.get(&id).unwrap().unwrap();
    assert_eq!(row.priority, 77);
}

// ---- 种子解析确定性 ---------------------------------------------------------

#[test]
fn seed_resolution_is_deterministic_per_seq() {
    let fixed = SeedStrategyJson { kind: SeedStrategyKind::SeedFixed as i32, fixed_seed: 42, ..Default::default() };
    assert_eq!(resolve_seed(&fixed, 0), 42);
    assert_eq!(resolve_seed(&fixed, 999), 42);

    let inc = SeedStrategyJson { kind: SeedStrategyKind::SeedIncremental as i32, base_seed: 1000, ..Default::default() };
    assert_eq!(resolve_seed(&inc, 0), 1000);
    assert_eq!(resolve_seed(&inc, 7), 1007);

    let rnd = SeedStrategyJson { kind: SeedStrategyKind::SeedRandom as i32, base_seed: 7, ..Default::default() };
    assert_eq!(resolve_seed(&rnd, 3), resolve_seed(&rnd, 3), "同 seq 恒同种子");
    assert_ne!(resolve_seed(&rnd, 3), resolve_seed(&rnd, 4), "不同 seq 应派生不同种子");

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
    // 单点区间退化为 min
    let point = SeedStrategyJson {
        kind: SeedStrategyKind::SeedRange as i32,
        min_seed: 11,
        max_seed: 11,
        ..Default::default()
    };
    assert_eq!(resolve_seed(&point, 5), 11);
}

// ---- 完成口径聚合 -----------------------------------------------------------

#[test]
fn record_report_aggregates_and_concludes_with_cancellation() {
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 100, "running");
    // 派发 3 个在飞任务（64 局，超过剩余需求——在飞不受封顶）
    register_in_flight(&e, "t1", &job_id, "w1", 32);
    register_in_flight(&e, "t2", &job_id, "w2", 32);
    e.jobs.note_dispatch("t3", &job_id, 32, "w3", TaskKind::TaskCollect, "", "", "", 0, None).unwrap();

    // 非映射任务（rating 等）静默忽略
    e.jobs.record_report("unknown_task", 16).unwrap();

    // t1 上报 32 局：completed=32 < 100，不收敛
    e.jobs.record_report("t1", 32).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 32);
    assert_eq!(row.state, "running");

    // t2 上报 32 局：completed=64 < 100
    e.jobs.record_report("t2", 32).unwrap();

    // t3 上报 32 局：completed=96... 96 < 100 → 仍 running（不含在飞口径）
    e.jobs.record_report("t3", 32).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 96);
    assert_eq!(row.state, "running");
    assert!(e.jobs.cancel_task_ids_for_worker("w1").is_empty(), "未收敛不取消");

    // 补一批：t4 上报 16 局 → completed=112 ≥ 100 → 收敛 + 取消全部在飞
    e.jobs.note_dispatch("t4", &job_id, 16, "w1", TaskKind::TaskCollect, "", "", "", 0, None).unwrap();
    e.jobs.record_report("t4", 16).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.state, "completed");
    // 在飞取消集合含 w2 名下任务（t1/t3 已聚合删映射，但 disp 任务仍在 → 仍取消）
    let mut got = e.jobs.cancel_task_ids_for_worker("w1");
    got.sort();
    assert_eq!(got, vec!["t1".to_string(), "t4".to_string()], "w1 名下在飞应被取消");
    assert_eq!(e.jobs.cancel_task_ids_for_worker("w2"), vec!["t2".to_string()]);
    // 下发即清除：二次心跳为空
    assert!(e.jobs.cancel_task_ids_for_worker("w1").is_empty());
    assert!(e.jobs.cancel_task_ids_for_worker("w2").is_empty());
}

#[test]
fn record_report_accepts_late_report_after_restart_gap() {
    // 重启后内存任务表丢失：映射行在 job_tasks（fake 内亦同），迟到上报仍聚合。
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 30, "running");
    e.jobs.note_dispatch("t1", &job_id, 32, "w1", TaskKind::TaskCollect, "", "", "", 0, None).unwrap();
    // 模拟重启：内存任务表清空（映射与 job 行在库）
    e.disp.delete("t1");
    e.jobs.record_report("t1", 32).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 32);
    assert_eq!(row.state, "completed", "32 ≥ total 30 → 收敛");
    // 迟到的重复上报：映射已删，静默忽略不双计
    e.jobs.record_report("t1", 32).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 32);
    assert_eq!(row.state, "completed");
    // 重启后在飞视为丢失：无取消下发
    assert!(e.jobs.cancel_task_ids_for_worker("w1").is_empty());
}

#[test]
fn zero_games_report_ignored() {
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 50, "running");
    e.jobs.note_dispatch("t1", &job_id, 16, "w1", TaskKind::TaskCollect, "", "", "", 0, None).unwrap();
    e.jobs.record_report("t1", 0).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 0, "0 局上报不计聚合");
    assert_eq!(row.state, "running");
}

// ---- 完成收敛 + 优先级抢占 ---------------------------------------------------

#[test]
fn preemption_cancels_lower_priority_in_flight_on_submit() {
    let e = env("");
    let low = seed_collect_job(&e, 10, 1000, "running");
    register_in_flight(&e, "low-t1", &low, "w1", 32);
    register_in_flight(&e, "low-t2", &low, "w2", 32);

    // 提交更高优先级 job：立即取消更低优先级 job 的在飞
    e.f.networks.seed_network("n1", None, "onnx");
    let (_, _, high) = e.jobs.submit(&collect_req("4x8", 16, "n1", "mcts_resnet", "random")).unwrap();
    assert_eq!(e.jobs.cancel_task_ids_for_worker("w1"), vec!["low-t1".to_string()]);
    assert_eq!(e.jobs.cancel_task_ids_for_worker("w2"), vec!["low-t2".to_string()]);
    // 被抢占 job 仍是 running
    assert_eq!(e.jobs.get(&low).unwrap().unwrap().state, "running");
    assert_eq!(e.jobs.get(&high).unwrap().unwrap().state, "running");
    // 同优先级/更高优先级 job 的在飞不被取消
    register_in_flight(&e, "high-t1", &high, "w3", 8);
    e.jobs.preemption_sweep(true);
    assert!(e.jobs.cancel_task_ids_for_worker("w3").is_empty(), "最高优先级的在飞不取消");
}

#[test]
fn preemption_skips_paused_high_priority() {
    let e = env("");
    let low = seed_collect_job(&e, 10, 1000, "running");
    register_in_flight(&e, "p-t1", &low, "w1", 32);
    // 最高可派发优先级仍是 low（高优先级 job paused）→ 不抢占
    seed_eval_job(&e, 100, 100, "paused");
    e.jobs.preemption_sweep(true);
    assert!(e.jobs.cancel_task_ids_for_worker("w1").is_empty(), "无可派发高优先级 job 不抢占");
}

#[test]
fn preemption_resume_cancels_lower_priority_in_flight() {
    let e = env("");
    let low = seed_collect_job(&e, 10, 1000, "running");
    register_in_flight(&e, "low-t1", &low, "w1", 32);
    let high = seed_eval_job(&e, 100, 100, "paused");
    // resume 高优先级 job：恢复即变为可派发 → 立即取消更低优先级在飞
    let (ok, msg, _) = e.jobs.control(&high, "resume").unwrap();
    assert!(ok, "{msg}");
    assert_eq!(e.jobs.cancel_task_ids_for_worker("w1"), vec!["low-t1".to_string()]);
}

#[test]
fn completion_only_concludes_when_total_reached_not_persistent() {
    // 持续型（total=0）：completed 无上限，永不收敛
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 0, "running");
    e.jobs.note_dispatch("t1", &job_id, 16, "w1", TaskKind::TaskCollect, "", "", "", 0, None).unwrap();
    e.jobs.record_report("t1", 16).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 16);
    assert_eq!(row.state, "running", "持续型 job 不收敛");
}

// ---- 暂停 / 恢复 / 取消状态机 -----------------------------------------------

#[test]
fn pause_resume_cancel_state_machine() {
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 100, "running");
    register_in_flight(&e, "t1", &job_id, "w1", 32);

    // pause：running → paused
    let (ok, msg, st) = e.jobs.control(&job_id, "pause").unwrap();
    assert!(ok, "{msg}");
    assert_eq!(st, "paused");
    // paused 再 pause → 拒绝
    let (ok, msg, _) = e.jobs.control(&job_id, "pause").unwrap();
    assert!(!ok);
    assert!(msg.contains("pause_requires_running"), "{msg}");

    // resume：paused → running
    let (ok, _, st) = e.jobs.control(&job_id, "resume").unwrap();
    assert!(ok);
    assert_eq!(st, "running");
    // running 再 resume → 拒绝
    let (ok, msg, _) = e.jobs.control(&job_id, "resume").unwrap();
    assert!(!ok);
    assert!(msg.contains("resume_requires_paused"), "{msg}");

    // cancel：running → cancelled + 取消在飞
    let (ok, _, st) = e.jobs.control(&job_id, "cancel").unwrap();
    assert!(ok);
    assert_eq!(st, "cancelled");
    assert_eq!(e.jobs.cancel_task_ids_for_worker("w1"), vec!["t1".to_string()]);
    // 终态不可再控制
    for action in ["pause", "resume", "cancel"] {
        let (ok, msg, _) = e.jobs.control(&job_id, action).unwrap();
        assert!(!ok, "{action} 于终态应拒绝");
        assert!(msg.contains("terminal") || msg.contains("requires"), "{msg}");
    }
    // 未知 job / 非法 action
    let (ok, msg, _) = e.jobs.control("nope", "pause").unwrap();
    assert!(!ok && msg.contains("not_found"), "{msg}");
    let (ok, msg, _) = e.jobs.control(&job_id, "explode").unwrap();
    assert!(!ok && msg.contains("invalid_control"), "{msg}");
}

#[test]
fn cancelled_late_report_still_counts() {
    // 取消后迟到上报照常聚合（取消≠作废；已上报部分不回滚）
    let e = env("");
    let job_id = seed_collect_job(&e, 50, 100, "running");
    e.jobs.note_dispatch("t1", &job_id, 32, "w1", TaskKind::TaskCollect, "", "", "", 0, None).unwrap();
    e.jobs.control(&job_id, "cancel").unwrap();
    e.jobs.record_report("t1", 32).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 32, "取消后迟到上报仍聚合");
    assert_eq!(row.state, "cancelled", "终态不被聚合改写");
}

#[test]
fn selfplay_bootstrap_is_idempotent() {
    let e = env("[selfplay]\ndata_kind = \"nnue\"\n");
    e.jobs.ensure_selfplay_job().unwrap();
    let rows = e.jobs.list(None, Some(JobKind::JobSelfplay as i64)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, "running");
    assert_eq!(rows[0].total_games, 0, "引导的是持续型 job");
    assert_eq!(rows[0].priority, 0);
    // 重复引导不新增
    e.jobs.ensure_selfplay_job().unwrap();
    assert_eq!(e.jobs.list(None, Some(JobKind::JobSelfplay as i64)).unwrap().len(), 1);
    // paused 的 selfplay job 也算存在（不重复引导）
    let other = env("");
    other.jobs.ensure_selfplay_job().unwrap();
    let id = other.jobs.list(None, Some(JobKind::JobSelfplay as i64)).unwrap()[0].job_id.clone();
    other.jobs.control(&id, "pause").unwrap();
    other.jobs.ensure_selfplay_job().unwrap();
    assert_eq!(other.jobs.list(None, Some(JobKind::JobSelfplay as i64)).unwrap().len(), 1);
}

#[test]
fn eval_job_auto_creation_dedups_by_network_and_opponent() {
    let e = env("[eval]\ngames = 40\n");
    e.f.networks.seed_network("n1", None, "onnx");
    e.f.networks.promote_best("n1").unwrap();
    e.jobs.create_eval_job("", "random", "").unwrap();
    e.jobs.create_eval_job("", "random", "").unwrap(); // 幂等
    let rows = e.jobs.list(None, Some(JobKind::JobEval as i64)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].total_games, 40);
    assert_eq!(rows[0].priority, 100, "eval 默认优先级");
    let JobSpecJson::Eval(spec) = JobSpecJson::parse(&rows[0].spec_json).unwrap() else {
        panic!("eval spec json");
    };
    assert_eq!(spec.network_sha, "n1", "空 sha 解析为当前 best");
    assert_eq!(spec.opponent_kind, "random");
    // 不同对手 → 新 job
    e.jobs.create_eval_job("", "rule:capture_first", "").unwrap();
    assert_eq!(e.jobs.list(None, Some(JobKind::JobEval as i64)).unwrap().len(), 2);
}

#[test]
fn dispatchable_filters_state_and_completion() {
    let e = env("");
    let running = seed_collect_job(&e, 50, 100, "running");
    seed_collect_job(&e, 50, 100, "paused");
    // running 且 completed < total → 可派发
    assert_eq!(e.jobs.dispatchable(JobKind::JobCollect).unwrap().len(), 1);
    // completed ≥ total → 不可派发
    e.jobs.note_dispatch("t1", &running, 100, "w1", TaskKind::TaskCollect, "", "", "", 0, None).unwrap();
    e.jobs.record_report("t1", 100).unwrap();
    assert!(e.jobs.dispatchable(JobKind::JobCollect).unwrap().is_empty());
    // 持续型（total=0）恒可派发
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
fn eval_report_via_job_link_records_completion() {
    // eval 任务的上报（ReportMatchResult 路径）走同一 record_report 聚合
    let e = env("");
    let job_id = seed_eval_job(&e, 100, 100, "running");
    e.jobs.note_eval_claim("et1", &job_id).unwrap();
    // disp 在飞（evalc 路径登记的形态）
    let mut t = crate::dispatch::RunningTask::new(TaskKind::TaskEval);
    t.worker_id = "w1".into();
    t.games = 50;
    t.job_id = job_id.clone();
    e.disp.register("et1", t);
    e.jobs.record_report("et1", 50).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.completed_games, 50);
    // 100 局达成
    e.jobs.note_eval_claim("et2", &job_id).unwrap();
    let mut t2 = crate::dispatch::RunningTask::new(TaskKind::TaskEval);
    t2.worker_id = "w1".into();
    t2.games = 50;
    t2.job_id = job_id.clone();
    e.disp.register("et2", t2);
    e.jobs.record_report("et2", 50).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.state, "completed");
    // 收敛即取消该 job 全部在飞（et1/et2 的 disp 任务仍为 Assigned 形态）
    let mut got = e.jobs.cancel_task_ids_for_worker("w1");
    got.sort();
    assert_eq!(got, vec!["et1".to_string(), "et2".to_string()]);
}

#[test]
fn fake_clock_preemption_throttled() {
    let e = env("");
    let low = seed_collect_job(&e, 10, 1000, "running");
    register_in_flight(&e, "low-t1", &low, "w1", 32);
    let _high = seed_eval_job(&e, 100, 100, "running");
    // 首次惰性 sweep（时钟 1000）：取消更低优先级在飞
    e.jobs.preemption_sweep(false);
    assert_eq!(e.jobs.cancel_task_ids_for_worker("w1"), vec!["low-t1".to_string()]);
    register_in_flight(&e, "low-t2", &low, "w1", 32);
    e.jobs.preemption_sweep(false); // 节流：不扫
    assert!(e.jobs.cancel_task_ids_for_worker("w1").is_empty(), "节流窗口内不重复取消");
    e.f.clock.advance(consts::JOB_PREEMPT_INTERVAL.as_secs() as i64);
    e.jobs.preemption_sweep(false);
    // sweep 会重发同 job 全部在飞的取消（含先前已下发过的 low-t1）
    let mut got = e.jobs.cancel_task_ids_for_worker("w1");
    got.sort();
    assert_eq!(got, vec!["low-t1".to_string(), "low-t2".to_string()]);
}

#[test]
fn submit_timestamps_from_injected_clock() {
    // created_at / updated_at 取自注入时钟（Fakes 起始 1000）
    let e = env("");
    e.f.networks.seed_network("n1", None, "onnx");
    let (_, _, id) = e.jobs.submit(&collect_req("4x8", 16, "n1", "mcts_resnet", "random")).unwrap();
    let row = e.jobs.get(&id).unwrap().unwrap();
    assert_eq!(row.created_at, 1000);
    assert_eq!(row.updated_at, row.created_at);
}

// ---- rating job（手动 SPRT 对打）--------------------------------------------

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
            sprt_elo1: 0.0,
            sprt_alpha: 0.0,
            sprt_beta: 0.0,
            side_a: None,
            side_b: None,
        }),
        ..Default::default()
    }
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
    // 候选未登记 → 拒绝
    let (ok, msg, _) = e.jobs.submit(&rating_req(20, "ghost", "")).unwrap();
    assert!(!ok);
    assert!(msg.contains("network_not_registered"), "{msg}");
    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.promote_best("best").unwrap();
    e.f.networks.seed_network("cand", None, "onnx");
    // 奇数局数 → 拒绝；1 局 → 拒绝
    for total in [1u64, 3] {
        let (ok, msg, _) = e.jobs.submit(&rating_req(total, "cand", "")).unwrap();
        assert!(!ok, "total={total} 应拒绝");
        assert!(msg.contains("total_games"), "{msg}");
    }
    // 指定对手未登记 → 拒绝
    let (ok, msg, _) = e.jobs.submit(&rating_req(20, "cand", "ghost")).unwrap();
    assert!(!ok);
    assert!(msg.contains("network_not_registered"), "{msg}");
    // 对手空 = 当前 best：接受并对打行以 best 为对手，spec 回写 match_id
    let (ok, msg, id) = e.jobs.submit(&rating_req(20, "cand", "")).unwrap();
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
    assert_eq!(m.opponent, "best", "对手空解析为当前 best");
    assert_eq!(m.target_games, 10, "对打目标（对数）= 局数上限一半");
    // variant 非法 → 拒绝
    let mut bad = rating_req(20, "cand", "");
    bad.rating.as_mut().unwrap().variant = "9x9".into();
    let (ok, msg, _) = e.jobs.submit(&bad).unwrap();
    assert!(!ok);
    assert!(msg.contains("invalid_variant"), "{msg}");
    // 无 best 且对手空 → 拒绝
    let e2 = env("");
    e2.f.networks.seed_network("cand", None, "onnx");
    let (ok, msg, _) = e2.jobs.submit(&rating_req(20, "cand", "")).unwrap();
    assert!(!ok);
    assert!(msg.contains("no_best_network"), "{msg}");
    // sprt 参数范围校验
    let e3 = env("");
    e3.f.networks.seed_network("best", None, "onnx");
    e3.f.networks.promote_best("best").unwrap();
    e3.f.networks.seed_network("cand", None, "onnx");
    let mut bad = rating_req(20, "cand", "");
    bad.rating.as_mut().unwrap().sprt_alpha = 1.5;
    let (ok, msg, _) = e3.jobs.submit(&bad).unwrap();
    assert!(!ok);
    assert!(msg.contains("sprt_alpha"), "{msg}");
}

#[test]
fn rating_cancel_stops_match_and_in_flight() {
    let e = env("");
    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.promote_best("best").unwrap();
    e.f.networks.seed_network("cand", None, "onnx");
    let (_, _, id) = e.jobs.submit(&rating_req(20, "cand", "")).unwrap();
    let row = e.jobs.get(&id).unwrap().unwrap();
    let JobSpecJson::Rating(spec) = JobSpecJson::parse(&row.spec_json).unwrap() else {
        panic!("rating spec json");
    };
    let match_id = spec.match_id;
    // 在飞任务 + 派发记账（真实路径在 JobSource::dispatch_rating）
    e.jobs.note_dispatch("rt1", &id, 4, "w1", TaskKind::TaskRating, "cand", "best", "", match_id, None).unwrap();
    // 取消：job cancelled + 对打 cancelled + 在飞取消
    let (ok, msg, st) = e.jobs.control(&id, "cancel").unwrap();
    assert!(ok, "{msg}");
    assert_eq!(st, "cancelled");
    let m = e.f.matches.get_match(match_id).unwrap().unwrap();
    assert_eq!(m.status, crate::types::match_status::CANCELLED, "对打应置 cancelled");
    assert_eq!(e.jobs.cancel_task_ids_for_worker("w1"), vec!["rt1".to_string()]);
    // 已 cancelled 的对打拒绝继续累加（迟到上报在聚合闸门被拒）
    let (_, ok2) = e
        .f
        .matches
        .add_match_games(match_id, bqs_judgment::Hexanomial([1, 0, 0, 0, 0, 0]), 2)
        .unwrap();
    assert!(!ok2);
}

#[test]
fn rating_complete_job_on_verdict_is_idempotent() {
    let e = env("");
    let match_id = e.f.matches.seed_match("cand", "best", 10);
    let job_id = seed_rating_job(&e, match_id, 0, "running");
    e.jobs.note_dispatch("rt1", &job_id, 4, "w1", TaskKind::TaskRating, "cand", "best", "", match_id, None).unwrap();
    // SPRT 判停 → job 收敛 + 在飞取消；重复收敛幂等
    e.jobs.complete_rating_job(&job_id).unwrap();
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.state, "completed");
    assert_eq!(e.jobs.cancel_task_ids_for_worker("w1"), vec!["rt1".to_string()]);
    e.jobs.complete_rating_job(&job_id).unwrap();
    assert_eq!(e.jobs.get(&job_id).unwrap().unwrap().state, "completed");
    // 终态 job 不再可派发
    assert!(e.jobs.dispatchable(JobKind::JobRating).unwrap().is_empty());
}

// ---- rating job sides 形态（双方棋手自由组合）--------------------------------

fn sides_rating_req(variant_sides: Option<(CollectPlayerSpec, CollectPlayerSpec)>, total: u64) -> SubmitJobRequest {
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
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn nnue_side(sha: &str) -> CollectPlayerSpec {
    CollectPlayerSpec { kind: "expectimax_nnue".into(), network_sha: sha.into(), search: None }
}

#[test]
fn rating_sides_submit_normalizes_and_freezes() {
    let e = env("");
    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.promote_best("best").unwrap();
    e.f.networks.seed_network("cand", None, "onnx");
    // side_a 模型类空 sha = 当前 best（冻结为具体 sha）；side_b 非网络类
    // 双方各自携带独立搜索参数（互不共享，对照 collect 的 player 承载）
    let req = sides_rating_req(
        Some((
            CollectPlayerSpec {
                kind: "expectimax_nnue".into(),
                network_sha: String::new(),
                search: Some(bqs_proto::CollectSearchParams {
                    expectimax_node_budget: 4000,
                    labeler_mode: "best".into(),
                    ..Default::default()
                }),
            },
            CollectPlayerSpec {
                kind: "expectimax_rule".into(),
                network_sha: String::new(),
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
    // kind 规范化 + 空 sha 冻结为当前 best
    assert_eq!(spec.side_a.kind, "expectimax_nnue");
    assert_eq!(spec.side_a.network_sha, "best", "模型类空 sha 冻结为当前 best");
    assert_eq!(spec.side_b.kind, "expectimax_rule");
    assert_eq!(spec.side_b.network_sha, "", "非网络类 sha 为空");
    // 双方搜索参数独立保留
    assert_eq!(spec.side_a.search.as_ref().unwrap().expectimax_node_budget, 4000);
    assert_eq!(spec.side_a.search.as_ref().unwrap().labeler_mode, "best");
    assert_eq!(spec.side_b.search.as_ref().unwrap().expectimax_node_budget, 1000);
    // 对发行以冻结 sha 入行（非网络类 side 为空串）
    let m = e.f.matches.get_match(spec.match_id).unwrap().unwrap();
    assert_eq!(m.candidate, "best");
    assert_eq!(m.opponent, "");
}

#[test]
fn rating_sides_validation_rejects() {
    let e = env("");
    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.promote_best("best").unwrap();
    e.f.networks.seed_network("cand", None, "onnx");
    // 只填一侧 → 拒绝
    let mut req = sides_rating_req(None, 20);
    req.rating.as_mut().unwrap().side_a = Some(nnue_side("cand"));
    req.rating.as_mut().unwrap().side_b = None;
    let (ok, msg, _) = e.jobs.submit(&req).unwrap();
    assert!(!ok);
    assert!(msg.contains("成对填写"), "{msg}");
    // 非法 kind → 拒绝
    let mut bad = nnue_side("cand");
    bad.kind = "chess_engine".into();
    let (ok, msg, _) = e.jobs.submit(&sides_rating_req(Some((bad, nnue_side(""))), 20)).unwrap();
    assert!(!ok);
    assert!(msg.contains("invalid_player_kind"), "{msg}");
    // 模型类 sha 未登记 → 拒绝
    let (ok, msg, _) = e
        .jobs
        .submit(&sides_rating_req(Some((nnue_side("ghost"), nnue_side(""))), 20))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("network_not_registered"), "{msg}");
    // 非网络类带 sha → 拒绝（对照 collect 校验口径）
    let mut rule = nnue_side("");
    rule.kind = "random".into();
    rule.network_sha = "cand".into();
    let (ok, msg, _) = e
        .jobs
        .submit(&sides_rating_req(Some((nnue_side("cand"), rule)), 20))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("network_sha 必须为空"), "{msg}");
    // 模型类空 sha 但无 best → 拒绝
    let e2 = env("");
    e2.f.networks.seed_network("cand", None, "onnx");
    let (ok, msg, _) = e2
        .jobs
        .submit(&sides_rating_req(Some((nnue_side(""), nnue_side(""))), 20))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("no_best_network"), "{msg}");
    // sides 形态下奇数局数 → 拒绝（共享校验不变）
    let (ok, msg, _) = e
        .jobs
        .submit(&sides_rating_req(Some((nnue_side("cand"), nnue_side(""))), 3))
        .unwrap();
    assert!(!ok);
    assert!(msg.contains("total_games"), "{msg}");
}

#[test]
fn is_rating_match_filters_pending_match() {
    // rating job 的对打不得劫持 selfplay 混入查询
    let e = env("");
    let gate_id = e.f.matches.seed_match("cand", "best", 10);
    let rating_id = e.f.matches.seed_match("rcand", "best", 10);
    let _job = seed_rating_job(&e, rating_id, 0, "running");
    assert!(e.jobs.is_rating_match(rating_id));
    assert!(!e.jobs.is_rating_match(gate_id));
    assert!(!e.jobs.is_rating_match(999));
}
