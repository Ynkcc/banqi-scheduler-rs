use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{MatchResult, TaskKind};

use crate::{
    consts::EVAL_MAX_ATTEMPTS,
    control::Control,
    dispatch::{TaskDispatcher, TaskState},
    test_support::Fakes,
    types::{EvalDelta, EvalResult},
};

use super::*;

struct TestEnv {
    f: Fakes,
    disp: Arc<TaskDispatcher>,
    evalc: EvalCoordinator,
    ctl: Arc<Control>,
}

fn env(config_extra: &str) -> TestEnv {
    let f = Fakes::new();
    let cfg = Arc::new(Config::parse(config_extra, "test").unwrap());
    let ports = f.ports();
    let ctl = Arc::new(Control::new(&ports, cfg.clone()));
    let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), f.clock.clone()));
    let evalc = EvalCoordinator::new(
        &ports,
        cfg.clone(),
        f.clock.clone(),
        ctl.clone(),
        disp.clone(),
    );
    TestEnv {
        f,
        disp,
        evalc,
        ctl,
    }
}

fn enabled_cfg() -> &'static str {
    "[eval]\nenabled = true\ngames = 100\ngames_per_task = 10\nevery_n_promotions = 1\nno_progress_n = 2\nno_progress_eps = 0.01\nrollback_eps = 0.04\n"
}

fn delta(games: i64, wins: i64) -> EvalDelta {
    EvalDelta {
        games,
        wins,
        ..Default::default()
    }
}

fn claim_spec(env: &TestEnv, spec: &str) -> ClaimedEvalTask {
    loop {
        let claimed = env.evalc.claim_task("w1", 4).expect("应有可认领分片");
        if claimed.opponent_spec == spec {
            return claimed;
        }
        env.evalc.release_claim(&claimed.task_id);
    }
}

fn claim_and_report(env: &TestEnv, spec: &str, games: i64, wins: i64) -> EvalReportAck {
    let claimed = claim_spec(env, spec);
    env.evalc
        .report_result(&claimed.task_id, "", delta(games, wins))
}

fn eval_row(
    sha: &str,
    spec: &str,
    wins: i64,
    losses: i64,
    num_games: i64,
    created_at: i64,
) -> EvalResult {
    EvalResult {
        id: 0,
        network_sha: sha.to_string(),
        opponent_spec: spec.to_string(),
        wins,
        draws: 0,
        losses,
        num_games,
        avg_moves: 0.0,
        created_at,
        red_wins: 0,
        black_wins: 0,
        draw_no_capture: 0,
        draw_truncation: 0,
    }
}

#[test]
fn enqueue_dedup_and_every_n_promotions_throttle() {
    let raw = "[eval]\nenabled = true\ngames = 50\nevery_n_promotions = 2\n";
    let e = env(raw);
    e.f.networks.seed_network("n1", None, "onnx");

    e.evalc.on_new_best("n1").unwrap();
    assert_eq!(e.evalc.pending_eval(), 0);

    e.evalc.on_new_best("n1").unwrap();
    assert_eq!(e.evalc.pending_eval(), 4);

    e.evalc.on_new_best("n1").unwrap();
    e.evalc.on_new_best("n1").unwrap();
    assert_eq!(e.evalc.pending_eval(), 4);

    assert_eq!(e.f.eval.get_int_setting("eval_promotions", 0).unwrap(), 4);
}

#[test]
fn report_eval_result_derives_wld_from_pair_buckets() {
    let e = env(enabled_cfg());
    e.f.networks.seed_network("n", None, "onnx");
    e.evalc.on_new_best("n").unwrap();
    let c = e.evalc.claim_task("w1", 4).unwrap();
    let task = e.disp.lookup(&c.task_id).unwrap();
    let req = MatchResult {
        worker_id: "w1".into(),
        task_id: c.task_id.clone(),
        kind: TaskKind::TaskEval as i32,
        games: 8,

        pair_ll: 1,
        pair_ld: 1,
        pair_dd: 1,
        pair_wl: 1,
        pair_dw: 1,
        pair_ww: 1,
        ..Default::default()
    };
    let ack = e.evalc.report_eval_result(&task, &req).unwrap();
    assert!(ack.accepted, "{}", ack.message);

    let row =
        e.f.eval
            .get_eval_job("n", "random")
            .unwrap()
            .expect("应有聚合行");
    assert_eq!(row.num_games, 8);
    assert_eq!((row.wins, row.losses, row.draws), (3, 3, 3));
}

#[test]
fn ensure_best_evaluated_fills_missing_opponents() {
    let e = env(enabled_cfg());
    e.f.networks.seed_network("best1", None, "onnx");
    e.f.networks.promote_best("best1").unwrap();

    e.f.eval
        .upsert_eval_result(eval_row("best1", "random", 1, 0, 1, 1))
        .unwrap();
    e.evalc.ensure_best_evaluated();
    assert_eq!(e.evalc.pending_eval(), 3, "已有结果的对手不再补");

    e.evalc.ensure_best_evaluated();
    assert_eq!(e.evalc.pending_eval(), 3);
}

#[test]
fn trigger_manual_error_paths() {
    let e = env(enabled_cfg());

    assert!(e.evalc.trigger_manual().is_err());
    e.f.networks.seed_network("b", None, "onnx");
    e.f.networks.promote_best("b").unwrap();
    let n = e.evalc.trigger_manual().unwrap();
    assert_eq!(n, 4);

    let mut o = EvalOverride::default();
    o.enabled = Some(false);
    e.ctl.set_eval_override(o).unwrap();
    assert!(e.evalc.trigger_manual().is_err());
}

#[test]
fn claim_respects_net_remaining_quota_and_stale_inflight() {
    let e = env(enabled_cfg());
    e.f.networks.seed_network("n", None, "onnx");
    e.evalc.on_new_best("n").unwrap();

    let c1 = e.evalc.claim_task("w1", 4).unwrap();
    assert_eq!(c1.opponent_spec, "random");
    assert_eq!(c1.games, 10);
    let t1 = e.disp.lookup(&c1.task_id).unwrap();
    assert_eq!(t1.games, 10, "认领即在 disp 登记在飞");
    assert_eq!(t1.kind, TaskKind::TaskEval);

    let c2 = e.evalc.claim_task("w2", 4).unwrap();
    assert_ne!(c1.task_id, c2.task_id);

    let mut random_shards = 2;
    let mut moved_on = false;
    while let Some(c) = e.evalc.claim_task("w3", 4) {
        if c.opponent_spec == "random" {
            random_shards += 1;
            assert!(random_shards <= 10, "在飞不应超过净剩余额度");
        } else {
            moved_on = true;
            e.evalc.release_claim(&c.task_id);
            break;
        }
    }
    assert_eq!(random_shards, 10);
    assert!(moved_on, "额度覆盖后应跳到下一作业");

    e.f.clock.advance(16 * 60);
    assert!(e.evalc.claim_task("w4", 4).is_some());
}

#[test]
fn report_partial_then_finalize_at_target() {
    let e = env(enabled_cfg());
    e.f.networks.seed_network("n", None, "onnx");
    e.evalc.on_new_best("n").unwrap();

    for i in 0..10 {
        let claimed = claim_spec(&e, "random");
        let ack = e.evalc.report_result(&claimed.task_id, "", delta(10, 6));
        if i == 9 {
            assert!(ack.concluded, "第 10 片应触发收尾");
        } else {
            assert!(!ack.concluded);
        }
    }

    let r = e.f.eval.get_eval_result("n", "random").unwrap().unwrap();
    assert_eq!(r.num_games, 100);
    assert_eq!(r.wins, 60);
    assert!(e.f.eval.get_eval_job("n", "random").unwrap().is_none());

    let _ = TaskState::Completed;
}

#[test]
fn zero_games_report_ignored_but_task_completed() {
    let e = env(enabled_cfg());
    e.f.networks.seed_network("n", None, "onnx");
    e.evalc.on_new_best("n").unwrap();
    let claimed = claim_spec(&e, "random");
    let ack = e.evalc.report_result(&claimed.task_id, "", delta(0, 0));
    assert_eq!(ack.message, "eval_zero_games_ignored");
    assert_eq!(
        e.disp.lookup(&claimed.task_id).unwrap().state,
        TaskState::Completed,
        "0 局上报也收尾任务本身"
    );

    assert!(
        e.f.eval
            .get_eval_job("n", &claimed.opponent_spec)
            .unwrap()
            .is_some()
    );
}

#[test]
fn stale_attempts_exhausted_drops_job() {
    let raw = "[eval]\nenabled = true\ngames = 10\ngames_per_task = 0\n";
    let e = env(raw);
    e.f.networks.seed_network("n", None, "onnx");
    e.evalc.on_new_best("n").unwrap();

    for _ in 0..EVAL_MAX_ATTEMPTS {
        let c = e.evalc.claim_task("w", 4).unwrap();
        e.f.clock
            .advance(crate::consts::EVAL_TASK_STALE_AFTER.as_secs() as i64 + 1);
        assert!(
            e.disp.lookup(&c.task_id).is_some(),
            "超期在飞台账保留（仅额度失效）"
        );
    }
    let next = e.evalc.claim_task("w", 4);
    assert!(
        next.is_none() || next.unwrap().opponent_spec != "random",
        "random 作业 attempts 达上限且在飞为空：应被丢弃"
    );
    assert!(e.f.eval.get_eval_job("n", "random").unwrap().is_none());
}

#[test]
fn in_flight_present_blocks_drop() {
    let raw = "[eval]\nenabled = true\ngames = 10\ngames_per_task = 0\n";
    let e = env(raw);
    e.f.networks.seed_network("n", None, "onnx");
    e.evalc.on_new_best("n").unwrap();

    for _ in 0..(EVAL_MAX_ATTEMPTS - 1) {
        let c = e.evalc.claim_task("w", 4).unwrap();
        e.f.clock
            .advance(crate::consts::EVAL_TASK_STALE_AFTER.as_secs() as i64 + 1);
        e.evalc.release_claim(&c.task_id);
    }
    let _alive = e.evalc.claim_task("w", 4).unwrap();

    let c2 = e.evalc.claim_task("w2", 4).unwrap();
    assert_ne!(c2.opponent_spec, "random");
    e.evalc.release_claim(&c2.task_id);
    assert!(e.f.eval.get_eval_job("n", "random").unwrap().is_some());
}

#[test]
fn stop_criterion_after_consecutive_no_progress() {
    let e = env(enabled_cfg());
    e.f.networks.seed_network("n1", None, "onnx");

    for created_at in 1..=2i64 {
        e.f.eval
            .upsert_eval_result(eval_row(
                &format!("old{created_at}"),
                "random",
                50,
                50,
                100,
                created_at,
            ))
            .unwrap();
    }
    e.f.networks.promote_best("n1").unwrap();
    e.evalc.on_new_best("n1").unwrap();

    let _ = claim_and_report(&e, "random", 100, 50);

    let (stopped, reason) = e.ctl.should_stop();
    assert!(stopped, "应触发停机");
    assert!(reason.contains("eval_no_progress"), "{reason}");
    assert!(reason.contains("random"), "{reason}");
}

#[test]
fn no_progress_does_not_fire_when_improving_or_disabled() {
    let e = env(enabled_cfg());
    e.f.networks.seed_network("n1", None, "onnx");
    for created_at in 1..=2i64 {
        e.f.eval
            .upsert_eval_result(eval_row(
                &format!("old{created_at}"),
                "random",
                40,
                60,
                100,
                created_at,
            ))
            .unwrap();
    }
    e.f.networks.promote_best("n1").unwrap();
    e.evalc.on_new_best("n1").unwrap();
    let _ = claim_and_report(&e, "random", 100, 70);
    assert!(!e.ctl.should_stop().0);

    let raw = "[eval]\nenabled = true\ngames = 10\nno_progress_n = 0\n";
    let e2 = env(raw);
    e2.f.networks.seed_network("n", None, "onnx");
    e2.evalc.on_new_best("n").unwrap();
    let _ = claim_and_report(&e2, "random", 10, 0);
    assert!(!e2.ctl.should_stop().0);
}

#[test]
fn rollback_to_parent_when_regressed_on_strongest_gauge() {
    let e = env(enabled_cfg());

    e.f.networks.seed_network("parent", None, "nnue");
    e.f.networks.seed_network("child", Some("parent"), "nnue");
    e.f.eval
        .upsert_eval_result(eval_row(
            "parent",
            "rule:material_expectimax",
            60,
            40,
            100,
            1,
        ))
        .unwrap();
    e.f.networks.promote_best("child").unwrap();
    e.evalc.on_new_best("child").unwrap();
    let _ = claim_and_report(&e, "rule:material_expectimax", 100, 30);

    assert_eq!(e.f.networks.best_sha().as_deref(), Some("parent"));
    assert_eq!(e.f.networks.status_of("child").as_deref(), Some("rejected"));
}

#[test]
fn rollback_aborts_if_best_replaced_during_eval() {
    let e = env(enabled_cfg());
    e.f.networks.seed_network("parent", None, "nnue");
    e.f.networks.seed_network("child", Some("parent"), "nnue");
    e.f.networks.seed_network("newer", Some("child"), "nnue");
    e.f.eval
        .upsert_eval_result(eval_row(
            "parent",
            "rule:material_expectimax",
            60,
            40,
            100,
            1,
        ))
        .unwrap();
    e.f.networks.promote_best("child").unwrap();
    e.evalc.on_new_best("child").unwrap();

    let claimed = claim_spec(&e, "rule:material_expectimax");
    e.f.networks.promote_best("newer").unwrap();
    e.evalc.report_result(&claimed.task_id, "", delta(100, 30));
    assert_eq!(e.f.networks.best_sha().as_deref(), Some("newer"));

    assert_eq!(e.f.networks.status_of("child").as_deref(), Some("archived"));
}

#[test]
fn target_stop_when_strongest_gauge_reaches_goal() {
    let raw = "[eval]\nenabled = true\ngames = 10\nstop_win_rate = 0.8\n";
    let e = env(raw);
    e.f.networks.seed_network("n1", None, "onnx");
    e.f.networks.promote_best("n1").unwrap();
    e.evalc.on_new_best("n1").unwrap();
    let _ = claim_and_report(&e, "rule:material_expectimax", 10, 9);
    let (stopped, reason) = e.ctl.should_stop();
    assert!(stopped);
    assert!(reason.contains("eval_target"), "{reason}");
}

#[test]
fn partial_result_does_not_degrade_full_result() {
    let e = env(enabled_cfg());
    e.f.networks.seed_network("n", None, "onnx");
    e.evalc.on_new_best("n").unwrap();
    let _ = claim_and_report(&e, "random", 100, 60);

    e.f.eval
        .upsert_eval_result(eval_row("n", "random", 10, 40, 50, 99))
        .unwrap();
    let r = e.f.eval.get_eval_result("n", "random").unwrap().unwrap();
    assert_eq!(r.num_games, 100);
}

#[test]
fn queue_restore_after_restart() {
    let e = env(enabled_cfg());
    e.f.networks.seed_network("n", None, "onnx");
    e.evalc.on_new_best("n").unwrap();
    let claimed = claim_spec(&e, "random");
    e.evalc.report_result(&claimed.task_id, "", delta(10, 6));

    let evalc2 = EvalCoordinator::new(
        &e.f.ports(),
        Arc::new(Config::parse(enabled_cfg(), "test").unwrap()),
        e.f.clock.clone(),
        e.ctl.clone(),
        e.disp.clone(),
    );
    assert_eq!(evalc2.pending_eval(), 4);

    let job = evalc2
        .running_jobs()
        .into_iter()
        .find(|j| j.opponent_spec == "random")
        .expect("random 作业应恢复");
    assert_eq!(job.num_games, 10);
    assert_eq!(job.target_games, 100);
}
