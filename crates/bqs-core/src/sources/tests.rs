use bqs_proto::{JobKind, RatingJobSpec, SubmitJobRequest, TaskRequest};

use super::*;
use crate::jobs::JobSpecJson;
use crate::test_support::Fakes;
use std::sync::Arc;

struct Env {
    f: Fakes,
    jobs: Arc<JobManager>,
    src: JobSource,
}

fn env() -> Env {
    env_with_jobs("[jobs]\ncollect_overdispatch_games = -1\n")
}

fn env_with_jobs(jobs_toml: &str) -> Env {
    let f = Fakes::new();
    let cfg = Arc::new(Config::parse(
        &format!(
            "[selfplay]\ngames_per_task = 8\n{jobs_toml}"
        ),
        "test",
    )
    .unwrap());
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
    let asm = Arc::new(TaskAssembler::new(f.signer.clone(), cfg.clone(), {
        let ports = f.ports();
        Arc::new(crate::control::Control::new(&ports, cfg.clone()))
    }));
    let src = JobSource {
        jobs: jobs.clone(),
        matches: ports.matches.clone(),
        nets: ports.networks.clone(),
        disp,
        asm,
        cfg,
    };
    Env { f, jobs, src }
}

fn submit_rating(env: &Env, total: u64, opponent: &str) -> (String, i64) {
    env.f.networks.seed_network("best", None, "onnx");
    env.f.networks.seed_network("cand", None, "onnx");
    env.f.networks.seed_network("fixed", None, "onnx");
    let (ok, msg, id) = env
        .jobs
        .submit(&SubmitJobRequest {
            requester: "test".into(),
            kind: JobKind::JobRating as i32,
            priority: 0,
            rating: Some(RatingJobSpec {
                candidate_sha: "cand".into(),
                opponent_sha: opponent.into(),
                total_games: total,
                sprt_elo0: 0.0,
                sprt_elo1: 15.0,
                sprt_alpha: 0.05,
                sprt_beta: 0.05,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
    assert!(ok, "{msg}");
    let row = env.jobs.get(&id).unwrap().unwrap();
    let Some(JobSpecJson::Rating(spec)) = JobSpecJson::parse(&row.spec_json) else {
        panic!("rating spec json");
    };
    (id, spec.match_id)
}

fn req(threads: i32) -> TaskRequest {
    req_as("w1", threads)
}

fn req_as(worker: &str, threads: i32) -> TaskRequest {
    TaskRequest {
        worker_id: worker.into(),
        threads,
        ..Default::default()
    }
}

#[test]
fn rating_dispatch_builds_task_and_books_job() {
    let e = env();
    let (job_id, match_id) = submit_rating(&e, 20, "best");
    let resp = e
        .src
        .dispatch_rating(
            &e.jobs.dispatchable(JobKind::JobRating).unwrap().remove(0),
            &req(4),
        )
        .unwrap()
        .unwrap();
    let g = resp.generation.clone().expect("generation");
    let p = match g.behavior.expect("behavior") {
        bqs_proto::generation_spec::Behavior::Plays(p) => p,
        _ => panic!("expected plays"),
    };
    assert_eq!(p.player_a.unwrap().network_sha, "cand");
    assert_eq!(resp.network_sha, "cand");
    assert_eq!(resp.opponent_sha, "best");
    assert_eq!(g.games, 8, "games_per_task=8 缩放（threads=4/baseline=4）");
    assert_eq!(resp.network_key, "networks/cand.onnx");
    assert_eq!(
        resp.opponent_url, "get:networks/best.onnx",
        "对手 URL 恒签发"
    );

    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.dispatched_games, 8);
    let t = e.src.disp.lookup(&resp.task_id).unwrap();
    assert_eq!(t.match_id, match_id);
    assert!(t.gating, "rating 任务登记表 gating 预计算为 true");
    assert_eq!(t.job_id, job_id);
    assert!(e.jobs.rating_spec(&job_id).unwrap().is_some());
}

#[test]
fn rating_dispatch_clamps_to_remaining_games() {
    let e = env();
    let (job_id, _) = submit_rating(&e, 10, "best");

    let resp = e
        .src
        .dispatch_rating(
            &e.jobs.dispatchable(JobKind::JobRating).unwrap().remove(0),
            &req(4),
        )
        .unwrap()
        .unwrap();
    let g = resp.generation.clone().unwrap();
    assert_eq!(g.games, 8);

    let resp2 = e
        .src
        .dispatch_rating(
            &e.jobs.dispatchable(JobKind::JobRating).unwrap().remove(0),
            &req(4),
        )
        .unwrap()
        .unwrap();
    assert_eq!(resp2.generation.unwrap().games, 2);

    let next = e
        .src
        .dispatch_rating(
            &e.jobs.dispatchable(JobKind::JobRating).unwrap().remove(0),
            &req(4),
        )
        .unwrap();
    assert!(next.is_none(), "剩余不足 2 局不应派发");
    let _ = job_id;
}

#[test]
fn rating_dispatch_skips_concluded_match() {
    let e = env();
    let (job_id, match_id) = submit_rating(&e, 0, "best");
    e.f.matches.conclude_match(match_id).unwrap();
    let out = e
        .src
        .dispatch_rating(
            &e.jobs.dispatchable(JobKind::JobRating).unwrap().remove(0),
            &req(4),
        )
        .unwrap();
    assert!(out.is_none(), "对打已结转不再派发");
    let _ = job_id;
}

#[test]
fn rating_dispatch_with_sides_and_non_network_opponent() {
    let e = env();
    e.f.networks.seed_network("cand", None, "onnx");
    let (ok, msg, _id) = e
        .jobs
        .submit(&SubmitJobRequest {
            requester: "test".into(),
            kind: JobKind::JobRating as i32,
            priority: 0,
            rating: Some(RatingJobSpec {
                side_a: Some(bqs_proto::CollectPlayerSpec {
                    kind: "expectimax_nnue".into(),
                    network_sha: "cand".into(),
                    search: Some(bqs_proto::CollectSearchParams {
                        expectimax_node_budget: 4000,
                        ..Default::default()
                    }),
                }),
                side_b: Some(bqs_proto::CollectPlayerSpec {
                    kind: "rule:capture_first".into(),
                    network_sha: String::new(),
                    search: Some(bqs_proto::CollectSearchParams {
                        expectimax_node_budget: 1000,
                        ..Default::default()
                    }),
                }),
                total_games: 20,
                sprt_elo0: 0.0,
                sprt_elo1: 15.0,
                sprt_alpha: 0.05,
                sprt_beta: 0.05,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
    assert!(ok, "{msg}");
    let resp = e
        .src
        .dispatch_rating(
            &e.jobs.dispatchable(JobKind::JobRating).unwrap().remove(0),
            &req(4),
        )
        .unwrap()
        .unwrap();
    let g = resp.generation.clone().expect("generation");
    let sides = match g.behavior.expect("behavior") {
        bqs_proto::generation_spec::Behavior::Plays(p) => p,
        _ => panic!("expected plays"),
    };
    assert_eq!(resp.network_sha, "cand");
    assert_eq!(resp.network_key, "networks/cand.onnx");
    assert_eq!(resp.opponent_sha, "");
    assert_eq!(resp.opponent_key, "");
    assert_eq!(resp.opponent_url, "");

    let sa = sides.player_a.unwrap();
    assert_eq!(sa.kind, "expectimax_nnue");
    assert_eq!(sa.network_sha, "cand");
    assert_eq!(sa.search.unwrap().expectimax_node_budget, 4000);
    let sb = sides.player_b.unwrap();
    assert_eq!(sb.kind, "rule:capture_first");
    assert_eq!(sb.network_sha, "");
    assert_eq!(sb.search.unwrap().expectimax_node_budget, 1000);

    let t = e.src.disp.lookup(&resp.task_id).unwrap();
    assert_eq!(t.opponent_sha, "");
}

fn plays_of(resp: &bqs_proto::TaskResponse) -> bqs_proto::Plays {
    let g = resp.generation.clone().expect("generation");
    match g.behavior.expect("behavior") {
        bqs_proto::generation_spec::Behavior::Plays(p) => p,
        _ => panic!("expected plays"),
    }
}

fn submit_collect(env: &Env, total: u64) -> String {
    env.f.networks.seed_network("n1", None, "onnx");
    let (ok, msg, id) = env
        .jobs
        .submit(&SubmitJobRequest {
            requester: "test".into(),
            kind: JobKind::JobCollect as i32,
            priority: 0,
            collect: Some(bqs_proto::CollectionJobSpec {
                variant: "4x8".into(),
                scenario: bqs_proto::OpeningScenario::ScenarioAllRevealed as i32,
                seed_strategy: Some(bqs_proto::SeedStrategy {
                    kind: bqs_proto::SeedStrategyKind::SeedIncremental as i32,
                    base_seed: 1000,
                    ..Default::default()
                }),
                player_a: Some(bqs_proto::CollectPlayerSpec {
                    kind: "mcts_resnet".into(),
                    network_sha: "n1".into(),
                    ..Default::default()
                }),
                player_b: Some(bqs_proto::CollectPlayerSpec {
                    kind: "random".into(),
                    ..Default::default()
                }),
                data_kind: bq_record::DataKind::DataResnet as i32,
                total_games: total,
                pairs_parallel: None,
                pair_seed_refresh: None,
                start_fens: Vec::new(),
            }),
            ..Default::default()
        })
        .unwrap();
    assert!(ok, "{msg}");
    id
}

#[test]
fn collect_dispatch_generation_snapshot() {
    let e = env();
    let job_id = submit_collect(&e, 100);
    let resp = e.src.try_task(&req(4)).unwrap().expect("collect task");
    let g = resp.generation.clone().expect("generation");
    assert_eq!(g.variant, "4x8");
    assert_eq!(
        g.scenario,
        bqs_proto::OpeningScenario::ScenarioAllRevealed as i32
    );
    assert_eq!(g.seed, Some(1000), "INCREMENTAL base_seed + seq=0");
    assert_eq!(
        g.games, 8,
        "collect_games_per_task 缺省回落 games_per_task 缩放"
    );
    assert_eq!(
        g.deadline_unix,
        e.src.jobs.now() as u64 + e.src.cfg.jobs.collect_deadline_seconds as u64,
        "collect deadline 换算为 unix 截止"
    );
    let p = plays_of(&resp);
    let pa = p.player_a.unwrap();
    let pb = p.player_b.unwrap();
    assert_eq!(pa.kind, "mcts_resnet");
    assert_eq!(pa.network_sha, "n1");
    assert_eq!(pb.kind, "random");

    let t = e.src.disp.lookup(&resp.task_id).unwrap();
    assert!(!t.gating, "collect 任务不进 gating 通道");
    let _ = job_id;
}

#[test]
fn collect_dispatch_clamps_to_remaining_games() {
    let e = env();
    let job_id = submit_collect(&e, 10);

    let job_row = || e.jobs.dispatchable(JobKind::JobCollect).unwrap().remove(0);

    let resp = e
        .src
        .dispatch_collect(&job_row(), &req_as("w1", 4))
        .unwrap()
        .unwrap();
    assert_eq!(
        resp.generation.clone().unwrap().games,
        8,
        "首批不受余量影响"
    );

    let resp = e
        .src
        .dispatch_collect(&job_row(), &req_as("w2", 4))
        .unwrap()
        .unwrap();
    assert_eq!(
        resp.generation.clone().unwrap().games,
        2,
        "collect 派发必须按 total_games 余量封顶"
    );

    assert!(
        e.src
            .dispatch_collect(&job_row(), &req_as("w3", 4))
            .unwrap()
            .is_none(),
        "余量 < 2 应停止派发 collect 任务"
    );
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.dispatched_games, 10, "dispatched 不得超出 total_games");
}

#[test]
fn collect_dispatch_spreads_across_workers() {
    let e = env();
    let _job_id = submit_collect(&e, 30);

    let job_row = || e.jobs.dispatchable(JobKind::JobCollect).unwrap().remove(0);

    let resp = e
        .src
        .dispatch_collect(&job_row(), &req_as("w1", 4))
        .unwrap()
        .unwrap();
    assert_eq!(resp.generation.clone().unwrap().games, 8);
    assert!(
        e.src
            .dispatch_collect(&job_row(), &req_as("w1", 4))
            .unwrap()
            .is_none(),
        "同一 worker 在飞上限内不得连拿同 job 任务"
    );

    let resp = e
        .src
        .dispatch_collect(&job_row(), &req_as("w2", 4))
        .unwrap()
        .unwrap();
    assert_eq!(resp.generation.unwrap().games, 8);
    let resp = e
        .src
        .dispatch_collect(&job_row(), &req_as("w3", 4))
        .unwrap()
        .unwrap();
    assert_eq!(resp.generation.unwrap().games, 8);
}

#[test]
fn collect_dispatch_released_after_expiry() {
    let e = env();
    let job_id = submit_collect(&e, 10);

    let job_row = || e.jobs.dispatchable(JobKind::JobCollect).unwrap().remove(0);

    let resp = e
        .src
        .dispatch_collect(&job_row(), &req_as("w1", 4))
        .unwrap()
        .unwrap();
    assert_eq!(resp.generation.clone().unwrap().games, 8);
    e.f.clock.advance(1800);
    e.src.disp.prune();
    assert_eq!(
        e.src.disp.lookup(&resp.task_id).unwrap().state,
        crate::dispatch::TaskState::Expired
    );

    let resp = e
        .src
        .dispatch_collect(&job_row(), &req_as("w2", 4))
        .unwrap()
        .unwrap();
    assert_eq!(
        resp.generation.unwrap().games,
        8,
        "超期任务应释放预算，慢/掉线 worker 由其他 worker 对冲重派"
    );
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert!(row.completed_games < 10, "尚未完成，job 不应终结");
    let _ = job_id;
}

#[test]
fn collect_dispatch_overdispatch_fills_idle_workers() {
    let e = env_with_jobs("");
    let job_id = submit_collect(&e, 10);

    let job_row = || e.jobs.dispatchable(JobKind::JobCollect).unwrap().remove(0);

    let resp = e
        .src
        .dispatch_collect(&job_row(), &req_as("w1", 4))
        .unwrap()
        .unwrap();
    assert_eq!(resp.generation.clone().unwrap().games, 8);

    let resp = e
        .src
        .dispatch_collect(&job_row(), &req_as("w2", 4))
        .unwrap()
        .unwrap();
    assert_eq!(resp.generation.clone().unwrap().games, 8);

    let resp = e
        .src
        .dispatch_collect(&job_row(), &req_as("w3", 4))
        .unwrap()
        .unwrap();
    assert_eq!(resp.generation.clone().unwrap().games, 2);

    assert!(
        e.src
            .dispatch_collect(&job_row(), &req_as("w4", 4))
            .unwrap()
            .is_none(),
        "超发有上界：completed+inflight 达 total+slack 后停发"
    );
    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(
        row.dispatched_games, 18,
        "在飞+已完成超出目标的量恰好 = 超发余量"
    );
    let _ = job_id;
}

#[test]
fn rating_dispatch_boundary_remaining_three_games() {
    let e = env();
    let (job_id, _) = submit_rating(&e, 10, "best");

    e.jobs
        .note_dispatch(
            "ghost-task",
            &job_id,
            7,
            "wx",
            bqs_proto::TaskKind::TaskRating,
            "",
            "",
            "",
            0,
            None,
        )
        .unwrap();
    let resp = e
        .src
        .dispatch_rating(
            &e.jobs.dispatchable(JobKind::JobRating).unwrap().remove(0),
            &req(4),
        )
        .unwrap();
    match resp {
        Some(r) => assert_eq!(
            r.generation.unwrap().games,
            2,
            "剩余 3 局（奇数）应成对化为 2"
        ),
        None => panic!("剩余 3 局应下发 2 局"),
    }
}
