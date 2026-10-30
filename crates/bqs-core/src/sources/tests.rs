use bqs_proto::{JobKind, RatingJobSpec, SubmitJobRequest, TaskKind, TaskRequest};

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
    let f = Fakes::new();
    let cfg = Arc::new(Config::parse(
        "[selfplay]\ngames_per_task = 8\n[sprt]\nelo0 = 0.0\nelo1 = 15.0\nalpha = 0.05\nbeta = 0.05\n",
        "test",
    )
    .unwrap());
    let ports = f.ports();
    let ctl = Arc::new(crate::control::Control::new(&ports, cfg.clone()));
    let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), f.clock.clone()));
    let jobs = Arc::new(JobManager::new(
        ports.jobs.clone(),
        disp.clone(),
        ports.networks.clone(),
        ports.matches.clone(),
        ctl.clone(),
        cfg.clone(),
        f.clock.clone(),
    ));
    let asm = Arc::new(TaskAssembler::new(
        ports.networks.clone(),
        f.signer.clone(),
        cfg.clone(),
        ctl.clone(),
    ));
    let src = JobSource {
        jobs: jobs.clone(),
        matches: ports.matches.clone(),
        nets: ports.networks.clone(),
        disp,
        reana: Arc::new(ReanaQueue::new(&ports, cfg.clone(), f.clock.clone())),
        asm,
        ctl,
        cfg,
    };
    Env { f, jobs, src }
}

fn submit_rating(env: &Env, total: u64, opponent: &str) -> (String, i64) {
    env.f.networks.seed_network("best", None, "onnx");
    env.f.networks.promote_best("best").unwrap();
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
    TaskRequest {
        worker_id: "w1".into(),
        threads,
        ..Default::default()
    }
}

#[test]
fn rating_dispatch_builds_task_and_books_job() {
    let e = env();
    let (job_id, match_id) = submit_rating(&e, 20, "");
    let resp = e
        .src
        .dispatch_rating(
            &e.jobs.dispatchable(JobKind::JobRating).unwrap().remove(0),
            &req(4),
        )
        .unwrap()
        .unwrap();
    assert_eq!(resp.kind, TaskKind::TaskRating as i32);
    assert_eq!(resp.network_sha, "cand");
    assert_eq!(resp.opponent_sha, "best", "对手空解析为当前 best");
    assert_eq!(
        resp.games, 8,
        "games_per_task=8 缩放（threads=4/baseline=4）"
    );
    assert_eq!(resp.network_key, "networks/cand.onnx");
    assert_eq!(
        resp.opponent_url, "get:networks/best.onnx",
        "对手 URL 恒签发"
    );

    let row = e.jobs.get(&job_id).unwrap().unwrap();
    assert_eq!(row.dispatched_games, 8);
    let t = e.src.disp.lookup(&resp.task_id).unwrap();
    assert_eq!(t.match_id, match_id);
    assert_eq!(t.job_id, job_id);
    assert!(e.jobs.rating_spec(&job_id).unwrap().is_some());
}

#[test]
fn rating_dispatch_clamps_to_remaining_games() {
    let e = env();
    let (job_id, _) = submit_rating(&e, 10, "");

    let resp = e
        .src
        .dispatch_rating(
            &e.jobs.dispatchable(JobKind::JobRating).unwrap().remove(0),
            &req(4),
        )
        .unwrap()
        .unwrap();
    assert_eq!(resp.games, 8);

    let resp2 = e
        .src
        .dispatch_rating(
            &e.jobs.dispatchable(JobKind::JobRating).unwrap().remove(0),
            &req(4),
        )
        .unwrap()
        .unwrap();
    assert_eq!(resp2.games, 2);

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
    let (job_id, match_id) = submit_rating(&e, 0, "");
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
    e.f.networks.seed_network("best", None, "onnx");
    e.f.networks.promote_best("best").unwrap();
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
                        labeler_mode: "best".into(),
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
    assert_eq!(resp.kind, TaskKind::TaskRating as i32);

    assert_eq!(resp.network_sha, "cand");
    assert_eq!(resp.network_key, "networks/cand.onnx");
    assert_eq!(resp.opponent_sha, "");
    assert_eq!(resp.opponent_key, "");
    assert_eq!(resp.opponent_url, "");

    let sa = resp.rating_side_a.unwrap();
    assert_eq!(sa.kind, "expectimax_nnue");
    assert_eq!(sa.network_sha, "cand");
    assert_eq!(sa.search.unwrap().expectimax_node_budget, 4000);
    let sb = resp.rating_side_b.unwrap();
    assert_eq!(sb.kind, "rule:capture_first");
    assert_eq!(sb.network_sha, "");
    assert_eq!(sb.search.unwrap().expectimax_node_budget, 1000);

    let t = e.src.disp.lookup(&resp.task_id).unwrap();
    assert_eq!(t.opponent_sha, "");
}

#[test]
fn selfplay_mix_skips_rating_match() {
    let e = env();
    let (_, rating_match) = submit_rating(&e, 0, "");
    assert!(e.f.matches.pending_match().unwrap().is_some());
    assert!(e.jobs.is_rating_match(rating_match));

    let pm = e.f.matches.pending_match().unwrap();
    let filtered = pm.filter(|m| !e.jobs.is_rating_match(m.id));
    assert!(filtered.is_none(), "rating match 应被过滤出混入查询");
}
