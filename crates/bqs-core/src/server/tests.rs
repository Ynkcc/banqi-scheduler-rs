use bqs_judgment::Hexanomial;
use bqs_proto::*;

use crate::dispatch::{RunningTask, TaskState};
use crate::server::{Server, ServerDeps};

use super::*;

fn test_config(raw: &str) -> bqs_config::Config {
    bqs_config::Config::parse(raw, "test").expect("测试配置解析失败")
}

fn server_with(raw: &str) -> (Arc<Server>, crate::test_support::Fakes) {
    let f = crate::test_support::Fakes::new();
    let cfg = test_config(raw);
    let server = Server::assemble(ServerDeps {
        cfg,
        ports: f.ports(),
        signer: f.signer.clone(),
        clock: f.clock.clone(),
    });
    (server, f)
}

fn default_server() -> (Arc<Server>, crate::test_support::Fakes) {
    server_with("")
}

fn resp_generation(resp: &TaskResponse) -> GenerationSpec {
    resp.generation
        .clone()
        .expect("任务响应应携带 generation（无任务时为 None）")
}

fn resp_behavior(resp: &TaskResponse) -> generation_spec::Behavior {
    resp_generation(resp)
        .behavior
        .expect("generation 应携带 behavior")
}

#[test]
fn list_episodes_paging_and_kind_filter() {
    let (s, f) = default_server();
    f.episodes.seed_episode(1, "episodes/sha/a.epb.gz", 0);
    f.episodes.seed_episode(2, "episodes/sha/b.epb.gz", 1);
    f.episodes.seed_episode(3, "episodes/sha/c.epb.gz", 2);
    f.episodes.seed_episode(4, "episodes/sha/d.epb.gz", 0);

    let all = s
        .list_episodes(ListEpisodesRequest {
            limit: 10,
            after_key: String::new(),
            kind: None,
        })
        .unwrap();
    assert_eq!(all.objects.len(), 3);
    assert!(!all.has_more);

    assert_eq!(all.objects[0].object_key, "episodes/sha/a.epb.gz");
    assert_eq!(all.objects[2].object_key, "episodes/sha/d.epb.gz");
    assert!(all.objects[0].download_url.starts_with("get:"));

    let page2 = s
        .list_episodes(ListEpisodesRequest {
            limit: 10,
            after_key: "episodes/sha/b.epb.gz".into(),
            kind: None,
        })
        .unwrap();
    assert_eq!(page2.objects.len(), 1);
    assert_eq!(page2.objects[0].object_key, "episodes/sha/d.epb.gz");

    let resnet = s
        .list_episodes(ListEpisodesRequest {
            limit: 10,
            after_key: "episodes/sha/b.epb.gz".into(),
            kind: Some(0),
        })
        .unwrap();
    assert_eq!(resnet.objects.len(), 1);
    assert_eq!(resnet.objects[0].object_key, "episodes/sha/d.epb.gz");

    let more = s
        .list_episodes(ListEpisodesRequest {
            limit: 1,
            after_key: String::new(),
            kind: None,
        })
        .unwrap();
    assert_eq!(more.objects.len(), 1);
    assert!(more.has_more);
    let none = s
        .list_episodes(ListEpisodesRequest {
            limit: 0,
            after_key: String::new(),
            kind: None,
        })
        .unwrap();
    assert_eq!(none.objects.len(), 3);
    assert!(!none.has_more);
}

#[test]
fn request_feature_pack_get_or_enqueue() {
    let (s, f) = default_server();
    let req = |key: &str| FeaturePackRequest {
        requester: "trainer".into(),
        episode_object_key: key.into(),
        pack_version: 0,
    };
    let r1 = s
        .request_feature_pack(req("episodes/sha/x.epb.gz"))
        .unwrap();
    assert!(r1.accepted);
    assert!(!r1.ready);
    assert_eq!(r1.message, "queued_for_derivation");
    assert_eq!(s.derive.pending(), 1);

    let r = s
        .request_feature_pack(FeaturePackRequest {
            episode_object_key: String::new(),
            ..req("k")
        })
        .unwrap();
    assert!(!r.accepted);
    assert_eq!(r.message, "missing_episode_object_key");

    f.packs
        .insert_feature_pack(crate::ports::FeaturePackDraft {
            episode_object_key: "episodes/sha/y.epb.gz".into(),
            pack_version: 1,
            object_key: "episodes/sha/y.pack.v1.fpb.gz".into(),
            created_at: 0,
        })
        .unwrap();
    let r2 = s
        .request_feature_pack(req("episodes/sha/y.epb.gz"))
        .unwrap();
    assert!(r2.accepted);
    assert!(r2.ready);
    assert_eq!(r2.object_key, "episodes/sha/y.pack.v1.fpb.gz");
    assert_eq!(r2.download_url, "get:episodes/sha/y.pack.v1.fpb.gz");
}

#[test]
fn report_feature_pack_idempotent_and_signs_put() {
    let (s, f) = default_server();

    s.disp.register(
        "task-1",
        RunningTask::new(bqs_proto::TaskKind::TaskDeriveFeatures),
    );
    let meta = FeaturePackMeta {
        worker_id: "w".into(),
        task_id: "task-1".into(),
        episode_object_key: "episodes/sha/x.epb.gz".into(),
        pack_version: 2,
        content_length: 10,
        content_sha256: String::new(),
    };
    let ack = s.report_feature_pack(meta.clone()).unwrap();
    assert!(ack.accepted);
    assert_eq!(ack.object_key, "episodes/sha/x.pack.v2.fpb.gz");
    assert_eq!(ack.upload_url, "put:episodes/sha/x.pack.v2.fpb.gz");
    assert_eq!(f.packs.count(), 1);

    assert_eq!(s.disp.lookup("task-1").unwrap().state, TaskState::Completed);

    let ack2 = s.report_feature_pack(meta.clone()).unwrap();
    assert!(ack2.accepted);
    assert_eq!(ack2.object_key, ack.object_key);
    assert_eq!(f.packs.count(), 1);

    let bad = s
        .report_feature_pack(FeaturePackMeta {
            episode_object_key: String::new(),
            ..meta
        })
        .unwrap();
    assert!(!bad.accepted);
    assert_eq!(bad.message, "missing_episode_object_key");
}

fn register_task(s: &Server, task_id: &str, worker: &str, sha: &str) {
    s.disp.register(
        task_id,
        RunningTask {
            network_sha: sha.into(),
            worker_id: worker.into(),
            games: 4,
            ..RunningTask::new(bqs_proto::TaskKind::TaskSelfplay)
        },
    );
}

fn episode_meta(task_id: &str, worker: &str, sha: &str) -> EpisodeMeta {
    EpisodeMeta {
        worker_id: worker.into(),
        task_id: task_id.into(),
        game_count: 4,
        total_steps: 100,
        winner: 1,
        network_sha: sha.into(),
        timestamp: 0,
        content_length: 128,
        content_sha256: String::new(),
        kind: 0,
        red_wins: 2,
        black_wins: 1,
        draw_no_capture: 1,
        draw_truncation: 0,
        seed_base: Some(1234),
        pairs_parallel: Some(true),
        pair_seed_refresh: Some(true),
        games: Vec::new(),
    }
}

#[test]
fn report_episode_rejections() {
    let (s, _) = default_server();

    let r = s.report_episode(episode_meta("nope", "w1", "sha")).unwrap();
    assert!(!r.accepted);
    assert_eq!(r.message, "unknown_task_id:nope");

    register_task(&s, "t1", "w1", "sha");
    let r = s.report_episode(episode_meta("t1", "w2", "sha")).unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("worker_mismatch"), "{}", r.message);

    let r = s.report_episode(episode_meta("t1", "w1", "other")).unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("network_sha_mismatch"), "{}", r.message);
}

#[test]
fn report_episode_registers_and_signs_put() {
    let (s, f) = default_server();

    s.disp.register(
        "t1",
        RunningTask {
            network_sha: "sha-a".into(),
            worker_id: "w1".into(),
            opponent_sha: "opp".into(),
            opponent_spec: "rule:capture_first".into(),
            match_id: 3,
            games: 4,
            ..RunningTask::new(bqs_proto::TaskKind::TaskSelfplay)
        },
    );
    let ack = s.report_episode(episode_meta("t1", "w1", "sha-a")).unwrap();
    assert!(ack.accepted);
    assert!(ack.object_key.starts_with("episodes/sha-a/"));
    assert!(ack.object_key.ends_with(".epb.gz"));
    assert_eq!(ack.upload_url, format!("put:{}", ack.object_key));

    assert_eq!(s.disp.lookup("t1").unwrap().state, TaskState::Completed);

    let ep = f.episodes.by_task("t1").unwrap();
    assert_eq!(ep.game_count, 4);
    assert_eq!(ep.red_wins, 2);
    assert_eq!(ep.opponent_sha, "opp");
    assert_eq!(ep.opponent_spec, "rule:capture_first");
    assert_eq!(ep.match_id, 3);
}

#[test]
fn report_episode_idempotent_duplicate_replays_key() {
    let (s, _) = default_server();
    register_task(&s, "t1", "w1", "sha-a");
    let first = s.report_episode(episode_meta("t1", "w1", "sha-a")).unwrap();
    let second = s.report_episode(episode_meta("t1", "w1", "sha-a")).unwrap();
    assert!(second.accepted);
    assert_eq!(second.object_key, first.object_key, "重试必须复用原对象键");
    assert_eq!(second.message, "duplicate_replay");
    assert!(!second.upload_url.is_empty(), "重试返回重签 PUT URL");
}

#[test]
fn report_match_result_rejections_and_stats_ack() {
    let (s, _) = default_server();
    let result = |task_id: &str, worker: &str| MatchResult {
        worker_id: worker.into(),
        task_id: task_id.into(),
        games: 2,
        pair_ll: 1,
        ..Default::default()
    };

    let r = s.report_match_result(result("nope", "w1")).unwrap();
    assert!(!r.accepted);
    assert_eq!(r.message, "unknown_task_id:nope");

    s.disp.register(
        "t1",
        RunningTask {
            worker_id: "w1".into(),
            ..RunningTask::new(bqs_proto::TaskKind::TaskSelfplay)
        },
    );
    let r = s.report_match_result(result("t1", "w2")).unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("worker_mismatch"), "{}", r.message);

    let r = s.report_match_result(result("t1", "w1")).unwrap();
    assert!(r.accepted, "非 gating 统计按登记表收讫");
    assert_eq!(r.message, "non_gating_stats_ack");
}

#[test]
fn report_match_result_completed_short_circuit() {
    let (s, _) = default_server();

    s.disp.register(
        "t1",
        RunningTask {
            worker_id: "w1".into(),
            match_id: 7,
            gating: true,
            ..RunningTask::new(bqs_proto::TaskKind::TaskSelfplay)
        },
    );
    s.disp.mark_done("t1");
    let r = s
        .report_match_result(MatchResult {
            worker_id: "w1".into(),
            task_id: "t1".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(r.accepted);
    assert_eq!(r.message, "already_reported:t1");
}

#[test]
fn register_network_validations() {
    let (s, _) = default_server();
    let r = s
        .register_network(RegisterNetworkRequest {
            sha: "a".repeat(64),
            format: "ckpt".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("unsupported_format"), "{}", r.message);
}

#[test]
fn register_network_registers_without_best_or_match() {
    let (s, f) = default_server();

    let r = s
        .register_network(RegisterNetworkRequest {
            sha: "first".into(),
            format: "onnx".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(r.accepted);
    assert_eq!(r.message, "network_registered", "只登记，不晋级 best");
    assert!(r.match_task_hint.is_empty(), "不再创建 gatekeeper 对打");
    assert!(f.matches.list_matches().unwrap().is_empty());

    let dup = s
        .register_network(RegisterNetworkRequest {
            sha: "first".into(),
            format: "onnx".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(!dup.accepted);
    assert_eq!(dup.message, "duplicate_sha:first");
}

#[test]
fn get_task_no_sources_means_no_task_available() {
    let (s, _) = default_server();
    let resp = s
        .get_task(TaskRequest {
            worker_id: "w1".into(),
            client_version: "1.0.0".into(),
            threads: 4,
            ..Default::default()
        })
        .unwrap();
    assert!(resp.generation.is_none(), "无任务不应携带 generation");
    assert_eq!(resp.message, "no_task_available");
}

#[test]
fn get_task_client_version_gate() {
    let (s, _) = server_with("[server]\nmin_client_version = \"0.9.0\"\n");

    let resp = s
        .get_task(TaskRequest {
            worker_id: "w".into(),
            client_version: "0.10.0".into(),
            ..Default::default()
        })
        .unwrap();
    assert_ne!(resp.message, "client_version_too_old");

    let resp = s
        .get_task(TaskRequest {
            worker_id: "w".into(),
            client_version: "0.8.9".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(resp.generation.is_none());
    assert_eq!(resp.message, "client_version_too_old");

    let resp = s
        .get_task(TaskRequest {
            worker_id: "w".into(),
            ..Default::default()
        })
        .unwrap();
    assert_ne!(resp.message, "client_version_too_old");
}

#[test]
fn get_task_serves_collect_then_derive_chain() {
    let (s, f) = server_with("[jobs]\ncollect_overdispatch_games = -1\n");
    f.networks.seed_network("n1", None, "onnx");
    let reply = s
        .submit_job(SubmitJobRequest {
            requester: "t".into(),
            kind: JobKind::JobCollect as i32,
            priority: 0,
            collect: Some(CollectionJobSpec {
                variant: "4x8".into(),
                total_games: 8,
                player_a: Some(CollectPlayerSpec {
                    kind: "mcts_resnet".into(),
                    network_sha: "n1".into(),
                    ..Default::default()
                }),
                player_b: Some(CollectPlayerSpec {
                    kind: "random".into(),
                    ..Default::default()
                }),
                data_kind: DataKind::DataResnet as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
    assert!(reply.accepted, "{}", reply.message);

    let req = TaskRequest {
        worker_id: "w1".into(),
        threads: 4,
        ..Default::default()
    };
    let resp = s.get_task(req.clone()).unwrap();
    assert!(
        matches!(resp_behavior(&resp), generation_spec::Behavior::Plays(_)),
        "collect job 优先派发"
    );
    assert_eq!(resp.network_sha, "n1");

    s.request_feature_pack(FeaturePackRequest {
        requester: "t".into(),
        episode_object_key: "episodes/n1/x.epb.gz".into(),
        pack_version: 1,
    })
    .unwrap();
    let resp = s.get_task(req.clone()).unwrap();
    assert!(matches!(
        resp_behavior(&resp),
        generation_spec::Behavior::Derive(_)
    ));

    let resp = s.get_task(req).unwrap();
    assert!(
        resp.generation.is_none(),
        "collect 余量耗尽 + 队列空 → 无任务"
    );
}

#[test]
fn get_task_rating_job_dispatches() {
    let (s, f) = server_with("");
    f.networks.seed_network("best", None, "onnx");
    f.networks.seed_network("cand", None, "onnx");
    let reply = s
        .submit_job(SubmitJobRequest {
            requester: "t".into(),
            kind: JobKind::JobRating as i32,
            priority: 0,
            rating: Some(RatingJobSpec {
                candidate_sha: "cand".into(),
                opponent_sha: "best".into(),
                total_games: 8,
                sprt_elo0: 0.0,
                sprt_elo1: 15.0,
                sprt_alpha: 0.05,
                sprt_beta: 0.05,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
    assert!(reply.accepted, "{}", reply.message);
    let resp = s
        .get_task(TaskRequest {
            worker_id: "w1".into(),
            threads: 4,
            ..Default::default()
        })
        .unwrap();
    assert!(
        matches!(resp_behavior(&resp), generation_spec::Behavior::Plays(_)),
        "rating job 派发对打任务"
    );
    assert_eq!(resp.network_sha, "cand");
}

#[test]
fn submit_job_rejects_selfplay_kind() {
    let (s, _) = default_server();
    let reply = s
        .submit_job(SubmitJobRequest {
            requester: "t".into(),
            kind: JobKind::JobSelfplay as i32,
            selfplay: Some(SelfPlayJobSpec::default()),
            ..Default::default()
        })
        .unwrap();
    assert!(!reply.accepted);
    assert!(
        reply.message.contains("selfplay_removed"),
        "{}",
        reply.message
    );
}

#[test]
fn heartbeat_touches_worker() {
    let (s, f) = default_server();
    let reply = s
        .heartbeat(
            HeartbeatRequest {
                worker_id: "w1".into(),
                current_threads: 8,
                completed_games: 42,
                client_version: "1.2.3".into(),
                memory_mb: 1024,
                ..Default::default()
            },
            "192.168.1.10",
        )
        .unwrap();
    assert!(
        !reply.pause_self_play,
        "暂停语义已移除，恒为 false"
    );
    assert!(reply.best_network.is_empty(), "best 语义已移除");
    let w = f.workers.get("w1").unwrap();
    assert_eq!(w.threads, 8);
    assert_eq!(w.completed_games, 42);
    assert_eq!(w.client_version, "1.2.3");
    assert_eq!(w.ip, "192.168.1.10");
}

#[test]
fn get_info_returns_variant_meta_and_stop_signal() {
    let (s, _) = default_server();
    let reply = s.get_info(GetInfoRequest {}).unwrap();
    assert_eq!(reply.variant, "4x8");
    let meta = reply.variant_meta.expect("variants.json 内嵌元信息");
    assert_eq!(meta.id, "4x8");
    assert_eq!(meta.piece_counts.len(), 7);
    assert!(!reply.should_stop);

    s.ctl.set_stop("test_stop_reason").unwrap();
    let reply = s.get_info(GetInfoRequest {}).unwrap();
    assert!(reply.should_stop);
    assert!(reply.stop_reason.contains("test_stop_reason"));
}

#[test]
fn hexanomial_bucket_order_matches_proto_pair_fields() {
    let req = MatchResult {
        pair_ll: 1,
        pair_ld: 2,
        pair_dd: 3,
        pair_wl: 4,
        pair_dw: 5,
        pair_ww: 6,
        ..Default::default()
    };
    let h = Hexanomial([
        req.pair_ll as i64,
        req.pair_ld as i64,
        req.pair_dd as i64,
        req.pair_wl as i64,
        req.pair_dw as i64,
        req.pair_ww as i64,
    ]);
    assert_eq!(h.0, [1, 2, 3, 4, 5, 6]);
    assert_eq!(h.wins(), 4 + 5 + 2 * 6);
}
