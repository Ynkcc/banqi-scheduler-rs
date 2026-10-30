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

#[test]
fn get_network_absent_returns_empty_url() {
    let (s, _) = default_server();
    let reply = s
        .get_network(NetworkRequest { sha: "nope".into() })
        .unwrap();
    assert_eq!(reply.sha, "nope");
    assert_eq!(reply.download_url, "");
    assert!(!reply.is_best);

    let reply = s
        .get_network(NetworkRequest { sha: String::new() })
        .unwrap();
    assert_eq!(reply.download_url, "");
}

#[test]
fn get_network_by_sha_and_best() {
    let (s, f) = default_server();
    f.networks.seed_network("abc", Some("parent"), "onnx");
    f.networks.promote_best("abc").unwrap();

    let by_sha = s.get_network(NetworkRequest { sha: "abc".into() }).unwrap();
    assert_eq!(by_sha.key, "networks/abc.onnx");
    assert_eq!(by_sha.download_url, "get:networks/abc.onnx");
    assert!(by_sha.is_best);
    assert_eq!(by_sha.created_at, 1);
    assert_eq!(by_sha.game_data, r#"{"parent_sha":"parent"}"#);

    let by_best = s
        .get_network(NetworkRequest { sha: String::new() })
        .unwrap();
    assert_eq!(by_best.sha, "abc");
}

#[test]
fn sign_network_upload_rejects_bad_sha_and_format() {
    let (s, _) = default_server();
    let req = |sha: &str, format: &str| SignNetworkUploadRequest {
        trainer_id: "t".into(),
        sha: sha.into(),
        content_length: 1,
        content_sha256: String::new(),
        format: format.into(),
    };
    let short = s.sign_network_upload(req("abc", "onnx")).unwrap();
    assert!(!short.accepted);
    assert!(short.message.contains("invalid_sha_len=3"));

    let bad_format = s
        .sign_network_upload(req(&"a".repeat(64), "safetensors"))
        .unwrap();
    assert!(!bad_format.accepted);
    assert!(bad_format.message.contains("unsupported_format"));

    let ok = s
        .sign_network_upload(req(&"a".repeat(64), "  NNUE "))
        .unwrap();
    assert!(ok.accepted);
    assert_eq!(
        ok.object_key,
        "networks/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.nnue"
    );
    assert_eq!(
        ok.upload_url,
        format!("put:{}", ok.object_key),
        "上传走预签名 PUT"
    );
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
fn submit_reanalysis_rejection_paths() {
    let (s, _) = default_server();
    let req = SubmitReanalysisRequest {
        requester: "trainer".into(),
        variant: "4x8".into(),
        mcts_sims: 0,
        positions: 5,
        payload: vec![1, 2, 3],
    };
    let r = s.submit_reanalysis(req.clone()).unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("未启用局面重搜"), "{}", r.message);

    let (s, _) = server_with("[reanalysis]\ninterval_tasks = 2\nmax_queue = 10\n");
    let r = s
        .submit_reanalysis(SubmitReanalysisRequest {
            variant: "4x4".into(),
            ..req.clone()
        })
        .unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("变体不一致"), "{}", r.message);

    let r = s
        .submit_reanalysis(SubmitReanalysisRequest {
            payload: vec![],
            ..req.clone()
        })
        .unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("载荷为空"), "{}", r.message);

    let r = s.submit_reanalysis(req).unwrap();
    assert!(r.accepted);
    assert_eq!(r.queued_positions, 5);
    assert_eq!(s.reana.pending(), (5, 1));
}

#[test]
fn submit_reanalysis_queue_full_rejects_without_dropping() {
    let (s, _) = server_with("[reanalysis]\ninterval_tasks = 1\nmax_queue = 1\n");
    let req = |p: u32| SubmitReanalysisRequest {
        requester: "trainer".into(),
        variant: "4x8".into(),
        mcts_sims: 0,
        positions: p,
        payload: vec![1],
    };
    assert!(s.submit_reanalysis(req(1)).unwrap().accepted);
    let r = s.submit_reanalysis(req(2)).unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("队列已满"), "{}", r.message);

    assert_eq!(s.reana.pending(), (1, 1));
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

fn register_selfplay_task(s: &Server, task_id: &str, worker: &str, sha: &str) {
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
    }
}

#[test]
fn report_episode_rejections() {
    let (s, _) = default_server();

    let r = s.report_episode(episode_meta("nope", "w1", "sha")).unwrap();
    assert!(!r.accepted);
    assert_eq!(r.message, "unknown_task_id:nope");

    register_selfplay_task(&s, "t1", "w1", "sha");
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
    register_selfplay_task(&s, "t1", "w1", "sha-a");
    let first = s.report_episode(episode_meta("t1", "w1", "sha-a")).unwrap();
    let second = s.report_episode(episode_meta("t1", "w1", "sha-a")).unwrap();
    assert!(second.accepted);
    assert_eq!(second.object_key, first.object_key, "重试必须复用原对象键");
    assert_eq!(second.message, "duplicate_replay");
    assert!(!second.upload_url.is_empty(), "重试返回重签 PUT URL");
}

#[test]
fn report_match_result_gate_rejections() {
    let (s, _) = default_server();
    let result = |task_id: &str, worker: &str, kind: i32| MatchResult {
        worker_id: worker.into(),
        task_id: task_id.into(),
        kind,
        games: 2,
        pair_ll: 1,
        ..Default::default()
    };

    let r = s.report_match_result(result("nope", "w1", 1)).unwrap();
    assert!(!r.accepted);
    assert_eq!(r.message, "unknown_task_id:nope");

    s.disp.register(
        "t1",
        RunningTask {
            worker_id: "w1".into(),
            ..RunningTask::new(bqs_proto::TaskKind::TaskSelfplay)
        },
    );
    let r = s.report_match_result(result("t1", "w2", 1)).unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("worker_mismatch"), "{}", r.message);

    let r = s
        .report_match_result(result("t1", "w1", TaskKind::TaskSelfplay as i32))
        .unwrap();
    assert!(!r.accepted);
    assert_eq!(r.message, "task_is_not_gating");
}

#[test]
fn report_match_result_completed_short_circuit() {
    let (s, _) = default_server();

    s.disp.register(
        "t1",
        RunningTask {
            worker_id: "w1".into(),
            match_id: 7,
            ..RunningTask::new(bqs_proto::TaskKind::TaskSelfplay)
        },
    );
    s.disp.mark_done("t1");
    let r = s
        .report_match_result(MatchResult {
            worker_id: "w1".into(),
            task_id: "t1".into(),
            kind: TaskKind::TaskSelfplay as i32,
            ..Default::default()
        })
        .unwrap();
    assert!(r.accepted);
    assert_eq!(r.message, "already_reported:t1");
}

#[test]
fn report_match_result_routes_to_gatekeeper() {
    let (s, f) = default_server();

    let match_id = f.matches.seed_match("cand", "best0", 10);
    s.disp.register(
        "t1",
        RunningTask {
            worker_id: "w1".into(),
            match_id,
            ..RunningTask::new(bqs_proto::TaskKind::TaskSelfplay)
        },
    );

    let r = s
        .report_match_result(MatchResult {
            worker_id: "w1".into(),
            task_id: "t1".into(),
            kind: TaskKind::TaskSelfplay as i32,
            games: 10,
            pair_dd: 5,
            ..Default::default()
        })
        .unwrap();
    assert!(r.accepted);
    assert!(!r.match_concluded);
    assert_eq!(
        f.matches.get_match(match_id).unwrap().unwrap().num_games,
        10
    );

    assert_eq!(s.disp.lookup("t1").unwrap().state, TaskState::Completed);
}

#[test]
fn report_match_result_match_not_running_is_business_rejection() {
    let (s, f) = default_server();

    let match_id = f.matches.seed_match("cand", "best0", 10);
    f.matches.force_conclude(match_id);
    s.disp.register(
        "t1",
        RunningTask {
            worker_id: "w1".into(),
            match_id,
            ..RunningTask::new(bqs_proto::TaskKind::TaskRating)
        },
    );
    let r = s
        .report_match_result(MatchResult {
            worker_id: "w1".into(),
            task_id: "t1".into(),
            kind: TaskKind::TaskRating as i32,
            games: 2,
            pair_ll: 1,
            ..Default::default()
        })
        .unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("not_running"), "{}", r.message);
    assert_eq!(s.disp.lookup("t1").unwrap().state, TaskState::Completed);
}

#[test]
fn report_match_result_eval_routes_to_eval_sink() {
    let (s, f) = server_with("[eval]\nenabled = true\ngames = 100\ngames_per_task = 10\n");
    f.networks.seed_network("best", None, "onnx");
    f.networks.promote_best("best").unwrap();
    s.evalc.ensure_best_evaluated();

    let (_, task_id) = loop {
        match s.evalc.claim_eval_task("w1", 4) {
            Some((job, id)) if job.opponent_spec == "random" => break (job, id),
            Some((_, id)) => s.evalc.release_eval_claim(&id),
            None => panic!("应有可认领的 eval 分片"),
        }
    };
    let r = s
        .report_match_result(MatchResult {
            worker_id: "w1".into(),
            task_id: task_id.clone(),
            kind: TaskKind::TaskEval as i32,
            games: 10,
            wins: 8,
            opponent_spec: "random".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(r.accepted, "{}", r.message);
    assert!(!r.match_concluded, "100 目标仅累计 10 局，不收尾");

    let dup = s
        .report_match_result(MatchResult {
            worker_id: "w1".into(),
            task_id: task_id.clone(),
            kind: TaskKind::TaskEval as i32,
            games: 10,
            ..Default::default()
        })
        .unwrap();
    assert!(dup.accepted);
    assert_eq!(dup.message, format!("already_reported:{task_id}"));
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
fn register_network_creates_match_against_best_snapshot() {
    let (s, f) = default_server();
    f.networks.seed_network("best-1", None, "onnx");
    f.networks.promote_best("best-1").unwrap();

    let r = s
        .register_network(RegisterNetworkRequest {
            sha: "best-1".into(),
            format: "onnx".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(!r.accepted);
    assert_eq!(r.message, "sha_equals_current_best");

    f.networks.seed_network("cand-1", None, "onnx");
    let r = s
        .register_network(RegisterNetworkRequest {
            sha: "cand-1".into(),
            format: "onnx".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(!r.accepted);
    assert_eq!(r.message, "duplicate_sha:cand-1");

    let r = s
        .register_network(RegisterNetworkRequest {
            sha: "cand-2".into(),
            format: "onnx".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(r.accepted);
    assert!(
        r.match_task_hint.contains("gatekeeper match"),
        "{}",
        r.match_task_hint
    );
    let matches = f.matches.list_matches().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].candidate, "cand-2");
    assert_eq!(matches[0].opponent, "best-1");
    assert_eq!(matches[0].target_games, 400);
}

#[test]
fn register_network_first_promotes_and_triggers_eval() {
    let (s, f) = server_with("[eval]\nenabled = true\n");
    let r = s
        .register_network(RegisterNetworkRequest {
            sha: "first".into(),
            format: "onnx".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(r.accepted);
    assert_eq!(r.message, "first_network_promoted");
    assert_eq!(f.networks.best_sha().as_deref(), Some("first"));

    assert_eq!(f.eval.load_eval_queue().unwrap().len(), 4);
}

#[test]
fn get_task_returns_none_when_paused() {
    let (s, _) = default_server();
    s.ctl.set_manual_paused(true).unwrap();
    let resp = s
        .get_task(TaskRequest {
            worker_id: "w1".into(),
            client_version: "1.0.0".into(),
            threads: 4,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(resp.kind, TaskKind::TaskNone as i32);
    assert_eq!(resp.message, "self_play_paused");
}

#[test]
fn get_task_auto_paused_stops_selfplay_but_not_reanalysis() {
    let (s, f) = server_with("[reanalysis]\ninterval_tasks = 1\nmax_queue = 10\n");
    f.networks.seed_network("best", None, "onnx");
    f.networks.promote_best("best").unwrap();
    s.ctl.set_auto_paused(true, "test");
    s.submit_reanalysis(SubmitReanalysisRequest {
        requester: "t".into(),
        variant: "4x8".into(),
        positions: 1,
        payload: vec![1],
        ..Default::default()
    })
    .unwrap();

    s.reana.note_selfplay_assigned();

    let resp = s
        .get_task(TaskRequest {
            worker_id: "w".into(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(resp.kind, TaskKind::TaskReanalysis as i32);

    let (s2, f2) = server_with(
        "[reanalysis]\ninterval_tasks = 1\nmax_queue = 10\n[balance]\nenabled = false\npause_reanalysis = true\n",
    );
    f2.networks.seed_network("best", None, "onnx");
    f2.networks.promote_best("best").unwrap();
    s2.ctl.set_auto_paused(true, "test");
    s2.reana.note_selfplay_assigned();
    s2.submit_reanalysis(SubmitReanalysisRequest {
        requester: "t".into(),
        variant: "4x8".into(),
        positions: 1,
        payload: vec![1],
        ..Default::default()
    })
    .unwrap();
    let resp = s2
        .get_task(TaskRequest {
            worker_id: "w".into(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(resp.kind, TaskKind::TaskNone as i32);
    assert!(
        resp.message.contains("self_play_auto_paused"),
        "{}",
        resp.message
    );
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
    assert_eq!(resp.kind, TaskKind::TaskNone as i32);
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
fn get_task_priority_chain() {
    let (s, f) = server_with("[reanalysis]\ninterval_tasks = 1\nmax_queue = 10\n");
    let req = TaskRequest {
        worker_id: "w1".into(),
        client_version: "1.0.0".into(),
        threads: 4,
        current_network: "best".into(),
        ..Default::default()
    };

    let none = s.get_task(req.clone()).unwrap();
    assert_eq!(none.kind, TaskKind::TaskNone as i32);
    assert_eq!(none.message, "no_best_network_registered");

    f.networks.seed_network("best", None, "onnx");
    f.networks.promote_best("best").unwrap();
    s.submit_reanalysis(SubmitReanalysisRequest {
        requester: "t".into(),
        variant: "4x8".into(),
        positions: 9,
        payload: vec![1],
        ..Default::default()
    })
    .unwrap();
    let resp = s.get_task(req.clone()).unwrap();
    assert_eq!(
        resp.kind,
        TaskKind::TaskSelfplay as i32,
        "节流未满足：先 selfplay"
    );
    assert_eq!(resp.network_sha, "best");
    assert_eq!(resp.network_key, "networks/best.onnx");
    assert_eq!(resp.network_url, "", "current_network == best 不签下载 URL");

    let resp = s.get_task(req.clone()).unwrap();
    assert_eq!(resp.kind, TaskKind::TaskReanalysis as i32);
    assert_eq!(resp.games, 9, "games = 位置条数");
    assert_eq!(s.reana.pending().1, 0);

    assert!(s.disp.lookup(&resp.task_id).is_some());

    s.request_feature_pack(FeaturePackRequest {
        requester: "t".into(),
        episode_object_key: "episodes/best/x.epb.gz".into(),
        pack_version: 1,
    })
    .unwrap();
    let resp = s.get_task(req.clone()).unwrap();
    assert_eq!(resp.kind, TaskKind::TaskDeriveFeatures as i32);
    assert_eq!(resp.episode_object_key, "episodes/best/x.epb.gz");

    let resp = s.get_task(req).unwrap();
    assert_eq!(resp.kind, TaskKind::TaskSelfplay as i32);
}

#[test]
fn get_task_eval_has_highest_priority() {
    let (s, f) = server_with(
        "[eval]\nenabled = true\ngames = 100\ngames_per_task = 10\n[reanalysis]\ninterval_tasks = 1\nmax_queue = 10\n",
    );
    f.networks.seed_network("best", None, "onnx");
    f.networks.promote_best("best").unwrap();
    s.evalc.ensure_best_evaluated();
    s.submit_reanalysis(SubmitReanalysisRequest {
        requester: "t".into(),
        variant: "4x8".into(),
        positions: 1,
        payload: vec![1],
        ..Default::default()
    })
    .unwrap();
    let resp = s
        .get_task(TaskRequest {
            worker_id: "w1".into(),
            threads: 4,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        resp.kind,
        TaskKind::TaskEval as i32,
        "eval 先于 reanalysis 与 selfplay"
    );
    assert_eq!(resp.network_sha, "best");
    assert_eq!(resp.opponent_spec, "random");
    assert_eq!(resp.games, 10);
}

#[test]
fn heartbeat_touches_worker_and_returns_best() {
    let (s, f) = default_server();
    f.networks.seed_network("best", None, "onnx");
    f.networks.promote_best("best").unwrap();
    let reply = s
        .heartbeat(HeartbeatRequest {
            worker_id: "w1".into(),
            current_threads: 8,
            completed_games: 42,
            client_version: "1.2.3".into(),
            memory_mb: 1024,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(reply.best_network, "best");
    let w = f.workers.get("w1").unwrap();
    assert_eq!(w.threads, 8);
    assert_eq!(w.completed_games, 42);
    assert_eq!(w.client_version, "1.2.3");

    s.ctl.set_manual_paused(true).unwrap();
    let reply = s
        .heartbeat(HeartbeatRequest {
            worker_id: "w1".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(reply.pause_self_play);
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

    s.ctl.set_stop("eval_no_progress: test").unwrap();
    let reply = s.get_info(GetInfoRequest {}).unwrap();
    assert!(reply.should_stop);
    assert!(reply.stop_reason.contains("eval_no_progress"));
}

#[test]
fn get_train_config_variant_gate() {
    let (s, _) = default_server();
    let r = s
        .get_train_config(GetTrainConfigRequest {
            variant: "4x4".into(),
            trainer_id: "t".into(),
        })
        .unwrap();
    assert!(!r.accepted);
    assert!(r.message.contains("变体不符"), "{}", r.message);
    let r = s
        .get_train_config(GetTrainConfigRequest {
            variant: "4x8".into(),
            trainer_id: "t".into(),
        })
        .unwrap();
    assert!(r.accepted);
    assert!(r.overrides.is_empty(), "无覆盖时下发空集");

    let mut m = std::collections::BTreeMap::new();
    m.insert("LEARNING_RATE".to_string(), "0.001".to_string());
    s.ctl.set_train_config_overrides(m).unwrap();
    let r = s
        .get_train_config(GetTrainConfigRequest {
            variant: "4x8".into(),
            trainer_id: "t".into(),
        })
        .unwrap();
    assert_eq!(
        r.overrides.get("LEARNING_RATE").map(|s| s.as_str()),
        Some("0.001")
    );
}

#[test]
fn trainer_heartbeat_registers_and_replies_pause() {
    let (s, f) = default_server();
    let r = s
        .trainer_heartbeat(TrainerHeartbeatRequest {
            trainer_id: "tr1".into(),
            variant: "4x8".into(),
            kind: 0,
            consumed_key: String::new(),
            samples_consumed: 10,
            backlog: 2,
            buffered: 3,
            health_status: "ok".into(),
            last_error: String::new(),
            progress_json: String::new(),
        })
        .unwrap();
    assert_eq!(r.message, "");
    let t = f.registry.get_trainer("tr1").unwrap().unwrap();
    assert_eq!(t.samples_consumed, 10);
    assert_eq!(t.backlog, 2);

    let r = s
        .trainer_heartbeat(TrainerHeartbeatRequest {
            variant: "4x4".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(r.message.contains("variant_mismatch"), "{}", r.message);
    assert!(f.registry.get_trainer("tr2").unwrap().is_none());
}

#[test]
fn trainer_heartbeat_watermark_mapping() {
    let (s, f) = default_server();
    f.episodes.seed_episode(7, "episodes/n/7.epb.gz", 0);
    let r = s
        .trainer_heartbeat(TrainerHeartbeatRequest {
            trainer_id: "tr1".into(),
            variant: "4x8".into(),
            kind: 0,
            consumed_key: "episodes/n/7.epb.gz".into(),
            samples_consumed: 20,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(r.message, "");
    assert_eq!(
        f.registry.get_trainer("tr1").unwrap().unwrap().consumed_id,
        7
    );
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
