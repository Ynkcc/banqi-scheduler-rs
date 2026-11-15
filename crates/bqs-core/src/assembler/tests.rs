use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::TaskRequest;

use crate::{
    control::Control,
    featurepack::PendingDerive,
    test_support::{FakeSigner, Fakes},
};

use super::*;

fn gen_spec(resp: &bqs_proto::TaskResponse) -> bqs_proto::GenerationSpec {
    resp.generation.clone().expect("generation 缺省")
}

fn assembler(f: &Fakes) -> TaskAssembler {
    let cfg = Arc::new(Config::parse("", "test").unwrap());
    let ports = f.ports();
    let ctl = Arc::new(Control::new(&ports, cfg.clone()));
    TaskAssembler::new(Arc::new(FakeSigner), cfg, ctl)
}

fn valid_4x8_fen() -> String {
    let cfg = bq_env::Variant::DarkChess4x8.config();
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
        cfg.variant.as_str(),
        bag(bq_env::Player::Red),
        bag(bq_env::Player::Black)
    )
}

#[test]
fn games_for_threads_scaling() {
    assert_eq!(games_for_threads(4, 16, 8), 32, "快设备多打");
    assert_eq!(games_for_threads(4, 16, 2), 8, "慢设备少打");
    assert_eq!(games_for_threads(4, 16, 0), 4, "threads<=0 视为 1");
    assert_eq!(games_for_threads(4, 16, 1), 4);
    assert_eq!(games_for_threads(0, 16, 100), 16, "baseline<=0 不缩放");
    assert_eq!(games_for_threads(-1, 16, 100), 16, "baseline<=0 不缩放");
    assert_eq!(games_for_threads(100, 16, 4), 1, "最小 1");
    assert_eq!(games_for_threads(3, 16, 2), 10);
}

#[test]
fn data_kind_for_format_mapping() {
    assert_eq!(
        data_kind_for_format("onnx"),
        bq_record::DataKind::DataResnet as i32
    );
    assert_eq!(
        data_kind_for_format("nnue"),
        bq_record::DataKind::DataNnue as i32
    );
    assert_eq!(
        data_kind_for_format("whatever"),
        bq_record::DataKind::DataResnet as i32
    );
}

#[test]
fn derive_task_assembly() {
    let f = Fakes::new();
    let asm = assembler(&f);
    let item = PendingDerive {
        episode_object_key: "episodes/sha/x.epb.gz".into(),
        pack_version: 1,
        requester: "trainer".into(),
        created_at: 0,
    };
    let resp = asm.derive_task("t1", &item).unwrap();
    let g = gen_spec(&resp);
    if let Some(bqs_proto::generation_spec::Behavior::Derive(d)) = g.behavior {
        assert_eq!(d.episode_object_key, "episodes/sha/x.epb.gz");
        assert_eq!(
            d.episode_url, "get:episodes/sha/x.epb.gz",
            "记录批次下载 URL 恒签发"
        );
    } else {
        panic!("expected derive behavior");
    }
    assert_eq!(g.variant, "4x8");
}

#[test]
fn rating_task_assembly() {
    let f = Fakes::new();
    f.networks.seed_network("cand", None, "onnx");
    f.networks.seed_network("opp", None, "nnue");
    let asm = assembler(&f);
    let cand = f.networks.get_network("cand").unwrap().unwrap();
    let opp = f.networks.get_network("opp").unwrap().unwrap();

    let req = TaskRequest {
        current_network: "cand".into(),
        threads: 4,
        ..Default::default()
    };
    let side_a = bqs_proto::CollectPlayerSpec {
        kind: "expectimax_nnue".into(),
        network_sha: "cand".into(),
        search_json: String::new(),
    };
    let side_b = bqs_proto::CollectPlayerSpec {
        kind: "mcts_resnet".into(),
        network_sha: "opp".into(),
        search_json: String::new(),
    };
    let fens = vec![valid_4x8_fen()];
    let resp = asm
        .rating_task(
            "t1",
            "4x8",
            &side_a,
            &side_b,
            Some(&cand),
            Some(&opp),
            8,
            0x1234_5678_9abc_def0,
            fens.clone(),
            r#"{"kind":"all_revealed"}"#.into(),
            &req,
        )
        .unwrap();
    let g = gen_spec(&resp);
    assert_eq!(g.variant, "4x8");
    assert_eq!(g.games, 8);
    assert_eq!(g.start_fens, fens, "rating 任务须携带 start_fens");
    assert_eq!(
        g.scenario_json, r#"{"kind":"all_revealed"}"#,
        "rating 任务须携带场景 JSON 透传"
    );
    assert_eq!(
        g.data_kind,
        bq_record::DataKind::DataResnet as i32,
        "data_kind 跟随候选架构"
    );
    let p = match g.behavior.expect("behavior 缺省") {
        bqs_proto::generation_spec::Behavior::Plays(p) => p,
        _ => panic!("expected plays behavior"),
    };
    assert_eq!(p.player_a.unwrap().network_sha, "cand");
    assert_eq!(p.player_b.unwrap().network_sha, "opp");
    assert_eq!(resp.network_sha, "cand");
    assert_eq!(resp.network_key, "networks/cand.onnx");
    assert_eq!(resp.network_url, "", "本地已有主网络不签 URL");
    assert_eq!(resp.opponent_sha, "opp");
    assert_eq!(resp.opponent_key, "networks/opp.nnue");
    assert_eq!(
        resp.opponent_url, "get:networks/opp.nnue",
        "对手 URL 恒签发"
    );

    let req2 = TaskRequest {
        current_network: String::new(),
        ..Default::default()
    };
    let resp2 = asm
        .rating_task(
            "t2",
            "4x8",
            &side_a,
            &side_b,
            Some(&cand),
            None,
            4,
            42,
            Vec::new(),
            String::new(),
            &req2,
        )
        .unwrap();
    assert_eq!(resp2.network_url, "get:networks/cand.onnx");
    assert_eq!(resp2.opponent_key, "", "无对手时不签对手键");
}

#[test]
fn extra_config_json_semantics() {
    let f = Fakes::new();
    let cfg = Arc::new(Config::parse("[selfplay]\ninitial_revealed = 3\n", "test").unwrap());
    let ports = f.ports();
    let ctl = Arc::new(Control::new(&ports, cfg.clone()));
    let asm = TaskAssembler::new(Arc::new(FakeSigner), cfg, ctl);
    assert_eq!(
        asm.extra_config(),
        r#"{"initial_revealed_pieces":3}"#,
        "initial_revealed > 0 时写入 extra_config"
    );

    let f2 = Fakes::new();
    let cfg2 = Arc::new(Config::parse("", "test").unwrap());
    let ports2 = f2.ports();
    let ctl2 = Arc::new(Control::new(&ports2, cfg2.clone()));
    let asm2 = TaskAssembler::new(Arc::new(FakeSigner), cfg2, ctl2);
    assert_eq!(asm2.extra_config(), "", "无有效字段时为空串");
}

#[test]
fn collect_task_passthrough_pair_overrides() {
    let f = Fakes::new();
    let asm = assembler(&f);

    let spec = |pairs_parallel: Option<bool>, pair_seed_refresh: Option<bool>| {
        bqs_proto::CollectionJobSpec {
            variant: "4x8".into(),
            scenario_json: String::new(),
            data_kind: bq_record::DataKind::DataResnet as i32,
            total_games: 4,
            pairs_parallel,
            pair_seed_refresh,
            ..Default::default()
        }
    };

    let req = TaskRequest {
        threads: 4,
        ..Default::default()
    };

    let resp = asm
        .collect_task(
            "t1",
            &req,
            &spec(Some(false), Some(false)),
            2,
            42,
            1000,
            None,
            None,
        )
        .unwrap();
    let g = gen_spec(&resp);
    assert_eq!(g.pairs_parallel, Some(false), "pairs_parallel 应透传");
    assert_eq!(g.pair_seed_refresh, Some(false), "pair_seed_refresh 应透传");

    let resp = asm
        .collect_task("t2", &req, &spec(None, None), 2, 42, 1000, None, None)
        .unwrap();
    let g = gen_spec(&resp);
    assert_eq!(g.pairs_parallel, None, "None 不应覆写");
    assert_eq!(g.pair_seed_refresh, None, "None 不应覆写");
}

#[test]
fn collect_task_fills_network_keys() {
    let f = Fakes::new();
    f.networks
        .register_network("best-nnue", None, None, "nnue")
        .unwrap();
    let asm = assembler(&f);
    let best = f.networks.get_network("best-nnue").unwrap().unwrap();
    let req = TaskRequest {
        threads: 4,
        ..Default::default()
    };
    let player = |sha: &str| bqs_proto::CollectPlayerSpec {
        kind: "expectimax_nnue".into(),
        network_sha: sha.into(),
        ..Default::default()
    };
    let spec = bqs_proto::CollectionJobSpec {
        variant: "4x8".into(),
        data_kind: bq_record::DataKind::DataNnue as i32,
        total_games: 4,
        player_a: Some(player("best-nnue")),
        player_b: Some(player("best-nnue")),
        ..Default::default()
    };

    let resp = asm
        .collect_task("t1", &req, &spec, 2, 42, 1000, Some(&best), Some(&best))
        .unwrap();
    assert_eq!(resp.network_sha, "best-nnue");
    assert_eq!(
        resp.network_key, "networks/best-nnue.nnue",
        "collect 任务必须下发主网络对象键"
    );
    assert_eq!(
        resp.opponent_key, "networks/best-nnue.nnue",
        "collect 任务必须下发对手网络对象键"
    );
    assert_eq!(
        resp.network_url, "get:networks/best-nnue.nnue",
        "worker 未持有时应签下载 URL"
    );
}

#[test]
fn collect_task_fills_chance_outcome_depth_default_and_explicit_zero() {
    let f = Fakes::new();
    let asm = assembler(&f);
    let req = TaskRequest {
        threads: 4,
        ..Default::default()
    };
    let player_search_of = |json: &str| -> serde_json::Value {
        let parsed = crate::jobs::JobSpecJson::parse(json).expect("collect spec json 应可解析");
        let crate::jobs::JobSpecJson::Collect(spec) = parsed else {
            panic!("应为 collect spec");
        };
        let resp = asm
            .collect_task("t", &req, &spec.to_proto(), 2, 42, 1000, None, None)
            .unwrap();
        let g = gen_spec(&resp);
        match g.behavior.expect("behavior 缺省") {
            bqs_proto::generation_spec::Behavior::Plays(p) => {
                let raw = p.player_a.expect("player_a 缺省").search_json;
                assert!(!raw.is_empty(), "组装后 search_json 必为 canonical JSON");
                serde_json::from_str(&raw).expect("search_json 应为合法 JSON")
            }
            _ => panic!("expected plays behavior"),
        }
    };

    let no_k = r#"{"collect":{"variant":"4x8","totalGames":2,
        "playerA":{"kind":"expectimax_nnue","networkSha":"sha",
        "searchJson":"{\"expectimax\":{\"deterministic_depth\":8}}"}}}"#;
    let search = player_search_of(no_k);
    assert_eq!(
        search["expectimax"]["deterministic_depth"], 8,
        "显式 deterministic_depth 保留"
    );
    assert_eq!(
        search["expectimax"]["chance_outcome_depth"], 0,
        "search_json 只写部分字段时 k 应补调度器默认值 0"
    );

    let explicit_zero = r#"{"collect":{"variant":"4x8","totalGames":2,
        "playerA":{"kind":"expectimax_nnue","networkSha":"sha",
        "searchJson":"{\"expectimax\":{\"deterministic_depth\":8,\"chance_outcome_depth\":0}}"}}}"#;
    let search = player_search_of(explicit_zero);
    assert_eq!(
        search["expectimax"]["chance_outcome_depth"], 0,
        "显式 chance_outcome_depth=0 应被尊重（与缺失同值但语义为显式透传）"
    );

    let full_default = r#"{"collect":{"variant":"4x8","totalGames":2,
        "playerA":{"kind":"expectimax_nnue","networkSha":"sha"}}}"#;
    let search = player_search_of(full_default);
    assert_eq!(
        search["expectimax"]["deterministic_depth"], 8,
        "search_json 缺省时 expectimax 域应填配置默认"
    );
    assert_eq!(
        search["expectimax"]["fullwidth"], true,
        "缺失字段补配置默认（调度器采集默认 fullwidth=true，SelfplaySection::default 覆盖 core schema false）"
    );
    assert!(
        search.get("mcts").is_none(),
        "expectimax_nnue 裁剪后不得携带 mcts 域（collector 跨域 fail-fast）"
    );
}

#[test]
fn search_json_domain_merge() {
    let f = Fakes::new();
    let asm = assembler(&f);

    let merged = asm
        .player_with_search(bqs_proto::CollectPlayerSpec {
            kind: "mcts_resnet".into(),
            network_sha: "sha".into(),
            search_json: r#"{"mcts":{"sims":128}}"#.into(),
        })
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&merged.search_json).unwrap();
    assert_eq!(v["mcts"]["sims"], 128, "job 显式字段优先");
    assert_eq!(v["mcts"]["max_considered_actions"], 16, "缺失字段补默认");
    assert!(
        v.get("expectimax").is_none(),
        "mcts_resnet 裁剪后不得携带 expectimax 域（collector 跨域 fail-fast）"
    );

    let merged = asm
        .player_with_search(bqs_proto::CollectPlayerSpec {
            kind: "expectimax_nnue".into(),
            network_sha: "sha".into(),
            search_json: r#"{"expectimax":{"chance_outcome_depth":2}}"#.into(),
        })
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&merged.search_json).unwrap();
    assert_eq!(v["expectimax"]["chance_outcome_depth"], 2);
    assert_eq!(
        v["expectimax"]["deterministic_depth"], 8,
        "同域缺失字段补默认"
    );
    assert!(
        v.get("mcts").is_none(),
        "expectimax_nnue 裁剪后不得携带 mcts 域"
    );

    let merged = asm
        .player_with_search(bqs_proto::CollectPlayerSpec {
            kind: "random".into(),
            network_sha: String::new(),
            search_json: String::new(),
        })
        .unwrap();
    assert_eq!(
        merged.search_json, "",
        "NoSearch kind 的 search_json 应为空串"
    );

    let merged = asm
        .player_with_search(bqs_proto::CollectPlayerSpec {
            kind: "mcts_resnet".into(),
            network_sha: "sha".into(),
            search_json: String::new(),
        })
        .unwrap();
    let d =
        crate::search_json::canonicalize("", &bqs_config::SearchDefaults::default(), "mcts_resnet")
            .unwrap();
    assert_eq!(
        merged.search_json, d,
        "空 search_json 应产出与配置默认一致的单域 canonical JSON"
    );

    let err = asm
        .player_with_search(bqs_proto::CollectPlayerSpec {
            kind: "mcts_resnet".into(),
            network_sha: "sha".into(),
            search_json: r#"{"mcts":{"nope":1}}"#.into(),
        })
        .unwrap_err();
    assert!(err.to_string().contains("search_json"), "{err}");
}

#[test]
fn collect_task_player_search_json_is_canonical_single_domain() {
    let f = Fakes::new();
    let asm = assembler(&f);
    let req = TaskRequest {
        threads: 4,
        ..Default::default()
    };
    let spec = bqs_proto::CollectionJobSpec {
        variant: "4x8".into(),
        data_kind: bq_record::DataKind::DataNnue as i32,
        total_games: 2,
        player_a: Some(bqs_proto::CollectPlayerSpec {
            kind: "expectimax_nnue".into(),
            network_sha: "sha".into(),
            search_json: r#"{"expectimax":{"deterministic_depth":6}}"#.into(),
        }),
        ..Default::default()
    };
    let resp = asm
        .collect_task("t", &req, &spec, 2, 42, 1000, None, None)
        .unwrap();
    let g = gen_spec(&resp);
    let player_a = match g.behavior.expect("behavior 缺省") {
        bqs_proto::generation_spec::Behavior::Plays(p) => p.player_a.expect("player_a 缺省"),
        _ => panic!("expected plays behavior"),
    };
    let v: serde_json::Value = serde_json::from_str(&player_a.search_json).unwrap();
    assert_eq!(v["expectimax"]["deterministic_depth"], 6, "显式字段保留");
    assert_eq!(
        v["expectimax"]["chance_outcome_depth"], 0,
        "k 由组装保证必填"
    );
    assert!(v.get("mcts").is_none(), "组装按 kind 裁剪为单域");
}
