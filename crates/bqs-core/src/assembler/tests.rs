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
        search: None,
    };
    let side_b = bqs_proto::CollectPlayerSpec {
        kind: "mcts_resnet".into(),
        network_sha: "opp".into(),
        search: None,
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
            bqs_proto::OpeningScenario::ScenarioAllRevealed as i32,
            &req,
        )
        .unwrap();
    let g = gen_spec(&resp);
    assert_eq!(g.variant, "4x8");
    assert_eq!(g.games, 8);
    assert_eq!(g.start_fens, fens, "rating 任务须携带 start_fens");
    assert_eq!(
        g.scenario,
        bqs_proto::OpeningScenario::ScenarioAllRevealed as i32,
        "rating 任务须携带 scenario"
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
            0,
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
            scenario: 0,
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
