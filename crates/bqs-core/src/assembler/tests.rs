use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{TaskKind, TaskRequest};

use crate::{
    consts,
    control::Control,
    featurepack::PendingDerive,
    reanalysis::PendingReanalysis,
    test_support::{FakeSigner, Fakes},
    types::{Match, Network},
};

use super::*;

fn net(sha: &str, format: &str) -> Network {
    Network {
        sha: sha.into(),
        format: format.into(),
        created_at: 0,
        is_best: false,
        status: "candidate".into(),
        parent_sha: None,
        notes: None,
    }
}

fn matcher(id: i64, opponent: &str) -> Match {
    Match {
        id,
        candidate: "cand".into(),
        opponent: opponent.into(),
        status: "running".into(),
        pairs: bqs_judgment::Hexanomial::ZERO,
        num_games: 0,
        target_games: 400,
    }
}

fn assembler(f: &Fakes) -> TaskAssembler {
    let cfg = Arc::new(Config::parse("", "test").unwrap());
    let ports = f.ports();
    let ctl = Arc::new(Control::new(&ports, cfg.clone()));
    TaskAssembler::new(f.networks.clone(), Arc::new(FakeSigner), cfg, ctl)
}

#[test]
fn select_producer_table() {
    let best_onnx = net("best-onnx", "onnx");
    let best_nnue = net("best-nnue", "nnue");
    let cand_nnue = net("cand-nnue", "nnue");
    let alt_nnue = net("alt-nnue", "nnue");
    let resnet = bqs_proto::DataKind::DataResnet as i32;
    let nnue = bqs_proto::DataKind::DataNnue as i32;

    struct Case {
        name: &'static str,
        best: Option<Network>,
        cand: Option<Network>,
        alt: Option<Network>,
        pm: Option<Match>,
        data_kind: i32,
        want_mode: SelfplayMode,
        want_sha: &'static str,
        want_nil: bool,
        want_message: &'static str,
    }
    let cases = vec![
        Case {
            name: "无best网络",
            best: None,
            cand: None,
            alt: None,
            pm: None,
            data_kind: resnet,
            want_mode: SelfplayMode::None,
            want_sha: "",
            want_nil: true,
            want_message: "no_best_network_registered",
        },
        Case {
            name: "混入模式_候选产数据",
            best: Some(best_onnx.clone()),
            cand: Some(cand_nnue.clone()),
            alt: None,
            pm: Some(matcher(7, "opp")),
            data_kind: resnet,
            want_mode: SelfplayMode::Mix,
            want_sha: "cand-nnue",
            want_nil: false,
            want_message: "",
        },
        Case {
            name: "混入模式_候选记录缺失回退纯自对弈_架构相符",
            best: Some(best_nnue.clone()),
            cand: None,
            alt: None,
            pm: Some(matcher(7, "opp")),
            data_kind: nnue,
            want_mode: SelfplayMode::Pure,
            want_sha: "best-nnue",
            want_nil: false,
            want_message: "",
        },
        Case {
            name: "混入模式_候选缺失且回退后架构不符_仍用best回退",
            best: Some(best_onnx.clone()),
            cand: None,
            alt: Some(alt_nnue.clone()),
            pm: Some(matcher(7, "opp")),
            data_kind: nnue,
            want_mode: SelfplayMode::Pure,
            want_sha: "best-onnx",
            want_nil: false,
            want_message: "",
        },
        Case {
            name: "纯自对弈_best架构相符",
            best: Some(best_onnx.clone()),
            cand: None,
            alt: None,
            pm: None,
            data_kind: resnet,
            want_mode: SelfplayMode::Pure,
            want_sha: "best-onnx",
            want_nil: false,
            want_message: "",
        },
        Case {
            name: "纯自对弈_架构不符改用最新同格式网络",
            best: Some(best_onnx.clone()),
            cand: None,
            alt: Some(alt_nnue.clone()),
            pm: None,
            data_kind: nnue,
            want_mode: SelfplayMode::Pure,
            want_sha: "alt-nnue",
            want_nil: false,
            want_message: "",
        },
        Case {
            name: "纯自对弈_架构不符且无同格式网络",
            best: Some(best_onnx.clone()),
            cand: None,
            alt: None,
            pm: None,
            data_kind: nnue,
            want_mode: SelfplayMode::None,
            want_sha: "",
            want_nil: true,
            want_message: "no_nnue_network_for_data_kind",
        },
        Case {
            name: "纯自对弈_nnue_best架构相符",
            best: Some(best_nnue.clone()),
            cand: None,
            alt: None,
            pm: None,
            data_kind: nnue,
            want_mode: SelfplayMode::Pure,
            want_sha: "best-nnue",
            want_nil: false,
            want_message: "",
        },
        Case {
            name: "混入优先级高于架构换道",
            best: Some(best_onnx.clone()),
            cand: Some(cand_nnue.clone()),
            alt: Some(alt_nnue.clone()),
            pm: Some(matcher(7, "opp")),
            data_kind: resnet,
            want_mode: SelfplayMode::Mix,
            want_sha: "cand-nnue",
            want_nil: false,
            want_message: "",
        },
    ];
    for c in cases {
        let got = select_producer(
            c.best.as_ref(),
            c.cand.as_ref(),
            c.alt.as_ref(),
            c.pm.as_ref(),
            c.data_kind,
        )
        .unwrap_or_else(|e| panic!("case {}: {e}", c.name));
        assert_eq!(got.mode, c.want_mode, "case {}: mode", c.name);
        if c.want_nil {
            assert!(
                got.producer.is_none(),
                "case {}: producer 应为 None",
                c.name
            );
            assert_eq!(got.none_message, c.want_message, "case {}: message", c.name);
        } else {
            assert_eq!(
                got.producer.as_ref().unwrap().sha,
                c.want_sha,
                "case {}: sha",
                c.name
            );
        }
        if c.want_mode == SelfplayMode::Mix {
            assert!(got.m.is_some(), "case {}: mix 必须携带 match", c.name);
        } else {
            assert!(got.m.is_none(), "case {}: 非 mix 不得携带 match", c.name);
        }
    }
}

#[test]
fn select_producer_unknown_data_kind_errors() {
    let best = net("s", "onnx");
    assert!(select_producer(Some(&best), None, None, None, 99).is_err());

    assert!(
        select_producer(
            Some(&best),
            None,
            None,
            None,
            bqs_proto::DataKind::DataNone as i32
        )
        .is_err()
    );
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
        bqs_proto::DataKind::DataResnet as i32
    );
    assert_eq!(
        data_kind_for_format("nnue"),
        bqs_proto::DataKind::DataNnue as i32
    );
    assert_eq!(
        data_kind_for_format("whatever"),
        bqs_proto::DataKind::DataResnet as i32
    );
}

#[test]
fn reanalysis_task_follows_best_stack() {
    let f = Fakes::new();
    let asm = assembler(&f);
    let item = PendingReanalysis {
        variant: "4x8".into(),
        mcts_sims: 64,
        payload: vec![9, 9],
        positions: 3,
        requester: "trainer".into(),
        created_at: 0,
    };
    let req = TaskRequest {
        current_network: String::new(),
        ..Default::default()
    };

    assert!(asm.reanalysis_task("t1", &item, &req).is_err());

    f.networks.seed_network("best", None, "onnx");
    f.networks.promote_best("best").unwrap();
    let resp = asm.reanalysis_task("t2", &item, &req).unwrap();
    assert_eq!(resp.kind, TaskKind::TaskReanalysis as i32);
    assert_eq!(resp.network_sha, "best");
    assert_eq!(resp.games, 3, "games = 位置条数");
    assert_eq!(resp.reanalysis_payload, vec![9, 9]);
    assert_eq!(resp.network_key, "networks/best.onnx");
    assert_eq!(resp.network_url, "get:networks/best.onnx");
    let p = resp.params.unwrap();
    assert_eq!(p.mcts_sims, 64, "取提交方指定值");
    assert_eq!(
        p.data_kind,
        bqs_proto::DataKind::DataResnet as i32,
        "onnx → MCTS 栈"
    );

    f.networks.seed_network("best2", None, "nnue");
    f.networks.promote_best("best2").unwrap();
    let resp2 = asm.reanalysis_task("t3", &item, &req).unwrap();
    assert_eq!(resp2.network_key, "networks/best2.nnue");
    assert_eq!(
        resp2.params.unwrap().data_kind,
        bqs_proto::DataKind::DataNnue as i32
    );

    f.networks.seed_network("best3", None, "pt");
    f.networks.promote_best("best3").unwrap();
    assert!(asm.reanalysis_task("t4", &item, &req).is_err());

    f.networks.promote_best("best").unwrap();
    let req_local = TaskRequest {
        current_network: "best".into(),
        ..Default::default()
    };
    let resp3 = asm.reanalysis_task("t5", &item, &req_local).unwrap();
    assert_eq!(resp3.network_url, "");
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
    assert_eq!(resp.kind, TaskKind::TaskDeriveFeatures as i32);
    assert_eq!(resp.episode_object_key, "episodes/sha/x.epb.gz");
    assert_eq!(
        resp.episode_url, "get:episodes/sha/x.epb.gz",
        "记录批次下载 URL 恒签发"
    );
    assert_eq!(resp.params.unwrap().variant, "4x8");
}

#[test]
fn selfplay_task_mixed_and_pure() {
    let f = Fakes::new();
    f.networks.seed_network("best", None, "onnx");
    f.networks.seed_network("opp", None, "onnx");
    let asm = assembler(&f);

    let sel = ProducerSelect {
        mode: SelfplayMode::Mix,
        producer: Some(net("cand", "nnue")),
        m: Some(matcher(7, "opp")),
        none_message: String::new(),
    };
    let req = TaskRequest {
        threads: 4,
        current_network: "cand".into(),
        ..Default::default()
    };
    let (resp, match_id) = asm.selfplay_task("t1", &req, &sel).unwrap();
    assert_eq!(match_id, 7);
    assert_eq!(resp.network_sha, "cand");
    assert_eq!(resp.network_key, "networks/cand.nnue");
    assert_eq!(
        resp.params.unwrap().data_kind,
        bqs_proto::DataKind::DataNnue as i32,
        "数据类别跟随产数据方（候选）架构"
    );
    assert_eq!(resp.opponent_sha, "opp");
    assert_eq!(resp.opponent_key, "networks/opp.onnx");
    assert_eq!(
        resp.opponent_url, "get:networks/opp.onnx",
        "对手 URL 恒签发"
    );
    assert_eq!(resp.network_url, "", "worker 已有主网络不签 URL");

    let sel_missing = ProducerSelect {
        mode: SelfplayMode::Mix,
        producer: Some(net("cand", "nnue")),
        m: Some(matcher(8, "ghost")),
        none_message: String::new(),
    };
    let (resp2, mid2) = asm.selfplay_task("t2", &req, &sel_missing).unwrap();
    assert_eq!(mid2, 0);
    assert_eq!(resp2.opponent_sha, "");

    let sel_pure = ProducerSelect {
        mode: SelfplayMode::Pure,
        producer: Some(net("best", "onnx")),
        m: None,
        none_message: String::new(),
    };
    let req_fresh = TaskRequest {
        threads: 4,
        current_network: String::new(),
        ..Default::default()
    };
    let (resp3, mid3) = asm.selfplay_task("t3", &req_fresh, &sel_pure).unwrap();
    assert_eq!(mid3, 0);
    assert_eq!(resp3.network_url, "get:networks/best.onnx");
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
    let resp = asm
        .rating_task(
            "t1",
            "4x8",
            &side_a,
            &side_b,
            Some(&cand),
            Some(&opp),
            8,
            &req,
        )
        .unwrap();
    assert_eq!(resp.kind, TaskKind::TaskRating as i32);
    assert_eq!(resp.network_sha, "cand");
    assert_eq!(resp.network_key, "networks/cand.onnx");
    assert_eq!(resp.network_url, "", "本地已有主网络不签 URL");
    assert_eq!(resp.opponent_sha, "opp");
    assert_eq!(resp.opponent_key, "networks/opp.nnue");
    assert_eq!(
        resp.opponent_url, "get:networks/opp.nnue",
        "对手 URL 恒签发"
    );
    assert_eq!(resp.games, 8);
    let p = resp.params.unwrap();
    assert_eq!(p.variant, "4x8");
    assert_eq!(
        p.data_kind,
        bqs_proto::DataKind::DataResnet as i32,
        "data_kind 跟随候选架构"
    );

    let req2 = TaskRequest {
        current_network: String::new(),
        ..Default::default()
    };
    let resp2 = asm
        .rating_task("t2", "4x8", &side_a, &side_b, Some(&cand), None, 4, &req2)
        .unwrap();
    assert_eq!(resp2.network_url, "get:networks/cand.onnx");
    assert_eq!(resp2.opponent_key, "", "无对手时不签对手键");
}

#[test]
fn sentinel_task_is_nnue_without_network() {
    let f = Fakes::new();
    let asm = assembler(&f);
    let req = TaskRequest {
        threads: 4,
        ..Default::default()
    };
    let resp = asm.sentinel_task("t1", &req);
    assert_eq!(resp.kind, TaskKind::TaskSelfplay as i32);
    assert_eq!(resp.network_sha, consts::RULE_SENTINEL_SHA);
    assert_eq!(resp.network_key, "", "哨兵任务不签发任何权重对象键");
    assert_eq!(
        resp.params.unwrap().data_kind,
        bqs_proto::DataKind::DataNnue as i32
    );
}

#[test]
fn extra_config_json_semantics() {
    let f = Fakes::new();
    let cfg = Arc::new(Config::parse("[selfplay]\nmix_ratio = 0.25\n", "test").unwrap());
    let ports = f.ports();
    let ctl = Arc::new(Control::new(&ports, cfg.clone()));
    let asm = TaskAssembler::new(f.networks.clone(), Arc::new(FakeSigner), cfg, ctl);
    assert_eq!(asm.extra_config(), r#"{"mix_ratio":0.25}"#);

    let cfg2 = Arc::new(
        Config::parse(
            "[selfplay]\nmix_ratio = 1.5\ninitial_revealed = -3\n",
            "test",
        )
        .unwrap(),
    );
    let ports2 = f.ports();
    let ctl2 = Arc::new(Control::new(&ports2, cfg2.clone()));
    let asm2 = TaskAssembler::new(f.networks.clone(), Arc::new(FakeSigner), cfg2, ctl2);
    assert_eq!(asm2.extra_config(), "", "无有效字段时为空串");
}
