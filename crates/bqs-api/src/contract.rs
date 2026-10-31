use super::*;
use crate::views::*;

fn keys<T: serde::Serialize>(v: &T) -> Vec<String> {
    match serde_json::to_value(v).unwrap() {
        serde_json::Value::Object(map) => map.keys().cloned().collect(),
        other => panic!("期望 JSON 对象，实际 {other}"),
    }
}

fn assert_keys(got: &[String], want: &[&str]) {
    for k in want {
        assert!(
            got.iter().any(|g| g == k),
            "缺少字段 {k}（前端按名字取值，缺字段是破坏性变更）"
        );
    }
    for g in got {
        assert!(
            want.contains(&g.as_str()),
            "出现未预期的字段 {g}（契约收敛后新增字段应同步更新快照测试）"
        );
    }
}

#[test]
fn status_top_level() {
    let v = StatusView {
        variant: "4x8".into(),
        paused: false,
        initial_revealed: 0,
        data_kind: "resnet".into(),
        min_client_version: String::new(),
        sprt: SprtView {
            elo0: 0.0,
            elo1: 15.0,
            alpha: 0.05,
            beta: 0.05,
        },
        best: None,
        counts: CountsView {
            networks: 0,
            candidates: 0,
            matches_running: 0,
            episodes: 0,
            episode_games: 0,
            workers: 0,
        },
        should_stop: false,
        stop_reason: String::new(),
        auto_paused: false,
        auto_pause_reason: String::new(),
    };
    assert_keys(
        &keys(&v),
        &[
            "variant",
            "paused",
            "initialRevealed",
            "dataKind",
            "minClientVersion",
            "sprt",
            "best",
            "counts",
            "shouldStop",
            "stopReason",
            "autoPaused",
            "autoPauseReason",
        ],
    );
    assert_keys(&keys(&v.sprt), &["elo0", "elo1", "alpha", "beta"]);
    assert_keys(
        &keys(&v.counts),
        &[
            "networks",
            "candidates",
            "matchesRunning",
            "episodes",
            "episodeGames",
            "workers",
        ],
    );
}

#[test]
fn network_view() {
    let v = NetworkView {
        sha: "abc".into(),
        parent_sha: String::new(),
        created_at: 1,
        is_best: true,
        status: "best".into(),
        notes: String::new(),
    };
    assert_keys(
        &keys(&v),
        &["sha", "parentSha", "createdAt", "isBest", "status", "notes"],
    );
}

#[test]
fn match_view() {
    let v = MatchView {
        id: 1,
        candidate: "c".into(),
        opponent: "o".into(),
        status: "running".into(),
        pairs: bqs_judgment::Hexanomial([0; 6]),
        num_games: 0,
        target_games: 400,
        score: None,
        llr: 0.0,
        lower: -2.944,
        upper: 2.944,
        verdict: "continue".into(),
    };
    assert_keys(
        &keys(&v),
        &[
            "id",
            "candidate",
            "opponent",
            "status",
            "pairs",
            "numGames",
            "targetGames",
            "score",
            "llr",
            "lower",
            "upper",
            "verdict",
        ],
    );
    let json = serde_json::to_value(&v).unwrap();
    assert_eq!(json["pairs"].as_array().unwrap().len(), 6);
    assert!(json["score"].is_null());
}

#[test]
fn worker_view() {
    let v = WorkerView {
        id: "w".into(),
        note: String::new(),
        last_seen: 0,
        online: true,
        threads: 8,
        completed_games: 0,
        client_version: "0.1.0".into(),
        memory_mb: 1024,
        health_status: "ok".into(),
        last_error: String::new(),
        episodes_1h: 0,
        games_1h: 0,
        episodes_24h: 0,
        games_24h: 0,
        est_games_per_hour: 0.0,
        est_per_thread_hour: 0.0,
        est_samples: 0,
    };
    assert_keys(
        &keys(&v),
        &[
            "id",
            "note",
            "lastSeen",
            "online",
            "threads",
            "completedGames",
            "clientVersion",
            "memoryMb",
            "healthStatus",
            "lastError",
            "episodes1h",
            "games1h",
            "episodes24h",
            "games24h",
            "estGamesPerHour",
            "estPerThreadHour",
            "estSamples",
        ],
    );
}

#[test]
fn trainers_view() {
    let tv = TrainerView {
        id: "tr-1".into(),
        last_seen: 0,
        online: true,
        variant: "4x8".into(),
        data_kind: "resnet".into(),
        consumed_key: String::new(),
        consumed_id: 0,
        registered_max_id: 0,
        unconsumed_batches: 0,
        samples_consumed: 0,
        backlog: 0,
        buffered: 0,
        health_status: "ok".into(),
        last_error: String::new(),
        progress: String::new(),
    };
    let v = TrainersView {
        config: BalanceConfigView {
            enabled: true,
            mode: "pause".into(),
            trainer_online_seconds: 120,
            stalled_seconds: 600,
            backlog_pause_batches: 0,
        },
        auto_paused: false,
        auto_pause_reason: String::new(),
        trainers: vec![tv],
    };
    assert_keys(
        &keys(&v),
        &["config", "autoPaused", "autoPauseReason", "trainers"],
    );
    assert_keys(
        &keys(&v.config),
        &[
            "enabled",
            "mode",
            "trainerOnlineSeconds",
            "stalledSeconds",
            "backlogPauseBatches",
        ],
    );
    assert_keys(
        &keys(&v.trainers[0]),
        &[
            "id",
            "lastSeen",
            "online",
            "variant",
            "dataKind",
            "consumedKey",
            "consumedId",
            "registeredMaxId",
            "unconsumedBatches",
            "samplesConsumed",
            "backlog",
            "buffered",
            "healthStatus",
            "lastError",
            "progress",
        ],
    );
}

#[test]
fn train_config_view() {
    let with_enum = TrainConfigFieldView {
        name: "VALUE_TARGET_MODE".into(),
        kind: "enum".into(),
        min: 0.0,
        max: 0.0,
        has_max: false,
        gt_zero: false,
        r#enum: vec!["mcts".into()],
    };
    let no_enum = TrainConfigFieldView {
        name: "TRAIN_BATCH".into(),
        kind: "int".into(),
        min: 1.0,
        max: 0.0,
        has_max: false,
        gt_zero: false,
        r#enum: vec![],
    };
    let v = TrainConfigView {
        overrides: Default::default(),
        fields: vec![with_enum, no_enum],
    };
    assert_keys(&keys(&v), &["overrides", "fields"]);
    let json = serde_json::to_value(&v).unwrap();
    let f0 = &json["fields"][0];
    assert!(f0.get("enum").is_some(), "enum 字段存在时应输出");
    assert!(
        json["fields"][1].get("enum").is_none(),
        "空 enum 应被省略（omitempty）"
    );
}

#[test]
fn episode_views() {
    let ev = EpisodeView {
        id: 1,
        worker_id: "w".into(),
        task_id: "t".into(),
        network_sha: "s".into(),
        game_count: 16,
        total_steps: 100,
        winner: 1,
        data_kind: "resnet".into(),
        object_key: "episodes/s/x.epb.gz".into(),
        opponent_sha: String::new(),
        opponent_spec: String::new(),
        match_id: 0,
        red_wins: 0,
        black_wins: 0,
        draw_no_capture: 0,
        draw_truncation: 0,
        created_at: 0,
    };
    assert_keys(
        &keys(&ev),
        &[
            "id",
            "workerId",
            "taskId",
            "networkSha",
            "gameCount",
            "totalSteps",
            "winner",
            "dataKind",
            "objectKey",
            "opponentSha",
            "opponentSpec",
            "matchId",
            "redWins",
            "blackWins",
            "drawNoCapture",
            "drawTruncation",
            "createdAt",
        ],
    );
    let page = EpisodePageView {
        items: vec![],
        next_before: 0,
        has_more: false,
    };
    assert_keys(&keys(&page), &["items", "nextBefore", "hasMore"]);
}

#[test]
fn storage_views() {
    assert_eq!(mask_key("abcd1234efgh"), "abcd****efgh");
    assert_eq!(mask_key("short"), "****");
    assert_eq!(mask_key(""), "");
    assert!(valid_profile_name("r2-prod_1"));
    assert!(!valid_profile_name("bad/name"));
    assert!(!valid_profile_name(""));
    assert!(!valid_profile_name(&"x".repeat(33)));
    let pv = StorageProfileView {
        name: "r2".into(),
        endpoint: "http://127.0.0.1:1".into(),
        region: "auto".into(),
        access_key: "abcd****efgh".into(),
        has_secret: true,
        bucket: "banqi".into(),
        path_style: true,
    };
    assert_keys(
        &keys(&pv),
        &[
            "name",
            "endpoint",
            "region",
            "accessKey",
            "hasSecret",
            "bucket",
            "pathStyle",
        ],
    );
    let sv = StorageView {
        active: "default".into(),
        profiles: vec![],
    };
    assert_keys(&keys(&sv), &["active", "profiles"]);
}

#[test]
fn replay_views() {
    use crate::views::*;

    let bag = HiddenBagView {
        red: [5, 2, 2, 2, 2, 2, 1],
        black: [0; 7],
    };
    assert_keys(&keys(&bag), &["red", "black"]);

    let summary = ReplayGameSummaryView {
        index: 0,
        steps: 8,
        game_length: 8,
        winner: Some(1),
        termination: "decisive".into(),
        has_seed: true,
    };
    assert_keys(
        &keys(&summary),
        &[
            "index",
            "steps",
            "gameLength",
            "winner",
            "termination",
            "hasSeed",
        ],
    );

    let list = ReplayListView {
        id: 7,
        kind: "nnue".into(),
        variant: "4x8".into(),
        game_count: 1,
        games: vec![summary],
    };
    assert_keys(
        &keys(&list),
        &["id", "kind", "variant", "gameCount", "games"],
    );

    let step = ReplayStepView {
        ply: 0,
        player: "Red".into(),
        action: 3,
        from: 3,
        to: 3,
        kind: "flip".into(),
        is_chance: true,
        revealed: "R_Gen".into(),
        captured: "B_Can".into(),
        move_counter: 0,
        scores: [60, 60],
        board: vec!["Hidden".into()],
        revealed_at: vec![false],
        hidden_bag: bag,
        board_source: "snapshot".into(),
        producer: 0,
        search_value: 0.25,
        player_value: -0.5,
        hidden_count: 32,
        search_depth: 4,
        search_nodes: 1024,
        chosen_value_gap: 0.0,
        expl_random: false,
        labeler_fallback: false,
        labeler_action: Some(3),
        policy_actions: Some(vec![3]),
        policy_values: Some(vec![1.0]),
    };
    let step_json = serde_json::to_value(&step).unwrap();
    for k in [
        "ply",
        "player",
        "action",
        "from",
        "to",
        "kind",
        "isChance",
        "revealed",
        "captured",
        "moveCounter",
        "scores",
        "board",
        "revealedAt",
        "hiddenBag",
        "boardSource",
        "producer",
        "searchValue",
        "playerValue",
        "hiddenCount",
        "searchDepth",
        "searchNodes",
        "chosenValueGap",
        "explRandom",
        "labelerFallback",
        "labelerAction",
        "policyActions",
        "policyValues",
    ] {
        assert!(step_json.get(k).is_some(), "ReplayStep 缺字段 {k}");
    }

    let mut quiet = ReplayStepView {
        is_chance: false,
        revealed: String::new(),
        captured: String::new(),
        policy_actions: None,
        policy_values: None,
        labeler_action: None,
        ..step.clone()
    };
    quiet.is_chance = false;
    quiet.revealed = String::new();
    quiet.captured = String::new();
    let quiet_json = serde_json::to_value(&quiet).unwrap();
    assert!(
        quiet_json.get("isChance").is_none(),
        "isChance=false 应省略"
    );
    assert!(quiet_json.get("revealed").is_none(), "revealed 空 应省略");
    assert!(quiet_json.get("captured").is_none(), "captured 空 应省略");

    quiet.policy_actions = None;
    quiet.policy_values = None;
    quiet.labeler_action = None;
    let null_json = serde_json::to_value(&quiet).unwrap();
    assert!(null_json["policyActions"].is_null());
    assert!(null_json["policyValues"].is_null());
    assert!(null_json["labelerAction"].is_null());

    let game = ReplayGameView {
        index: 0,
        steps: 8,
        game_length: 8,
        winner: Some(1),
        termination: "decisive".into(),
        seed: 42,
        has_seed: true,
        side_a: "NNUE abcdef123456".into(),
        side_b: "规则评估器（capture_first）".into(),
        red_is_a: true,
        a_side_only: false,
        initial_board: vec!["Hidden".into()],
        initial_shown: vec![false],
        initial_bag: bag,
        final_board: vec!["Empty".into()],
        final_shown: vec![true],
        final_bag: bag,
        moves: vec![step],
        labeler_agree: 1.0,
        labeler_total: 8,
        expl_count: 0,
    };
    assert_keys(
        &keys(&game),
        &[
            "index",
            "steps",
            "gameLength",
            "winner",
            "termination",
            "seed",
            "hasSeed",
            "sideA",
            "sideB",
            "redIsA",
            "aSideOnly",
            "initialBoard",
            "initialShown",
            "initialBag",
            "finalBoard",
            "finalShown",
            "finalBag",
            "moves",
            "labelerAgree",
            "labelerTotal",
            "explCount",
        ],
    );
}
