use bq_env::PositionSnapshot;
use bq_env::config::Variant;
use bq_replay::ReplayStep;

use bq_record::GameLabelPack;
use bq_record::types::GameRecordBatch;
use bqs_core::types::Episode;

use crate::views::{HiddenBagView, ReplayGameView, ReplayStepView};

fn f32_at(b: &[u8], i: usize) -> f32 {
    let s = i * 4;
    if b.len() < s + 4 {
        return 0.0;
    }
    f32::from_le_bytes(b[s..s + 4].try_into().unwrap())
}
fn u32_at(b: &[u8], i: usize) -> u32 {
    let s = i * 4;
    if b.len() < s + 4 {
        return 0;
    }
    u32::from_le_bytes(b[s..s + 4].try_into().unwrap())
}
fn u8_at(b: &[u8], i: usize) -> u8 {
    b.get(i).copied().unwrap_or(0)
}

fn u16_at(b: &[u8], i: usize) -> Option<u32> {
    let s = i * 2;
    if b.len() < s + 2 {
        return None;
    }
    Some(u16::from_le_bytes(b[s..s + 2].try_into().unwrap()) as u32)
}

fn decode_snapshots(snapshots: &[Vec<u8>], steps: usize) -> Result<Vec<PositionSnapshot>, String> {
    if snapshots.len() != steps {
        return Err(format!(
            "replay: 步前快照缺失/解码失败（{}/{} 条）",
            snapshots.len(),
            steps
        ));
    }
    snapshots
        .iter()
        .map(|b| PositionSnapshot::decode(b).map_err(|e| format!("replay: 快照解码失败: {e}")))
        .collect()
}

fn labels_for_game<'a>(pack: &'a GameLabelPack, index: usize) -> Option<&'a bq_record::GameLabels> {
    let exact = pack
        .games
        .iter()
        .find(|g| g.context.as_ref().map(|c| c.game_index as usize) == Some(index));
    exact.or_else(|| pack.games.get(index))
}

pub fn build_game(
    batch: &GameRecordBatch,
    labels: Option<&GameLabelPack>,
    ep: &Episode,
    index: usize,
) -> Result<ReplayGameView, String> {
    if index >= batch.records.len() {
        return Err(format!(
            "局序号 {} 超出范围（共 {} 局）",
            index + 1,
            batch.records.len()
        ));
    }
    let rec = &batch.records[index];
    let identity = rec.identity.as_ref();
    let outcome = rec.outcome.as_ref();
    let play = rec.play.as_ref().ok_or("replay: 缺少 play 分组")?;
    let steps = play.steps as usize;
    let variant: Variant = identity
        .map(|i| i.variant.as_str())
        .unwrap_or_default()
        .parse()
        .map_err(|e| format!("replay: 未知变体: {e}"))?;
    let _ = variant;

    let input = bq_record::replay::to_replay_input(rec)?;
    let game = bq_replay::build_replay(&input)?;

    let snaps = decode_snapshots(&play.snapshots, steps)?;
    let mut actions = Vec::with_capacity(steps);
    for i in 0..steps {
        actions.push(u16_at(&play.actions, i).ok_or_else(|| {
            format!(
                "replay: actions 字节数 {} != steps*2={}",
                play.actions.len(),
                steps * 2
            )
        })?);
    }

    let final_fen = input
        .final_snapshot
        .as_ref()
        .and_then(|b| PositionSnapshot::decode(b).ok())
        .map(|s| s.to_fen());

    let label = labels.and_then(|p| labels_for_game(p, index));
    let labels_missing = label.is_none();
    let train = label.and_then(|g| g.labels.as_ref());
    let diag = label.and_then(|g| g.diagnostics.as_ref());

    let mut expl_count = 0usize;

    let mut step_views = Vec::with_capacity(steps);
    for (ply, m) in game.moves.iter().enumerate() {
        let action = actions[ply];
        let st = &m;
        let ReplayStep {
            board,
            revealed_at,
            hidden_bag,
            scores,
            kind,
            ..
        } = st;

        let move_counter = if *kind == bq_replay::StepKind::Move {
            st.move_counter.saturating_sub(1)
        } else {
            snaps[ply].move_counter
        };

        let p = rec
            .provenance
            .as_ref()
            .map(|pr| u8_at(&pr.producers, ply))
            .unwrap_or(0xFF);
        let producer: i64 = if p == 0xFF { -1 } else { p as i64 };

        if let Some(d) = diag
            && u8_at(&d.expl_random, ply) == 1
        {
            expl_count += 1;
        }

        step_views.push(ReplayStepView {
            ply,
            player: m.player.clone(),
            action,
            from: m.from as i64,
            to: m.to as i64,
            kind: kind.as_str().to_string(),
            is_chance: m.is_chance,
            revealed: m.revealed.clone().unwrap_or_default(),
            captured: m.captured.clone().unwrap_or_default(),
            move_counter: move_counter as i64,
            scores: [scores[0] as i64, scores[1] as i64],
            board: board.clone(),
            fen: snaps[ply].to_fen(),
            revealed_at: revealed_at.clone(),
            hidden_bag: HiddenBagView {
                red: hidden_bag.red.map(i64::from),
                black: hidden_bag.black.map(i64::from),
            },
            board_source: m.board_source.clone(),
            producer,
            search_value: train.map(|t| f32_at(&t.value_labels, ply)),
            player_value: train.map(|t| f32_at(&t.own_values, ply)),
            hidden_count: diag.map(|d| u32_at(&d.hidden_counts, ply) as i64),
            search_depth: diag.map(|d| u8_at(&d.search_depths, ply) as i64),
            search_nodes: diag.map(|d| u32_at(&d.search_nodes, ply) as i64),
            chosen_value_gap: diag.map(|d| f32_at(&d.chosen_value_gaps, ply)),
            expl_random: diag.map(|d| u8_at(&d.expl_random, ply) == 1),
        });
    }

    let red_is_a = crate::handlers_replay::red_is_a_of(rec);
    Ok(ReplayGameView {
        index,
        steps,
        game_length: steps,
        winner: outcome.and_then(|o| o.winner),
        termination: crate::handlers_replay::termination_label(
            outcome.map(|o| o.termination_reason).unwrap_or(0),
        )
        .to_string(),
        seed: identity
            .and_then(|i| i.game_seed)
            .map(|s| s.to_string())
            .unwrap_or_default(),
        has_seed: identity.and_then(|i| i.game_seed).is_some(),
        side_a: crate::handlers_replay::describe_side(&ep.network_sha, ""),
        side_b: crate::handlers_replay::describe_side(&ep.opponent_sha, &ep.opponent_spec),
        red_is_a,
        a_side_only: false,
        initial_fen: snaps.first().map(|s| s.to_fen()),
        final_fen,
        initial_board: game.initial_board,
        initial_shown: game.initial_shown,
        initial_bag: HiddenBagView {
            red: game.initial_hidden_bag.red.map(i64::from),
            black: game.initial_hidden_bag.black.map(i64::from),
        },
        final_board: game.final_board,
        final_shown: game.final_shown,
        final_bag: HiddenBagView {
            red: game.final_hidden_bag.red.map(i64::from),
            black: game.final_hidden_bag.black.map(i64::from),
        },
        moves: step_views,
        expl_count,
        labels_missing,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bq_env::DarkChessEnv;
    use bq_env::GameEnv;
    use bq_env::SnapshotEnv;
    use bq_env::seed::SeedableEnv;
    use bq_env::types::{Player, Slot};
    use bq_record::codec::{LABEL_PACK_VERSION, encode_label_pack_gz};
    use bq_record::types::{
        DataKind, GameIdentity, GameOutcome, GamePlay, GameRecord, Provenance, RecordSource,
        StepDiagnostics, TrainingLabels,
    };
    use prost::Message;

    fn synth(steps: usize, with_diags: bool) -> (GameRecord, GameLabelPack) {
        let mut env = DarkChessEnv::default();

        env.set_seed(42);
        let initial_layout = env.initial_layout_bytes().expect("设种子后应有初始布局");

        let mut actions = Vec::new();
        let mut players = Vec::new();
        let mut snapshots = Vec::new();
        let mut producers = Vec::new();
        let mut value_labels = Vec::new();
        let mut own_values = Vec::new();
        let mut hidden_counts = Vec::new();
        let mut search_depths = Vec::new();
        let mut search_nodes = Vec::new();
        let mut chosen_gaps = Vec::new();
        let mut expl_random = Vec::new();

        for _i in 0..steps {
            if env.check_game_over_conditions().0 {
                break;
            }
            let action = *env.legal_actions().first().unwrap();
            snapshots.push(env.to_snapshot().encode());
            hidden_counts.extend_from_slice(
                &(env
                    .to_snapshot()
                    .slots
                    .iter()
                    .filter(|s| matches!(s, Slot::Hidden))
                    .count() as u32)
                    .to_le_bytes(),
            );
            actions.extend_from_slice(&(action as u16).to_le_bytes());
            players.push(if env.to_snapshot().current_player == Player::Red {
                1u8
            } else {
                0xFF
            });
            producers.push(0u8);
            if with_diags {
                value_labels.extend_from_slice(&0.25f32.to_le_bytes());
                own_values.extend_from_slice(&(-0.5f32).to_le_bytes());
                search_depths.push(4u8);
                search_nodes.extend_from_slice(&1024u32.to_le_bytes());
                chosen_gaps.extend_from_slice(&0.0f32.to_le_bytes());
                expl_random.push(0u8);
            }
            env.step_with_reveal(action, None).unwrap();
        }
        let real_steps = actions.len() / 2;

        let rec = GameRecord {
            identity: Some(GameIdentity {
                variant: "4x8".into(),
                source: RecordSource::SourceCollector as i32,
                game_seed: Some(42),
                game_index: Some(0),
                a_is_red: Some(true),
                ..Default::default()
            }),
            play: Some(GamePlay {
                steps: real_steps as u32,
                initial_layout: initial_layout.clone(),
                players,
                actions,
                snapshots,
            }),
            outcome: Some(GameOutcome {
                winner: Some(1),
                termination_reason: 1,
                ..Default::default()
            }),
            provenance: Some(Provenance {
                producers,
                ..Default::default()
            }),
            ..Default::default()
        };

        let mut labels = bq_record::GameLabels::new(0);
        if let Some(ctx) = labels.context.as_mut() {
            ctx.steps = real_steps as u32;
            ctx.players = rec.play.as_ref().unwrap().players.clone();
            ctx.actions = rec.play.as_ref().unwrap().actions.clone();
            ctx.game_seed = Some(42);
        }
        if with_diags {
            labels.labels = Some(TrainingLabels {
                value_labels,
                own_values,
                ..Default::default()
            });
            labels.diagnostics = Some(StepDiagnostics {
                hidden_counts,
                search_depths,
                search_nodes,
                chosen_value_gaps: chosen_gaps,
                expl_random,
            });
        } else {
            labels.labels = Some(TrainingLabels::default());
            labels.diagnostics = Some(StepDiagnostics::default());
        }

        let mut pack = GameLabelPack::new("4x8", DataKind::DataNnue, "f".repeat(64));
        pack.games = vec![labels];
        pack.game_count = 1;
        pack.steps_total = real_steps as u64;
        (rec, pack)
    }

    fn episode() -> Episode {
        Episode {
            id: 7,
            worker_id: "w".into(),
            task_id: "t".into(),
            network_sha: "abcdef1234567890".into(),
            game_count: 1,
            total_steps: 0,
            winner: 0,
            object_key: "episodes/7.epb.gz".into(),
            created_at: 0,
            kind: 1,
            opponent_sha: String::new(),
            opponent_spec: "capture_first".into(),
            match_id: 0,
            red_wins: 0,
            black_wins: 0,
            draw_no_capture: 0,
            draw_truncation: 0,
            seed_base: None,
            pairs_parallel: None,
            pair_seed_refresh: None,
            label_object_key: String::new(),
            label_sha256: String::new(),
            label_size: 0,
        }
    }

    fn batch(rec: GameRecord) -> GameRecordBatch {
        GameRecordBatch {
            variant: "4x8".into(),
            kind: DataKind::DataNnue as i32,
            records: vec![rec],
            ..Default::default()
        }
    }

    #[test]
    fn full_conversion_snapshot_path() {
        let (rec, pack) = synth(8, true);
        let g = build_game(&batch(rec), Some(&pack), &episode(), 0).expect("完整转换应成功");
        assert_eq!(g.index, 0);
        assert_eq!(g.steps, 8);
        assert_eq!(g.moves.len(), 8);
        assert_eq!(g.seed, "42");
        assert!(g.has_seed);
        assert_eq!(g.termination, "decisive");
        assert_eq!(g.winner, Some(1));
        assert_eq!(g.side_a, "NNUE abcdef123456");
        assert_eq!(g.side_b, "规则评估器（capture_first）");

        assert!(g.red_is_a);

        assert_eq!(g.initial_board.len(), 32);
        assert_eq!(g.initial_shown.len(), 32);
        assert_eq!(g.final_board.len(), 32);
        assert_eq!(g.board_source_of_first(), "snapshot");

        let s = &g.moves[0];
        assert_eq!(s.ply, 0);
        assert_eq!(s.search_value, Some(0.25));
        assert_eq!(s.player_value, Some(-0.5));
        assert_eq!(s.search_depth, Some(4));
        assert_eq!(s.search_nodes, Some(1024));
        assert_eq!(s.producer, 0);

        assert_eq!(g.final_shown.len(), 32);
        assert!(!g.labels_missing);

        let json = serde_json::to_value(&g).unwrap();
        assert!(json.get("labelsMissing").is_none());
    }

    #[test]
    fn missing_label_pack_degrades_to_null() {
        let (rec, _) = synth(3, false);
        let g = build_game(&batch(rec), None, &episode(), 0).unwrap();
        assert!(g.labels_missing);
        for m in &g.moves {
            assert_eq!(m.search_value, None);
            assert_eq!(m.player_value, None);
            assert_eq!(m.search_depth, None);
            assert_eq!(m.search_nodes, None);
            assert_eq!(m.hidden_count, None);
            assert_eq!(m.chosen_value_gap, None);
            assert_eq!(m.expl_random, None);
        }
        assert_eq!(g.expl_count, 0);

        let json = serde_json::to_value(&g).unwrap();
        assert_eq!(json["labelsMissing"], serde_json::json!(true));
        assert!(json["moves"][0]["searchValue"].is_null());
    }

    #[test]
    fn empty_label_groups_default_to_zero() {
        let (rec, pack) = synth(3, false);
        let g = build_game(&batch(rec), Some(&pack), &episode(), 0).unwrap();
        assert!(!g.labels_missing, "局有 GameLabels 槽位即视为有标签包");
        for m in &g.moves {
            assert_eq!(m.search_value, Some(0.0), "TrainingLabels 空组 → 0 值");
            assert_eq!(m.search_depth, Some(0), "StepDiagnostics 空组 → 0 值");
        }
    }

    #[test]
    fn index_out_of_range_message() {
        let (rec, pack) = synth(2, true);
        let err = build_game(&batch(rec), Some(&pack), &episode(), 3).unwrap_err();
        assert!(err.contains("超出范围"), "{err}");
    }

    #[test]
    fn unseeded_report_replays_without_identity() {
        let mut env = DarkChessEnv::default();
        let mut rec = GameRecord::new("4x8", RecordSource::SourceCollector);
        let reveal_actions = env.config.reveal_actions_count;
        for _ in 0..6 {
            if env.check_game_over_conditions().0 {
                break;
            }
            let action = *env
                .legal_actions()
                .iter()
                .find(|&&a| (a as usize) < reveal_actions)
                .expect("开局必有翻子动作");
            let play = rec.play.get_or_insert_with(Default::default);
            play.snapshots.push(env.to_snapshot().encode());
            play.actions
                .extend_from_slice(&(action as u16).to_le_bytes());
            play.players
                .push(if env.get_current_player() == Player::Red {
                    1u8
                } else {
                    0xFF
                });
            env.step_with_reveal(action, None).unwrap();
        }
        let play = rec.play.as_mut().unwrap();
        play.steps = play.players.len() as u32;
        assert!(play.steps >= 2, "合成翻子对局应至少 2 步");

        let g = build_game(&batch(rec), None, &episode(), 0).expect("无 seed 上报也应可回放");
        assert_eq!(g.moves.len(), g.steps);
        let pre_revealed = env.config.initial_revealed_pieces;
        assert_eq!(
            g.initial_shown.iter().filter(|&&s| s).count(),
            pre_revealed,
            "开局明子数应与 initial_revealed_pieces 一致"
        );

        let flips: Vec<_> = g.moves.iter().filter(|m| m.kind == "flip").collect();
        assert!(!flips.is_empty(), "合成对局应含翻子步");
    }

    #[test]
    fn missing_snapshot_rejects_whole_game() {
        let (mut rec, pack) = synth(4, true);
        rec.play.as_mut().unwrap().snapshots.truncate(2);
        let err = build_game(&batch(rec), Some(&pack), &episode(), 0).unwrap_err();
        assert!(err.contains("步前快照"), "{err}");
    }

    #[test]
    fn unknown_variant_rejected() {
        let (mut rec, pack) = synth(2, true);
        rec.identity.as_mut().unwrap().variant = "9x9".into();
        let err = build_game(&batch(rec), Some(&pack), &episode(), 0).unwrap_err();
        assert!(err.contains("未知变体"), "{err}");
    }

    #[test]
    fn proto_roundtrip_and_label_gz_preserves_fields() {
        let (rec, pack) = synth(5, true);
        let b = batch(rec);
        let bytes = b.encode_to_vec();
        let b2 = GameRecordBatch::decode(bytes.as_slice()).unwrap();

        let gz = encode_label_pack_gz(&pack).unwrap();
        let pack2 = bq_record::codec::decode_label_pack_gz(&gz).unwrap();
        assert_eq!(pack2.pack_version, LABEL_PACK_VERSION);

        let g = build_game(&b2, Some(&pack2), &episode(), 0).unwrap();
        assert_eq!(g.steps, 5);
        assert_eq!(g.moves[2].search_value, Some(0.25));
    }

    impl ReplayGameView {
        fn board_source_of_first(&self) -> &str {
            &self.moves[0].board_source
        }
    }
}
