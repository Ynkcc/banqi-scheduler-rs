use bq_env::config::Variant;
use bq_env::types::{Piece, PieceType, Player, Slot};
use bq_env::{PositionSnapshot, SnapshotEnv};
use bq_replay::{ReplayInput, ReplayMove, ReplayStep};

use bqs_core::types::Episode;
use bqs_proto::{EpisodeBatch, GameRecord};

use crate::views::{HiddenBagView, ReplayGameView, ReplayStepView};

fn action_to_coords(rows: usize, cols: usize) -> Vec<(usize, usize)> {
    let n = rows * cols;
    let mut out = Vec::with_capacity(n * 6);
    for sq in 0..n {
        out.push((sq, sq));
    }
    for r1 in 0..rows {
        for c1 in 0..cols {
            let from = r1 * cols + c1;
            for (dr, dc) in [(-1i64, 0i64), (1, 0), (0, -1), (0, 1)] {
                let (r2, c2) = (r1 as i64 + dr, c1 as i64 + dc);
                if r2 >= 0 && r2 < rows as i64 && c2 >= 0 && c2 < cols as i64 {
                    out.push((from, (r2 as usize) * cols + c2 as usize));
                }
            }
        }
    }
    for r1 in 0..rows {
        for c1 in 0..cols {
            let from = r1 * cols + c1;
            for c2 in 0..cols {
                if c1.abs_diff(c2) > 1 {
                    out.push((from, r1 * cols + c2));
                }
            }
            for r2 in 0..rows {
                if r1.abs_diff(r2) > 1 {
                    out.push((from, r2 * cols + c1));
                }
            }
        }
    }
    out
}

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

fn piece_from_layout_byte(b: u8) -> Option<Piece> {
    let ty = [
        PieceType::Soldier,
        PieceType::Cannon,
        PieceType::Horse,
        PieceType::Chariot,
        PieceType::Elephant,
        PieceType::Advisor,
        PieceType::General,
    ][(b & 0x7) as usize];
    let pl = if (b >> 3) & 1 == 0 {
        Player::Red
    } else {
        Player::Black
    };
    Some(Piece::new(ty, pl))
}

fn hidden_identity(snap: &PositionSnapshot, sq: usize, layout: Option<&[Piece]>) -> Option<Piece> {
    snap.true_board
        .as_ref()
        .and_then(|tb| tb.get(sq))
        .copied()
        .or_else(|| layout.and_then(|l| l.get(sq)).copied())
}

fn decode_snapshots(rec: &GameRecord, steps: usize) -> Result<Vec<PositionSnapshot>, String> {
    if rec.snapshots.len() != steps {
        return Err(format!(
            "replay: 步前快照缺失/解码失败（{}/{} 条）",
            rec.snapshots.len(),
            steps
        ));
    }
    rec.snapshots
        .iter()
        .map(|b| PositionSnapshot::decode(b).map_err(|e| format!("replay: 快照解码失败: {e}")))
        .collect()
}

pub fn build_game(
    batch: &EpisodeBatch,
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
    let variant: Variant = batch.variant.parse().map_err(|e| format!("replay: {e}"))?;
    let cfg = variant.config();
    let positions = cfg.total_positions;
    let steps = rec.steps as usize;
    let table = action_to_coords(cfg.rows, cfg.cols);

    let layout: Option<Vec<Piece>> = if rec.initial_layout.len() == positions {
        Some(
            rec.initial_layout
                .iter()
                .map(|&b| piece_from_layout_byte(b))
                .collect::<Option<Vec<_>>>()
                .ok_or("replay: initial_layout 含非法棋子字节")?,
        )
    } else {
        None
    };

    let snaps = decode_snapshots(rec, steps)?;

    let mut actions = Vec::with_capacity(steps);
    for i in 0..steps {
        actions.push(u16_at(&rec.actions, i).ok_or_else(|| {
            format!(
                "replay: actions 字节数 {} != steps*2={}",
                rec.actions.len(),
                steps * 2
            )
        })?);
    }
    if rec.players.len() != steps {
        return Err(format!(
            "replay: players 字节数 {} != steps={}",
            rec.players.len(),
            steps
        ));
    }
    let players_i8 = |i: usize| rec.players[i] as i8;

    let final_snapshot: Option<Vec<u8>> = if steps > 0 && snaps[steps - 1].true_board.is_some() {


        let last_pre = &snaps[steps - 1];
        let stepped = bq_env::for_variant_env!(variant, E => {
            <E as SnapshotEnv>::from_snapshot(last_pre)
                .and_then(|mut env| env.step_with_reveal(actions[steps - 1] as usize, None).map(|_| env.to_snapshot()))
        });
        stepped.ok().map(|s| s.encode())
    } else {
        None
    };

    let mut moves = Vec::with_capacity(steps);
    for (ply, &action) in actions.iter().enumerate() {
        let Some((from, to)) = table.get(action as usize).copied() else {
            return Err(format!(
                "replay: step {ply} 动作 {action} 超出动作空间 {}",
                table.len()
            ));
        };
        let pre = &snaps[ply];
        let (is_flip, is_chance, revealed, captured) = if (action as usize) < positions {
            if !matches!(pre.slots[to], Slot::Hidden) {
                return Err(format!("replay: step {ply} 翻棋目标 {to} 与快照明暗不符"));
            }


            let id = hidden_identity(pre, to, layout.as_deref());
            (true, true, id.map(|p| p.short_name()), None)
        } else {
            if !matches!(pre.slots[from], Slot::Revealed(_)) {
                return Err(format!(
                    "replay: step {ply} 起点格 {from} 与快照不符（无棋子）"
                ));
            }
            match pre.slots[to] {
                Slot::Empty => (false, false, None, None),
                Slot::Hidden => {
                    let id = hidden_identity(pre, to, layout.as_deref());
                    (
                        false,
                        true,
                        id.map(|p| p.short_name()),
                        id.map(|p| p.short_name()),
                    )
                }
                Slot::Revealed(victim) => (false, false, None, Some(victim.short_name())),
            }
        };
        moves.push(ReplayMove {
            player: if players_i8(ply) < 0 {
                "Black".into()
            } else {
                "Red".into()
            },
            action: action as usize,
            from,
            to,
            is_flip,
            is_chance,
            is_capture: captured.is_some() && !is_chance,
            revealed,
            captured,
        });
    }

    let input = ReplayInput {
        variant,
        initial_fen: None,
        initial_layout: layout.clone(),
        snapshots: rec.snapshots.clone(),
        final_snapshot,
        final_fen: None,
        moves,
    };
    let game = bq_replay::build_replay(&input)?;

    let policy_offsets: Vec<u32> = (0..=steps + 1)
        .take_while(|&i| i * 4 + 4 <= rec.labeler_policy_offsets.len())
        .map(|i| u32_at(&rec.labeler_policy_offsets, i))
        .collect();
    let mut labeler_total = 0usize;
    let mut labeler_agree = 0f64;
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

        let p = u8_at(&rec.producers, ply);
        let producer: i64 = if p == 0xFF { -1 } else { p as i64 };

        let mut labeler_action = None;
        let la = u32_at(&rec.labeler_actions, ply);
        if la != 0xFFFF_FFFF {
            labeler_action = Some(la);
            labeler_total += 1;
            if let (Some((lf, lt)), Some((af, at))) =
                (table.get(la as usize), table.get(action as usize))
                && (lf, lt) == (af, at)
            {
                labeler_agree += 1.0;
            }
        }
        if u8_at(&rec.expl_random, ply) == 1 {
            expl_count += 1;
        }

        let (policy_actions, policy_values) = {
            let (lo, hi) = (
                policy_offsets.get(ply).copied().unwrap_or(0),
                policy_offsets.get(ply + 1).copied().unwrap_or(0),
            );
            if hi > lo && (hi as usize) * 4 <= rec.labeler_policy_actions.len() {
                let pa: Vec<u32> = (lo as usize..hi as usize)
                    .map(|k| u32_at(&rec.labeler_policy_actions, k))
                    .collect();
                let pv: Vec<f32> = if (hi as usize) * 4 <= rec.labeler_policy_values.len() {
                    (lo as usize..hi as usize)
                        .map(|k| f32_at(&rec.labeler_policy_values, k))
                        .collect()
                } else {
                    Vec::new()
                };
                (Some(pa), Some(pv))
            } else {
                (None, None)
            }
        };

        step_views.push(ReplayStepView {
            ply,
            player: m.player.clone(),
            action: action,
            from: m.from as i64,
            to: m.to as i64,
            kind: kind.as_str().to_string(),
            is_chance: m.is_chance,
            revealed: m.revealed.clone().unwrap_or_default(),
            captured: m.captured.clone().unwrap_or_default(),
            move_counter: move_counter as i64,
            scores: [scores[0] as i64, scores[1] as i64],
            board: board.clone(),
            revealed_at: revealed_at.clone(),
            hidden_bag: HiddenBagView {
                red: hidden_bag.red.map(i64::from),
                black: hidden_bag.black.map(i64::from),
            },
            board_source: m.board_source.clone(),
            producer,
            search_value: f32_at(&rec.value_labels, ply),
            player_value: f32_at(&rec.own_values, ply),
            hidden_count: u32_at(&rec.hidden_counts, ply) as i64,
            search_depth: u8_at(&rec.search_depths, ply) as i64,
            search_nodes: u32_at(&rec.search_nodes, ply) as i64,
            chosen_value_gap: f32_at(&rec.chosen_value_gaps, ply),
            expl_random: u8_at(&rec.expl_random, ply) == 1,
            labeler_fallback: u8_at(&rec.labeler_fallbacks, ply) == 1,
            labeler_action,
            policy_actions,
            policy_values,
        });
    }
    if labeler_total > 0 {
        labeler_agree /= labeler_total as f64;
    }


    let red_is_a = crate::handlers_replay::red_is_a_of(rec);
    Ok(ReplayGameView {
        index,
        steps,
        game_length: steps,
        winner: rec.winner,
        termination: crate::handlers_replay::termination_label(rec.termination_reason).to_string(),
        seed: rec.game_seed.map(|s| s.to_string()).unwrap_or_default(),
        has_seed: rec.game_seed.is_some(),
        side_a: crate::handlers_replay::describe_side(&ep.network_sha, ""),
        side_b: crate::handlers_replay::describe_side(&ep.opponent_sha, &ep.opponent_spec),
        red_is_a,
        a_side_only: false,
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
        labeler_agree,
        labeler_total,
        expl_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bq_env::DarkChessEnv;
    use bq_env::GameEnv;
    use prost::Message;

    fn synth_record(steps: usize, with_diags: bool) -> (GameRecord, Vec<Vec<u8>>) {
        let mut env = DarkChessEnv::default();

        let mut initial_layout: Vec<u8> = Vec::with_capacity(32);
        for player in [0u8, 1u8] {
            initial_layout.extend(std::iter::repeat_n(player << 3, 5));
            for ty in 1..=6 {
                let count = if ty == 6 { 1 } else { 2 };
                initial_layout.extend(std::iter::repeat_n(player << 3 | ty, count));
            }
        }
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
        let mut labeler_actions = Vec::new();
        let mut labeler_fallbacks = Vec::new();
        let mut pol_actions = Vec::new();
        let mut pol_values = Vec::new();
        let mut pol_offsets: Vec<u32> = vec![0];

        for i in 0..steps {
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
                labeler_actions.extend_from_slice(&(action as u32).to_le_bytes());
                labeler_fallbacks.push(0u8);
                pol_actions.extend_from_slice(&(action as u32).to_le_bytes());
                pol_values.extend_from_slice(&1.0f32.to_le_bytes());
                pol_offsets.push((i as u32) + 1);
            }
            env.step_with_reveal(action, None).unwrap();
        }
        let rec = GameRecord {
            steps: actions.len() as u32 / 2,
            game_seed: Some(42),
            initial_layout: initial_layout.clone(),
            players,
            actions,
            snapshots,
            winner: Some(1),
            termination_reason: 1,
            producers,
            value_labels,
            own_values,
            hidden_counts,
            search_depths,
            search_nodes,
            chosen_value_gaps: chosen_gaps,
            expl_random,
            labeler_actions,
            labeler_fallbacks,
            labeler_policy_actions: pol_actions,
            labeler_policy_values: pol_values,
            labeler_policy_offsets: pol_offsets.iter().flat_map(|o| o.to_le_bytes()).collect(),
            ..Default::default()
        };
        let snaps = rec.snapshots.clone();
        (rec, snaps)
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
        }
    }

    fn batch(rec: GameRecord) -> EpisodeBatch {
        EpisodeBatch {
            variant: "4x8".into(),
            kind: 1,
            records: vec![rec],
            ..Default::default()
        }
    }

    #[test]
    fn full_conversion_snapshot_path() {
        let (rec, _) = synth_record(8, true);
        let g = build_game(&batch(rec), &episode(), 0).expect("完整转换应成功");
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
        assert_eq!(s.search_value, 0.25);
        assert_eq!(s.player_value, -0.5);
        assert_eq!(s.search_depth, 4);
        assert_eq!(s.search_nodes, 1024);
        assert_eq!(s.producer, 0);
        assert_eq!(s.labeler_action, Some(s.action));
        assert_eq!(s.policy_actions.as_deref(), Some(&[s.action][..]));
        assert_eq!(s.policy_values.as_deref(), Some(&[1.0][..]));

        assert_eq!(g.labeler_total, 8);
        assert!((g.labeler_agree - 1.0).abs() < 1e-9);

        assert_eq!(g.final_shown.len(), 32);
    }

    #[test]
    fn missing_diagnostics_default_to_zero() {
        let (mut rec, _) = synth_record(3, false);
        rec.labeler_actions = vec![0xFF; 12];
        let g = build_game(&batch(rec), &episode(), 0).unwrap();
        for m in &g.moves {
            assert_eq!(m.search_value, 0.0);
            assert_eq!(m.search_depth, 0);
            assert_eq!(m.labeler_action, None);
            assert!(m.policy_actions.is_none());
            assert!(m.policy_values.is_none());
        }
        assert_eq!(g.labeler_total, 0);
        assert_eq!(g.labeler_agree, 0.0);
    }

    #[test]
    fn index_out_of_range_message() {
        let (rec, _) = synth_record(2, false);
        let err = build_game(&batch(rec), &episode(), 3).unwrap_err();
        assert!(err.contains("超出范围"), "{err}");
    }


    #[test]
    fn unseeded_report_replays_without_identity() {
        let mut env = DarkChessEnv::default();
        let mut rec = GameRecord::default();
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
            rec.snapshots.push(env.to_snapshot().encode());
            rec.actions
                .extend_from_slice(&(action as u16).to_le_bytes());
            rec.players.push(if env.get_current_player() == Player::Red {
                1u8
            } else {
                0xFF
            });
            env.step_with_reveal(action, None).unwrap();
        }
        rec.steps = rec.players.len() as u32;
        assert!(rec.steps >= 2, "合成翻子对局应至少 2 步");

        let g = build_game(&batch(rec), &episode(), 0).expect("无 seed 上报也应可回放");
        assert_eq!(g.moves.len(), g.steps);
        let pre_revealed = env.config.initial_revealed_pieces;
        assert_eq!(
            g.initial_shown.iter().filter(|&&s| s).count(),
            pre_revealed,
            "开局明子数应与 initial_revealed_pieces 一致"
        );

        let flips: Vec<_> = g
            .moves
            .iter()
            .filter(|m| m.kind == "flip")
            .collect();
        assert!(!flips.is_empty(), "合成对局应含翻子步");
    }

    #[test]
    fn missing_snapshot_rejects_whole_game() {
        let (mut rec, _) = synth_record(4, false);
        rec.snapshots.truncate(2);
        let err = build_game(&batch(rec), &episode(), 0).unwrap_err();
        assert!(err.contains("步前快照"), "{err}");
    }

    #[test]
    fn unknown_variant_rejected() {
        let (rec, _) = synth_record(2, false);
        let mut b = batch(rec);
        b.variant = "9x9".into();
        let err = build_game(&b, &episode(), 0).unwrap_err();
        assert!(err.contains("未知变体"), "{err}");
    }

    #[test]
    fn proto_roundtrip_preserves_fields() {
        let (rec, _) = synth_record(5, true);
        let b = batch(rec);
        let bytes = b.encode_to_vec();
        let b2 = EpisodeBatch::decode(bytes.as_slice()).unwrap();
        let g = build_game(&b2, &episode(), 0).unwrap();
        assert_eq!(g.steps, 5);
        assert_eq!(g.moves[2].search_value, 0.25);
    }

    impl ReplayGameView {
        fn board_source_of_first(&self) -> &str {
            &self.moves[0].board_source
        }
    }
}
