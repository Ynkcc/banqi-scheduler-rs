//! 【所有权：core-judgment 成员】
//!
//! 评估对手标识词汇表：常量 / 默认阶梯 / 白名单校验。
//!
//! 参考实现：`banqi-scheduler/internal/scheduler/evalopponents.go`。
//! **跨端耦合**：与采集端 `rule_opponents.rs` 同一套标识
//! （`random` / `rule:capture_first` / `rule:reveal_first` / `rule:material_expectimax`），
//! 启动时校验（[`validate_eval_opponents`]），写错即失败。

/// 随机对手。
pub const EVAL_OPPONENT_RANDOM: &str = "random";
/// 优先吃子启发式（暴露「练了很久却打不过 3 行启发式」的那个对手）。
pub const EVAL_OPPONENT_CAPTURE_FIRST: &str = "rule:capture_first";
/// 优先翻棋启发式。
pub const EVAL_OPPONENT_REVEAL_FIRST: &str = "rule:reveal_first";
/// Expectimax + 规则材料评估器（最强标尺；[selfplay] best = "rule" 哨兵的下发形态）。
pub const EVAL_OPPONENT_MATERIAL_EXPECTIMAX: &str = "rule:material_expectimax";

/// 默认阶梯（由弱到强）：随机 → 优先翻棋 → 优先吃子 → Expectimax+规则评估器。
pub fn default_eval_opponents() -> Vec<String> {
    vec![
        EVAL_OPPONENT_RANDOM.to_string(),
        EVAL_OPPONENT_REVEAL_FIRST.to_string(),
        EVAL_OPPONENT_CAPTURE_FIRST.to_string(),
        EVAL_OPPONENT_MATERIAL_EXPECTIMAX.to_string(),
    ]
}

/// 校验对手阶梯：全部标识合法才通过（启动时调用，写错即失败）。
/// 空白条目跳过（与 Go 版 `ValidateEvalOpponents` 一致，由调用方决定是否过滤）。
pub fn validate_eval_opponents(opponents: &[String]) -> Result<(), String> {
    for spec in opponents {
        let s = spec.trim();
        if s.is_empty() {
            continue;
        }
        match s {
            EVAL_OPPONENT_RANDOM
            | EVAL_OPPONENT_CAPTURE_FIRST
            | EVAL_OPPONENT_REVEAL_FIRST
            | EVAL_OPPONENT_MATERIAL_EXPECTIMAX => {}
            other => {
                return Err(format!(
                    "未知评估对手 {other:?}（可选 {EVAL_OPPONENT_RANDOM} / \
                     {EVAL_OPPONENT_REVEAL_FIRST} / {EVAL_OPPONENT_CAPTURE_FIRST} / \
                     {EVAL_OPPONENT_MATERIAL_EXPECTIMAX}）"
                ))
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_builtin_opponents_valid() {
        let ladder = default_eval_opponents();
        assert_eq!(ladder.len(), 4);
        validate_eval_opponents(&ladder).unwrap();
    }

    #[test]
    fn empty_entries_skipped() {
        let l = vec!["random".to_string(), "  ".to_string(), String::new()];
        validate_eval_opponents(&l).unwrap();
    }

    #[test]
    fn unknown_rejected_with_message() {
        let l = vec!["rule:unknown".to_string()];
        let err = validate_eval_opponents(&l).unwrap_err();
        assert!(err.contains("rule:unknown"), "{err}");
        assert!(err.contains(EVAL_OPPONENT_MATERIAL_EXPECTIMAX), "{err}");
    }
}
