pub const EVAL_OPPONENT_RANDOM: &str = "random";

pub const EVAL_OPPONENT_CAPTURE_FIRST: &str = "rule:capture_first";

pub const EVAL_OPPONENT_REVEAL_FIRST: &str = "rule:reveal_first";

pub const EVAL_OPPONENT_MATERIAL_EXPECTIMAX: &str = "rule:material_expectimax";

pub fn default_eval_opponents() -> Vec<String> {
    vec![
        EVAL_OPPONENT_RANDOM.to_string(),
        EVAL_OPPONENT_REVEAL_FIRST.to_string(),
        EVAL_OPPONENT_CAPTURE_FIRST.to_string(),
        EVAL_OPPONENT_MATERIAL_EXPECTIMAX.to_string(),
    ]
}

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
                ));
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
