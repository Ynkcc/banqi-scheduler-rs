use super::EvalCoordinator;

impl EvalCoordinator {
    pub(super) fn eval_enabled(&self) -> bool {
        match self.ctl.eval_override() {
            Some(o) if o.enabled.is_some() => o.enabled.unwrap(),
            _ => self.cfg.eval.enabled,
        }
    }

    pub(super) fn eval_opponents(&self) -> Vec<String> {
        match self.ctl.eval_override() {
            Some(o) if o.opponents.is_some() => o.opponents.clone().unwrap(),
            _ => self.cfg.eval.opponents.clone(),
        }
    }

    pub(super) fn eval_games(&self) -> i64 {
        match self.ctl.eval_override() {
            Some(o) if o.games.is_some() => o.games.unwrap(),
            _ => self.cfg.eval.games,
        }
    }

    pub(super) fn eval_no_progress_n(&self) -> i64 {
        match self.ctl.eval_override() {
            Some(o) if o.no_progress_n.is_some() => o.no_progress_n.unwrap(),
            _ => self.cfg.eval.no_progress_n,
        }
    }

    pub(super) fn eval_no_progress_eps(&self) -> f64 {
        match self.ctl.eval_override() {
            Some(o) if o.no_progress_eps.is_some() => o.no_progress_eps.unwrap(),
            _ => self.cfg.eval.no_progress_eps,
        }
    }

    pub(super) fn eval_rollback_eps(&self) -> f64 {
        match self.ctl.eval_override() {
            Some(o) if o.rollback_eps.is_some() => o.rollback_eps.unwrap(),
            _ => self.cfg.eval.rollback_eps,
        }
    }

    pub(super) fn eval_floor_win_rate(&self) -> f64 {
        match self.ctl.eval_override() {
            Some(o) if o.floor_win_rate.is_some() => o.floor_win_rate.unwrap(),
            _ => self.cfg.eval.floor_win_rate,
        }
    }

    pub(super) fn eval_stop_win_rate(&self) -> f64 {
        match self.ctl.eval_override() {
            Some(o) if o.stop_win_rate.is_some() => o.stop_win_rate.unwrap(),
            _ => self.cfg.eval.stop_win_rate,
        }
    }
}
