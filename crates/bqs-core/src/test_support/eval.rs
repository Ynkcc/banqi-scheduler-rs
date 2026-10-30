use std::sync::Mutex;

use crate::{
    CoreError, CoreResult,
    ports::{EvalStore, SettingsStore},
    types::{EvalDelta, EvalQueueEntry, EvalResult},
};

use super::settings::FakeSettings;

#[derive(Debug, Default)]
struct EvalInner {
    queue: Vec<EvalQueueEntry>,
    results: Vec<EvalResult>,
    next_result_id: i64,
}

#[derive(Debug, Default)]
pub struct FakeEval {
    settings: FakeSettings,
    inner: Mutex<EvalInner>,
}

impl FakeEval {
    fn find_entry(&self, sha: &str, spec: &str) -> Option<EvalQueueEntry> {
        self.inner
            .lock()
            .unwrap()
            .queue
            .iter()
            .find(|e| e.network_sha == sha && e.opponent_spec == spec)
            .cloned()
    }

    pub fn load_eval_queue(&self) -> CoreResult<Vec<EvalQueueEntry>> {
        EvalStore::load_eval_queue(self)
    }

    pub fn enqueue_eval_job(
        &self,
        network_sha: &str,
        spec: &str,
        opponent_sha: &str,
        target_games: i64,
        per_task_games: i64,
    ) -> CoreResult<bool> {
        EvalStore::enqueue_eval_job(
            self,
            network_sha,
            spec,
            opponent_sha,
            target_games,
            per_task_games,
        )
    }

    pub fn delete_eval_job(&self, network_sha: &str, spec: &str) -> CoreResult<()> {
        EvalStore::delete_eval_job(self, network_sha, spec)
    }

    pub fn inc_eval_job_attempts(&self, network_sha: &str, spec: &str) -> CoreResult<i64> {
        EvalStore::inc_eval_job_attempts(self, network_sha, spec)
    }

    pub fn accumulate_eval_job(
        &self,
        network_sha: &str,
        spec: &str,
        delta: EvalDelta,
    ) -> CoreResult<EvalQueueEntry> {
        EvalStore::accumulate_eval_job(self, network_sha, spec, delta)
    }

    pub fn get_eval_job(
        &self,
        network_sha: &str,
        spec: &str,
    ) -> CoreResult<Option<EvalQueueEntry>> {
        EvalStore::get_eval_job(self, network_sha, spec)
    }

    pub fn upsert_eval_result(&self, r: EvalResult) -> CoreResult<()> {
        EvalStore::upsert_eval_result(self, r)
    }

    pub fn get_eval_result(
        &self,
        network_sha: &str,
        opponent_spec: &str,
    ) -> CoreResult<Option<EvalResult>> {
        EvalStore::get_eval_result(self, network_sha, opponent_spec)
    }

    pub fn eval_trend(&self, opponent_spec: &str, limit: i64) -> CoreResult<Vec<EvalResult>> {
        EvalStore::eval_trend(self, opponent_spec, limit)
    }

    pub fn get_setting(&self, key: &str) -> CoreResult<Option<String>> {
        SettingsStore::get_setting(self, key)
    }

    pub fn set_setting(&self, key: &str, value: &str) -> CoreResult<()> {
        SettingsStore::set_setting(self, key, value)
    }

    pub fn get_int_setting(&self, key: &str, default: i64) -> CoreResult<i64> {
        SettingsStore::get_int_setting(self, key, default)
    }

    pub fn inc_setting(&self, key: &str) -> CoreResult<i64> {
        SettingsStore::inc_setting(self, key)
    }
}

impl SettingsStore for FakeEval {
    fn get_setting(&self, key: &str) -> CoreResult<Option<String>> {
        self.settings.get_setting(key)
    }

    fn get_int_setting(&self, key: &str, default: i64) -> CoreResult<i64> {
        self.settings.get_int_setting(key, default)
    }

    fn set_setting(&self, key: &str, value: &str) -> CoreResult<()> {
        self.settings.set_setting(key, value)
    }

    fn inc_setting(&self, key: &str) -> CoreResult<i64> {
        self.settings.inc_setting(key)
    }
}

impl EvalStore for FakeEval {
    fn load_eval_queue(&self) -> CoreResult<Vec<EvalQueueEntry>> {
        Ok(self.inner.lock().unwrap().queue.clone())
    }

    fn enqueue_eval_job(
        &self,
        network_sha: &str,
        spec: &str,
        opponent_sha: &str,
        target_games: i64,
        per_task_games: i64,
    ) -> CoreResult<bool> {
        let mut g = self.inner.lock().unwrap();
        if g.queue
            .iter()
            .any(|e| e.network_sha == network_sha && e.opponent_spec == spec)
        {
            return Ok(false);
        }
        g.queue.push(EvalQueueEntry {
            network_sha: network_sha.to_string(),
            opponent_spec: spec.to_string(),
            opponent_sha: opponent_sha.to_string(),
            target_games,
            per_task_games,
            attempts: 0,
            wins: 0,
            draws: 0,
            losses: 0,
            num_games: 0,
            created_at: 0,
            red_wins: 0,
            black_wins: 0,
            draw_no_capture: 0,
            draw_truncation: 0,
        });
        Ok(true)
    }

    fn delete_eval_job(&self, network_sha: &str, spec: &str) -> CoreResult<()> {
        self.inner
            .lock()
            .unwrap()
            .queue
            .retain(|e| !(e.network_sha == network_sha && e.opponent_spec == spec));
        Ok(())
    }

    fn inc_eval_job_attempts(&self, network_sha: &str, spec: &str) -> CoreResult<i64> {
        let mut g = self.inner.lock().unwrap();
        match g
            .queue
            .iter_mut()
            .find(|e| e.network_sha == network_sha && e.opponent_spec == spec)
        {
            Some(e) => {
                e.attempts += 1;
                Ok(e.attempts)
            }
            None => Err(CoreError::Store(format!(
                "eval job {network_sha}/{spec} 不存在"
            ))),
        }
    }

    fn accumulate_eval_job(
        &self,
        network_sha: &str,
        spec: &str,
        delta: EvalDelta,
    ) -> CoreResult<EvalQueueEntry> {
        let mut g = self.inner.lock().unwrap();
        let Some(e) = g
            .queue
            .iter_mut()
            .find(|e| e.network_sha == network_sha && e.opponent_spec == spec)
        else {
            return Err(CoreError::Store(format!(
                "eval job {network_sha}/{spec} 不存在（已收尾或已丢弃）"
            )));
        };
        e.wins += delta.wins;
        e.draws += delta.draws;
        e.losses += delta.losses;
        e.num_games += delta.games;
        e.red_wins += delta.red_wins;
        e.black_wins += delta.black_wins;
        e.draw_no_capture += delta.draw_no_capture;
        e.draw_truncation += delta.draw_truncation;
        if delta.games > 0 {
            e.attempts = 0;
        }
        Ok(e.clone())
    }

    fn get_eval_job(&self, network_sha: &str, spec: &str) -> CoreResult<Option<EvalQueueEntry>> {
        Ok(self.find_entry(network_sha, spec))
    }

    fn upsert_eval_result(&self, r: EvalResult) -> CoreResult<()> {
        let mut g = self.inner.lock().unwrap();
        match g
            .results
            .iter_mut()
            .find(|x| x.network_sha == r.network_sha && x.opponent_spec == r.opponent_spec)
        {
            Some(old) => {
                if r.num_games >= old.num_games {
                    *old = r;
                }
            }
            None => {
                g.next_result_id += 1;
                let mut r = r;
                r.id = g.next_result_id;
                g.results.push(r);
            }
        }
        Ok(())
    }

    fn get_eval_result(
        &self,
        network_sha: &str,
        opponent_spec: &str,
    ) -> CoreResult<Option<EvalResult>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .results
            .iter()
            .find(|x| x.network_sha == network_sha && x.opponent_spec == opponent_spec)
            .cloned())
    }

    fn eval_trend(&self, opponent_spec: &str, limit: i64) -> CoreResult<Vec<EvalResult>> {
        let g = self.inner.lock().unwrap();
        let mut rows: Vec<EvalResult> = g
            .results
            .iter()
            .filter(|x| x.opponent_spec == opponent_spec)
            .cloned()
            .collect();

        rows.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        let skip = (rows.len() as i64 - limit).max(0) as usize;
        Ok(rows.into_iter().skip(skip).collect())
    }
}
