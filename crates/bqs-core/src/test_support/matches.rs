use std::collections::HashMap;
use std::sync::Mutex;

use crate::{
    ports::MatchStore,
    types::{match_status, Match},
    CoreError, CoreResult,
};

#[derive(Debug, Default)]
struct MatchesInner {
    next_id: i64,
    matches: HashMap<i64, Match>,
}

#[derive(Debug, Default)]
pub struct FakeMatches {
    inner: Mutex<MatchesInner>,
}

impl FakeMatches {
    pub fn seed_match(&self, candidate: &str, opponent: &str, target_games: i64) -> i64 {
        let mut g = self.inner.lock().unwrap();
        g.next_id += 1;
        let id = g.next_id;
        g.matches.insert(
            id,
            Match {
                id,
                candidate: candidate.to_string(),
                opponent: opponent.to_string(),
                status: match_status::RUNNING.to_string(),
                pairs: bqs_judgment::Hexanomial::ZERO,
                num_games: 0,
                target_games,
            },
        );
        id
    }

    pub fn force_progress(&self, id: i64, num_games: i64) {
        self.inner
            .lock()
            .unwrap()
            .matches
            .get_mut(&id)
            .unwrap()
            .num_games = num_games;
    }

    pub fn force_conclude(&self, id: i64) {
        self.inner
            .lock()
            .unwrap()
            .matches
            .get_mut(&id)
            .unwrap()
            .status = match_status::CONCLUDED.to_string();
    }

    pub fn create_match(
        &self,
        candidate: &str,
        opponent: &str,
        target_games: i64,
    ) -> CoreResult<i64> {
        MatchStore::create_match(self, candidate, opponent, target_games)
    }

    pub fn get_match(&self, id: i64) -> CoreResult<Option<Match>> {
        MatchStore::get_match(self, id)
    }

    pub fn list_matches(&self) -> CoreResult<Vec<Match>> {
        MatchStore::list_matches(self)
    }

    pub fn pending_match(&self) -> CoreResult<Option<Match>> {
        MatchStore::pending_match(self)
    }

    pub fn conclude_match(&self, id: i64) -> CoreResult<bool> {
        MatchStore::conclude_match(self, id)
    }

    pub fn cancel_match(&self, id: i64) -> CoreResult<bool> {
        MatchStore::cancel_match(self, id)
    }
}

impl MatchStore for FakeMatches {
    fn create_match(&self, candidate: &str, opponent: &str, target_games: i64) -> CoreResult<i64> {
        Ok(self.seed_match(candidate, opponent, target_games))
    }

    fn pending_match(&self) -> CoreResult<Option<Match>> {
        let g = self.inner.lock().unwrap();
        Ok(g.matches
            .values()
            .filter(|m| m.running())
            .max_by_key(|m| m.id)
            .cloned())
    }

    fn get_match(&self, id: i64) -> CoreResult<Option<Match>> {
        Ok(self.inner.lock().unwrap().matches.get(&id).cloned())
    }

    fn list_matches(&self) -> CoreResult<Vec<Match>> {
        let mut out: Vec<Match> = self
            .inner
            .lock()
            .unwrap()
            .matches
            .values()
            .cloned()
            .collect();
        out.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(out)
    }

    fn add_match_games(
        &self,
        id: i64,
        delta: bqs_judgment::Hexanomial,
        games: i64,
    ) -> CoreResult<(Match, bool)> {
        let mut g = self.inner.lock().unwrap();
        let Some(m) = g.matches.get_mut(&id) else {
            return Err(CoreError::Store(format!("match {id} 不存在")));
        };
        if !m.running() {
            return Ok((m.clone(), false));
        }
        for i in 0..6 {
            m.pairs.0[i] += delta.0[i];
        }
        m.num_games += games;
        Ok((m.clone(), true))
    }

    fn conclude_match(&self, id: i64) -> CoreResult<bool> {
        let mut g = self.inner.lock().unwrap();
        let Some(m) = g.matches.get_mut(&id) else {
            return Err(CoreError::Store(format!("match {id} 不存在")));
        };
        if !m.running() {
            return Ok(false);
        }
        m.status = match_status::CONCLUDED.to_string();
        Ok(true)
    }

    fn cancel_match(&self, id: i64) -> CoreResult<bool> {
        let mut g = self.inner.lock().unwrap();
        let Some(m) = g.matches.get_mut(&id) else {
            return Err(CoreError::Store(format!("match {id} 不存在")));
        };
        if !m.running() {
            return Ok(false);
        }
        m.status = match_status::CANCELLED.to_string();
        Ok(true)
    }
}
