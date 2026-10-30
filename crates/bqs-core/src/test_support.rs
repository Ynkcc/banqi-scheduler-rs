//! 【所有权：core-judgment 成员】共享内存 fake ports（**仅测试编译**，经 lib.rs
//! 的 `#[cfg(test)]` 门控引入）。
//!
//! 团队约定：`bqs-core` 的子系统单测统一用这里的 fake（避免 pipeline 与
//! core-judgment 各建一套）。pipeline 若需要额外端口 fake，请在本文件追加实现，
//! 不要另建文件。

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use crate::{
    ports::{
        Clock, EpisodeStore, EvalStore, FeaturePackDraft, FeaturePackStore, JobRow, JobStore,
        JobTaskLink, MatchStore, NetworkStore, ObjectSigner, Ports, SettingsStore,
        TrainerRegistryStore, TrainerStore, WorkerStore,
    },
    types::{
        Episode, EpisodeDraft, EvalDelta, EvalQueueEntry, EvalResult, FeaturePack, Match, Network,
        Trainer, Worker,
    },
    CoreError, CoreResult,
};

// ---------------------------------------------------------------------------
// 时钟
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct FakeClock(AtomicI64);

impl FakeClock {
    pub fn new(start: i64) -> Arc<Self> {
        let c = Self::default();
        c.0.store(start, Ordering::SeqCst);
        Arc::new(c)
    }

    pub fn set(&self, t: i64) {
        self.0.store(t, Ordering::SeqCst);
    }

    pub fn advance(&self, d: i64) {
        self.0.fetch_add(d, Ordering::SeqCst);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// settings
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct FakeSettings {
    map: Mutex<HashMap<String, String>>,
}

impl SettingsStore for FakeSettings {
    fn get_setting(&self, key: &str) -> CoreResult<Option<String>> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }

    fn get_int_setting(&self, key: &str, default: i64) -> CoreResult<i64> {
        match self.get_setting(key)? {
            None => Ok(default),
            Some(v) => v
                .parse::<i64>()
                .map_err(|_| CoreError::Store(format!("setting {key}={v:?} 不是整数"))),
        }
    }

    fn set_setting(&self, key: &str, value: &str) -> CoreResult<()> {
        self.map.lock().unwrap().insert(key.to_string(), value.to_string());
        Ok(())
    }

    fn inc_setting(&self, key: &str) -> CoreResult<i64> {
        let mut map = self.map.lock().unwrap();
        let next = map.get(key).and_then(|v| v.parse::<i64>().ok()).unwrap_or(0) + 1;
        map.insert(key.to_string(), next.to_string());
        Ok(next)
    }
}

// ---------------------------------------------------------------------------
// networks
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct NetworksInner {
    nets: HashMap<String, Network>,
    best: Option<String>,
    next_created_at: i64,
}

#[derive(Debug, Default)]
pub struct FakeNetworks {
    inner: Mutex<NetworksInner>,
}

impl FakeNetworks {
    /// 预置一个网络（candidate 状态；不触发晋级）。
    pub fn seed_network(&self, sha: &str, parent: Option<&str>, format: &str) {
        let mut g = self.inner.lock().unwrap();
        g.next_created_at += 1;
        let created = g.next_created_at;
        g.nets.insert(
            sha.to_string(),
            Network {
                sha: sha.to_string(),
                parent_sha: parent.map(|p| p.to_string()),
                created_at: created,
                is_best: false,
                status: crate::types::network_status::CANDIDATE.to_string(),
                notes: None,
                format: format.to_string(),
            },
        );
    }

    pub fn best_sha(&self) -> Option<String> {
        self.inner.lock().unwrap().best.clone()
    }

    pub fn status_of(&self, sha: &str) -> Option<String> {
        self.inner.lock().unwrap().nets.get(sha).map(|n| n.status.clone())
    }

    pub fn contains(&self, sha: &str) -> bool {
        self.inner.lock().unwrap().nets.contains_key(sha)
    }

    // —— 固有转发方法（测试无需 `use crate::ports::NetworkStore` 即可调用）——

    pub fn get_best(&self) -> CoreResult<Option<Network>> {
        NetworkStore::get_best(self)
    }

    pub fn get_network(&self, sha: &str) -> CoreResult<Option<Network>> {
        NetworkStore::get_network(self, sha)
    }

    pub fn promote_best(&self, sha: &str) -> CoreResult<()> {
        NetworkStore::promote_best(self, sha)
    }

    pub fn update_network_status(&self, sha: &str, status: &str) -> CoreResult<()> {
        NetworkStore::update_network_status(self, sha, status)
    }

    pub fn register_network(
        &self,
        sha: &str,
        parent_sha: Option<&str>,
        notes: Option<&str>,
        format: &str,
    ) -> CoreResult<bool> {
        NetworkStore::register_network(self, sha, parent_sha, notes, format)
    }
}

impl NetworkStore for FakeNetworks {
    fn get_best(&self) -> CoreResult<Option<Network>> {
        let g = self.inner.lock().unwrap();
        Ok(g.best.as_ref().and_then(|s| g.nets.get(s).cloned()))
    }

    fn get_network(&self, sha: &str) -> CoreResult<Option<Network>> {
        Ok(self.inner.lock().unwrap().nets.get(sha).cloned())
    }

    fn latest_network_by_format(&self, format: &str) -> CoreResult<Option<Network>> {
        let g = self.inner.lock().unwrap();
        Ok(g.nets
            .values()
            .filter(|n| n.format == format)
            .max_by_key(|n| n.created_at)
            .cloned())
    }

    fn list_networks(&self) -> CoreResult<Vec<Network>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<Network> = g.nets.values().cloned().collect();
        out.sort_by(|a, b| {
            b.is_best
                .cmp(&a.is_best)
                .then_with(|| a.created_at.cmp(&b.created_at))
        });
        Ok(out)
    }

    fn register_network(
        &self,
        sha: &str,
        parent_sha: Option<&str>,
        notes: Option<&str>,
        format: &str,
    ) -> CoreResult<bool> {
        let mut g = self.inner.lock().unwrap();
        if g.nets.contains_key(sha) {
            return Ok(false);
        }
        g.next_created_at += 1;
        let created = g.next_created_at;
        g.nets.insert(
            sha.to_string(),
            Network {
                sha: sha.to_string(),
                parent_sha: parent_sha.map(|p| p.to_string()),
                created_at: created,
                is_best: false,
                status: crate::types::network_status::CANDIDATE.to_string(),
                notes: notes.map(|s| s.to_string()),
                format: format.to_string(),
            },
        );
        Ok(true)
    }

    fn promote_best(&self, sha: &str) -> CoreResult<()> {
        let mut g = self.inner.lock().unwrap();
        if !g.nets.contains_key(sha) {
            return Err(CoreError::Store(format!("promote: network {sha} 不存在")));
        }
        if let Some(old) = g.best.take() {
            if let Some(n) = g.nets.get_mut(&old) {
                n.is_best = false;
                n.status = crate::types::network_status::ARCHIVED.to_string();
            }
        }
        if let Some(n) = g.nets.get_mut(sha) {
            n.is_best = true;
            n.status = crate::types::network_status::BEST.to_string();
        }
        g.best = Some(sha.to_string());
        Ok(())
    }

    fn update_network_status(&self, sha: &str, status: &str) -> CoreResult<()> {
        let mut g = self.inner.lock().unwrap();
        match g.nets.get_mut(sha) {
            Some(n) => {
                n.status = status.to_string();
                n.is_best = false;
                Ok(())
            }
            None => Err(CoreError::Store(format!("network {sha} 不存在"))),
        }
    }
}

// ---------------------------------------------------------------------------
// matches
// ---------------------------------------------------------------------------

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
    /// 预置一个 running 对打，返回 id。
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
                status: crate::types::match_status::RUNNING.to_string(),
                pairs: bqs_judgment::Hexanomial::ZERO,
                num_games: 0,
                target_games,
            },
        );
        id
    }

    /// 直接改写累计局数（绕过聚合，模拟历史进度）。
    pub fn force_progress(&self, id: i64, num_games: i64) {
        self.inner.lock().unwrap().matches.get_mut(&id).unwrap().num_games = num_games;
    }

    /// 直接结转（模拟并发赢家已完成的 CAS）。
    pub fn force_conclude(&self, id: i64) {
        self.inner
            .lock()
            .unwrap()
            .matches
            .get_mut(&id)
            .unwrap()
            .status = crate::types::match_status::CONCLUDED.to_string();
    }

    // —— 固有转发方法（测试无需 `use crate::ports::MatchStore` 即可调用）——

    pub fn create_match(&self, candidate: &str, opponent: &str, target_games: i64) -> CoreResult<i64> {
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
        let mut out: Vec<Match> = self.inner.lock().unwrap().matches.values().cloned().collect();
        out.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(out)
    }

    fn add_match_games(&self, id: i64, delta: bqs_judgment::Hexanomial, games: i64) -> CoreResult<(Match, bool)> {
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
        m.status = crate::types::match_status::CONCLUDED.to_string();
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
        m.status = crate::types::match_status::CANCELLED.to_string();
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// eval（含自带 settings：EvalStore 的 supertrait 是 SettingsStore）
// ---------------------------------------------------------------------------

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

    // —— 固有转发方法（测试无需 `use crate::ports::EvalStore` 即可调用）——

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
        EvalStore::enqueue_eval_job(self, network_sha, spec, opponent_sha, target_games, per_task_games)
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

    pub fn get_eval_job(&self, network_sha: &str, spec: &str) -> CoreResult<Option<EvalQueueEntry>> {
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

    fn accumulate_eval_job(&self, network_sha: &str, spec: &str, delta: EvalDelta) -> CoreResult<EvalQueueEntry> {
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
            e.attempts = 0; // 有进度即健康
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
                // UNIQUE 覆盖 + 部分结果不劣化完整结果
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

    fn get_eval_result(&self, network_sha: &str, opponent_spec: &str) -> CoreResult<Option<EvalResult>> {
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
        // 升序：created_at 优先，id 兜底
        rows.sort_by(|a, b| a.created_at.cmp(&b.created_at).then_with(|| a.id.cmp(&b.id)));
        let skip = (rows.len() as i64 - limit).max(0) as usize;
        Ok(rows.into_iter().skip(skip).collect())
    }
}

// ---------------------------------------------------------------------------
// trainers（权威积压口径）+ trainer registry + episodes
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct FakeTrainers {
    list: Mutex<Vec<Trainer>>,
    /// kind → 该类别已登记 episode 的 id 序列（kind_watermark 权威积压口径）
    episodes_by_kind: Mutex<HashMap<i64, Vec<i64>>>,
}

impl FakeTrainers {
    pub fn set_trainer(&self, t: Trainer) {
        let mut g = self.list.lock().unwrap();
        match g.iter_mut().find(|x| x.id == t.id) {
            Some(slot) => *slot = t,
            None => g.push(t),
        }
    }

    pub fn set_episode_ids(&self, kind: i64, ids: Vec<i64>) {
        self.episodes_by_kind.lock().unwrap().insert(kind, ids);
    }
}

impl TrainerStore for FakeTrainers {
    fn list_trainers(&self) -> CoreResult<Vec<Trainer>> {
        Ok(self.list.lock().unwrap().clone())
    }

    fn kind_watermark(&self, kind: i64, after_id: i64) -> CoreResult<(i64, i64)> {
        let g = self.episodes_by_kind.lock().unwrap();
        let ids = g.get(&kind).map(|v| v.as_slice()).unwrap_or(&[]);
        let beyond: Vec<i64> = ids.iter().copied().filter(|i| *i > after_id).collect();
        Ok((beyond.iter().copied().max().unwrap_or(0), beyond.len() as i64))
    }
}

#[derive(Debug, Default)]
pub struct FakeRegistry {
    map: Mutex<HashMap<String, Trainer>>,
    /// 与 [`FakeTrainers`] 的镜像（真实实现中两者是同一张 trainers 表）：
    /// 心跳登记同步写入权威积压视图，心跳路径的即时重估才能看到新水位。
    mirror: Mutex<Option<Arc<FakeTrainers>>>,
}

impl FakeRegistry {
    pub fn set_mirror(&self, t: Arc<FakeTrainers>) {
        *self.mirror.lock().unwrap() = Some(t);
    }

    // —— 固有转发方法（测试无需 `use crate::ports::TrainerRegistryStore` 即可调用）——

    pub fn get_trainer(&self, id: &str) -> CoreResult<Option<Trainer>> {
        TrainerRegistryStore::get_trainer(self, id)
    }

    pub fn touch_trainer(&self, t: Trainer) -> CoreResult<()> {
        TrainerRegistryStore::touch_trainer(self, t)
    }
}

impl TrainerRegistryStore for FakeRegistry {
    fn get_trainer(&self, id: &str) -> CoreResult<Option<Trainer>> {
        Ok(self.map.lock().unwrap().get(id).cloned())
    }

    fn touch_trainer(&self, t: Trainer) -> CoreResult<()> {
        self.map.lock().unwrap().insert(t.id.clone(), t.clone());
        if let Some(m) = self.mirror.lock().unwrap().as_ref() {
            m.set_trainer(t);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct EpisodesInner {
    next_id: i64,
    list: Vec<Episode>,
}

#[derive(Debug, Default)]
pub struct FakeEpisodes {
    inner: Mutex<EpisodesInner>,
}

impl FakeEpisodes {
    pub fn seed_episode(&self, id: i64, object_key: &str, kind: i64) {
        let mut g = self.inner.lock().unwrap();
        g.next_id = g.next_id.max(id);
        g.list.push(Episode {
            id,
            worker_id: String::new(),
            task_id: String::new(),
            network_sha: String::new(),
            game_count: 0,
            total_steps: 0,
            winner: 0,
            object_key: object_key.to_string(),
            created_at: 0,
            kind,
            opponent_sha: String::new(),
            opponent_spec: String::new(),
            match_id: 0,
            red_wins: 0,
            black_wins: 0,
            draw_no_capture: 0,
            draw_truncation: 0,
        });
    }

    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().list.len()
    }

    pub fn by_task(&self, task_id: &str) -> Option<Episode> {
        self.inner.lock().unwrap().list.iter().find(|e| e.task_id == task_id).cloned()
    }
}

impl EpisodeStore for FakeEpisodes {
    fn get_episode_by_task(&self, task_id: &str) -> CoreResult<Option<Episode>> {
        Ok(self.inner.lock().unwrap().list.iter().find(|e| e.task_id == task_id).cloned())
    }

    fn insert_episode(&self, draft: EpisodeDraft) -> CoreResult<i64> {
        let mut g = self.inner.lock().unwrap();
        if g.list.iter().any(|e| !draft.task_id.is_empty() && e.task_id == draft.task_id) {
            return Err(CoreError::Store(format!("duplicate task_id {}", draft.task_id)));
        }
        g.next_id += 1;
        let id = g.next_id;
        g.list.push(Episode {
            id,
            worker_id: draft.worker_id,
            task_id: draft.task_id,
            network_sha: draft.network_sha,
            game_count: draft.game_count,
            total_steps: draft.total_steps,
            winner: draft.winner,
            object_key: draft.object_key,
            created_at: draft.created_at,
            kind: draft.kind,
            opponent_sha: draft.opponent_sha,
            opponent_spec: draft.opponent_spec,
            match_id: draft.match_id,
            red_wins: draft.red_wins,
            black_wins: draft.black_wins,
            draw_no_capture: draft.draw_no_capture,
            draw_truncation: draft.draw_truncation,
        });
        Ok(id)
    }

    /// 与 Go 版 store.ListEpisodeKeys 语义一致：游标子查询独立于 kind 过滤、
    /// 恒排除 kind=2（eval 回放记录不进消费管道）、limit<=0 取默认 200。
    fn list_episode_keys(&self, after_key: &str, limit: i64, kind: Option<i64>) -> CoreResult<Vec<String>> {
        let g = self.inner.lock().unwrap();
        let mut limit = limit;
        if limit <= 0 || limit > 1000 {
            limit = 200;
        }
        let after_id = g
            .list
            .iter()
            .find(|e| e.object_key == after_key)
            .map(|e| e.id)
            .unwrap_or(0);
        let mut pairs: Vec<(i64, String)> = g
            .list
            .iter()
            .filter(|e| e.id > after_id && e.kind != 2)
            .filter(|e| kind.map_or(true, |k| e.kind == k))
            .map(|e| (e.id, e.object_key.clone()))
            .collect();
        pairs.sort_by_key(|(id, _)| *id);
        Ok(pairs.into_iter().map(|(_, k)| k).take(limit as usize).collect())
    }

    fn episode_id_by_key(&self, key: &str) -> CoreResult<Option<i64>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .list
            .iter()
            .find(|e| e.object_key == key)
            .map(|e| e.id))
    }

    fn list_episodes_page(&self, before_id: i64, limit: i64) -> CoreResult<Vec<Episode>> {
        let mut rows: Vec<Episode> = self
            .inner
            .lock()
            .unwrap()
            .list
            .iter()
            .filter(|e| e.id < before_id)
            .cloned()
            .collect();
        rows.sort_by(|a, b| b.id.cmp(&a.id));
        rows.truncate(limit.max(0) as usize);
        Ok(rows)
    }
}

// ---------------------------------------------------------------------------
// workers / feature packs / signer（pipeline 门面用例需要真实内存实现）
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct FakeWorkers {
    map: Mutex<HashMap<String, Worker>>,
}

impl FakeWorkers {
    pub fn get(&self, id: &str) -> Option<Worker> {
        self.map.lock().unwrap().get(id).cloned()
    }
}

impl WorkerStore for FakeWorkers {
    fn touch_worker(
        &self,
        id: &str,
        threads: i64,
        completed_games: i64,
        client_version: &str,
        memory_mb: i64,
        health_status: &str,
        last_error: &str,
    ) -> CoreResult<()> {
        let mut map = self.map.lock().unwrap();
        let w = map.entry(id.to_string()).or_default();
        w.id = id.to_string();
        w.threads = threads;
        w.completed_games = completed_games;
        w.client_version = client_version.to_string();
        w.memory_mb = memory_mb;
        w.health_status = health_status.to_string();
        w.last_error = last_error.to_string();
        Ok(())
    }

    fn worker_version(&self, id: &str) -> CoreResult<Option<String>> {
        Ok(self.map.lock().unwrap().get(id).map(|w| w.client_version.clone()))
    }

    fn list_workers(&self) -> CoreResult<Vec<Worker>> {
        Ok(self.map.lock().unwrap().values().cloned().collect())
    }

    fn set_worker_note(&self, id: &str, note: &str) -> CoreResult<()> {
        match self.map.lock().unwrap().get_mut(id) {
            Some(w) => {
                w.note = note.to_string();
                Ok(())
            }
            None => Err(CoreError::Store(format!("未知 worker {id}"))),
        }
    }
}

#[derive(Debug, Default)]
pub struct FakePacks {
    map: Mutex<HashMap<(String, i64), FeaturePack>>,
}

impl FakePacks {
    pub fn count(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    // —— 固有转发方法（测试无需 `use crate::ports::FeaturePackStore` 即可调用）——

    pub fn insert_feature_pack(&self, fp: FeaturePackDraft) -> CoreResult<()> {
        FeaturePackStore::insert_feature_pack(self, fp)
    }

    pub fn get_feature_pack(
        &self,
        episode_object_key: &str,
        pack_version: i64,
    ) -> CoreResult<Option<FeaturePack>> {
        FeaturePackStore::get_feature_pack(self, episode_object_key, pack_version)
    }
}

impl FeaturePackStore for FakePacks {
    fn insert_feature_pack(&self, fp: FeaturePackDraft) -> CoreResult<()> {
        let mut map = self.map.lock().unwrap();
        // ON CONFLICT DO NOTHING（保留首行；R2 对象内容由 sha256 校验，重复上传无害）
        map.entry((fp.episode_object_key.clone(), fp.pack_version)).or_insert_with(|| FeaturePack {
            id: 0,
            episode_object_key: fp.episode_object_key,
            pack_version: fp.pack_version,
            object_key: fp.object_key,
            created_at: fp.created_at,
        });
        Ok(())
    }

    fn get_feature_pack(&self, episode_object_key: &str, pack_version: i64) -> CoreResult<Option<FeaturePack>> {
        Ok(self.map.lock().unwrap().get(&(episode_object_key.to_string(), pack_version)).cloned())
    }
}

// ---------------------------------------------------------------------------
// jobs（统一 Job 机制）
// ---------------------------------------------------------------------------

/// jobs 表内存 fake（完整实现，供 JobManager 单测与装配束使用）。
#[derive(Debug, Default)]
pub struct FakeJobs {
    inner: Mutex<FakeJobsInner>,
}

#[derive(Debug, Default)]
struct FakeJobsInner {
    jobs: HashMap<String, JobRow>,
    tasks: HashMap<String, JobTaskLink>,
    next_id: i64,
}

impl JobStore for FakeJobs {
    fn insert_job(&self, row: JobRow) -> CoreResult<()> {
        self.inner.lock().unwrap().jobs.insert(row.job_id.clone(), row);
        Ok(())
    }

    fn get_job(&self, job_id: &str) -> CoreResult<Option<JobRow>> {
        Ok(self.inner.lock().unwrap().jobs.get(job_id).cloned())
    }

    fn list_jobs(&self, state: Option<&str>, kind: Option<i64>) -> CoreResult<Vec<JobRow>> {
        let inner = self.inner.lock().unwrap();
        let mut rows: Vec<JobRow> = inner
            .jobs
            .values()
            .filter(|j| state.is_none_or(|s| j.state == s) && kind.is_none_or(|k| j.kind == k))
            .cloned()
            .collect();
        rows.sort_by(|a, b| b.priority.cmp(&a.priority).then(a.created_at.cmp(&b.created_at)));
        Ok(rows)
    }

    fn update_job_state(&self, job_id: &str, state: &str, now: i64) -> CoreResult<bool> {
        let mut inner = self.inner.lock().unwrap();
        match inner.jobs.get_mut(job_id) {
            Some(j) => {
                j.state = state.to_string();
                j.updated_at = now;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn incr_job_dispatched(&self, job_id: &str, games: i64, now: i64) -> CoreResult<JobRow> {
        let mut inner = self.inner.lock().unwrap();
        let j = inner
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| CoreError::Store(format!("job {job_id} not found")))?;
        j.dispatched_games += games;
        j.next_seq += 1;
        j.updated_at = now;
        Ok(j.clone())
    }

    fn incr_job_completed(&self, job_id: &str, games: i64, now: i64) -> CoreResult<JobRow> {
        let mut inner = self.inner.lock().unwrap();
        let j = inner
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| CoreError::Store(format!("job {job_id} not found")))?;
        j.completed_games += games;
        j.updated_at = now;
        Ok(j.clone())
    }

    fn insert_job_task(&self, task_id: &str, job_id: &str, games: i64) -> CoreResult<()> {
        self.inner.lock().unwrap().tasks.insert(
            task_id.to_string(),
            JobTaskLink { task_id: task_id.to_string(), job_id: job_id.to_string(), games },
        );
        Ok(())
    }

    fn get_job_task(&self, task_id: &str) -> CoreResult<Option<JobTaskLink>> {
        Ok(self.inner.lock().unwrap().tasks.get(task_id).cloned())
    }

    fn delete_job_task(&self, task_id: &str) -> CoreResult<()> {
        self.inner.lock().unwrap().tasks.remove(task_id);
        Ok(())
    }

    fn update_job_spec(&self, job_id: &str, spec_json: &str, now: i64) -> CoreResult<()> {
        let mut inner = self.inner.lock().unwrap();
        match inner.jobs.get_mut(job_id) {
            Some(j) => {
                j.spec_json = spec_json.to_string();
                j.updated_at = now;
                Ok(())
            }
            None => Err(CoreError::Store(format!("job {job_id} not found"))),
        }
    }
}

impl FakeJobs {
    /// 预置一个 job 行（测试种子数据）。
    pub fn seed_job(&self, row: JobRow) {
        self.inner.lock().unwrap().jobs.insert(row.job_id.clone(), row);
    }

    /// 自增 id 生成器（测试构造 job_id 用）。
    pub fn next_id(&self) -> i64 {
        let mut inner = self.inner.lock().unwrap();
        inner.next_id += 1;
        inner.next_id
    }
}

/// 预签名 URL fake：`put:<key>` / `get:<key>`（测试断言对象键即可）。
#[derive(Debug, Default)]
pub struct FakeSigner;

impl ObjectSigner for FakeSigner {
    fn presign_put(&self, key: &str, _ttl_secs: i64) -> CoreResult<String> {
        Ok(format!("put:{key}"))
    }

    fn presign_get(&self, key: &str, _ttl_secs: i64) -> CoreResult<String> {
        Ok(format!("get:{key}"))
    }
}

// ---------------------------------------------------------------------------
// 端口束
// ---------------------------------------------------------------------------

/// 全套内存 fake；测试按需取字段（`f.eval` / `f.networks` / ...），
/// 经 [`Fakes::ports`] 组装 `Ports`。
#[derive(Debug, Default)]
pub struct Fakes {
    pub clock: Arc<FakeClock>,
    pub settings: Arc<FakeSettings>,
    pub networks: Arc<FakeNetworks>,
    pub matches: Arc<FakeMatches>,
    pub eval: Arc<FakeEval>,
    pub trainers: Arc<FakeTrainers>,
    pub registry: Arc<FakeRegistry>,
    pub episodes: Arc<FakeEpisodes>,
    pub workers: Arc<FakeWorkers>,
    pub packs: Arc<FakePacks>,
    pub jobs: Arc<FakeJobs>,
    pub signer: Arc<FakeSigner>,
}

impl Fakes {
    pub fn new() -> Self {
        let trainers = Arc::new(FakeTrainers::default());
        let registry = Arc::new(FakeRegistry::default());
        // 真实实现中 TrainerRegistryStore（心跳登记）与 TrainerStore（权威积压）
        // 是同一张 trainers 表：fake 侧做镜像，保证「心跳到达即影响停滞判定」。
        registry.set_mirror(trainers.clone());
        Self {
            clock: FakeClock::new(1_000),
            settings: Arc::new(FakeSettings::default()),
            networks: Arc::new(FakeNetworks::default()),
            matches: Arc::new(FakeMatches::default()),
            eval: Arc::new(FakeEval::default()),
            trainers,
            registry,
            episodes: Arc::new(FakeEpisodes::default()),
            workers: Arc::new(FakeWorkers::default()),
            packs: Arc::new(FakePacks::default()),
            jobs: Arc::new(FakeJobs::default()),
            signer: Arc::new(FakeSigner),
        }
    }

    pub fn ports(&self) -> Ports {
        Ports {
            settings: self.settings.clone(),
            networks: self.networks.clone(),
            matches: self.matches.clone(),
            eval: self.eval.clone(),
            trainers: self.trainers.clone(),
            episodes: self.episodes.clone(),
            workers: self.workers.clone(),
            trainer_registry: self.registry.clone(),
            feature_packs: self.packs.clone(),
            jobs: self.jobs.clone(),
        }
    }
}
