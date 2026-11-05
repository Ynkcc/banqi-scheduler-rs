use std::sync::Arc;

use bqs_judgment::Hexanomial;

use crate::{types::*, CoreResult};

pub trait SettingsStore: Send + Sync {
    fn get_setting(&self, key: &str) -> CoreResult<Option<String>>;

    fn get_int_setting(&self, key: &str, default: i64) -> CoreResult<i64>;
    fn set_setting(&self, key: &str, value: &str) -> CoreResult<()>;

    fn inc_setting(&self, key: &str) -> CoreResult<i64>;
}

pub trait NetworkStore: Send + Sync {
    fn get_network(&self, sha: &str) -> CoreResult<Option<Network>>;

    fn list_networks(&self) -> CoreResult<Vec<Network>>;

    fn register_network(
        &self,
        sha: &str,
        parent_sha: Option<&str>,
        notes: Option<&str>,
        format: &str,
    ) -> CoreResult<bool>;

    fn update_network_status(&self, sha: &str, status: &str) -> CoreResult<()>;
}

pub trait MatchStore: Send + Sync {
    fn create_match(&self, candidate: &str, opponent: &str, target_games: i64) -> CoreResult<i64>;

    fn get_match(&self, id: i64) -> CoreResult<Option<Match>>;

    fn list_matches(&self) -> CoreResult<Vec<Match>>;

    fn add_match_games(&self, id: i64, delta: Hexanomial, games: i64) -> CoreResult<(Match, bool)>;

    fn conclude_match(&self, id: i64) -> CoreResult<bool>;

    fn conclude_match_with_stats(
        &self,
        id: i64,
        conclusion: crate::types::MatchConclusion,
    ) -> CoreResult<bool> {
        let _ = conclusion;
        Err(CoreError::Internal(
            "conclude_match_with_stats 仅 bqs-store::Store 实现（判停结果持久化需求）".into(),
        ))
    }

    fn cancel_match(&self, id: i64) -> CoreResult<bool>;
}

pub trait EpisodeStore: Send + Sync {
    fn get_episode_by_task(&self, task_id: &str) -> CoreResult<Option<Episode>>;
    fn insert_episode(&self, draft: EpisodeDraft) -> CoreResult<i64>;

    fn get_episode_games(&self, episode_id: i64) -> CoreResult<Vec<crate::types::EpisodeGameRow>> {
        let _ = episode_id;
        Ok(Vec::new())
    }

    fn find_games_by_seed(
        &self,
        seed: u64,
    ) -> CoreResult<Vec<(Episode, crate::types::EpisodeGameRow)>> {
        let _ = seed;
        Ok(Vec::new())
    }

    fn list_episode_keys(
        &self,
        after_key: &str,
        limit: i64,
        kind: Option<i64>,
    ) -> CoreResult<Vec<String>>;

    fn episode_id_by_key(&self, key: &str) -> CoreResult<Option<i64>>;

    fn list_episodes_page(&self, before_id: i64, limit: i64) -> CoreResult<Vec<Episode>>;

    fn list_episodes_by_match(
        &self,
        match_id: i64,
        before_id: i64,
        limit: i64,
    ) -> CoreResult<Vec<Episode>> {
        let _ = (match_id, before_id, limit);
        Err(CoreError::Internal(
            "list_episodes_by_match 仅 bqs-store::Store 实现（match 对局反查需求）".into(),
        ))
    }

    fn get_episode(&self, id: i64) -> CoreResult<Option<Episode>> {
        let _ = id;
        Err(CoreError::Internal(
            "get_episode 仅 bqs-store::Store 实现（[replay 需求]）".into(),
        ))
    }

    fn episode_stats_by_worker(&self, window_secs: i64) -> CoreResult<BTreeMap<String, KindStats>> {
        let _ = (self, window_secs);
        Err(CoreError::Internal(
            "episode_stats_by_worker 仅 bqs-store::Store 实现（[api 需求]）".into(),
        ))
    }

    fn episode_rate_estimates(
        &self,
        window_secs: i64,
        samples_per_worker: i64,
    ) -> CoreResult<BTreeMap<String, EpisodeRateEstimate>> {
        let _ = (self, window_secs, samples_per_worker);
        Err(CoreError::Internal(
            "episode_rate_estimates 仅 bqs-store::Store 实现（[api 需求]）".into(),
        ))
    }

    fn episode_data_stats(&self) -> CoreResult<EpisodeDataStats> {
        let _ = self;
        Err(CoreError::Internal(
            "episode_data_stats 仅 bqs-store::Store 实现（[api 需求]）".into(),
        ))
    }

    fn generation_data_stats(&self) -> CoreResult<GenerationStats> {
        let _ = self;
        Err(CoreError::Internal(
            "generation_data_stats 仅 bqs-store::Store 实现（[api 需求]）".into(),
        ))
    }
}

pub trait WorkerStore: Send + Sync {
    fn touch_worker(
        &self,
        id: &str,
        threads: i64,
        completed_games: i64,
        client_version: &str,
        memory_mb: i64,
        health_status: &str,
        last_error: &str,
        ip: &str,
    ) -> CoreResult<()>;

    fn worker_version(&self, id: &str) -> CoreResult<Option<String>>;
    fn list_workers(&self) -> CoreResult<Vec<Worker>>;

    fn set_worker_note(&self, id: &str, note: &str) -> CoreResult<()>;

    fn delete_worker(&self, id: &str) -> CoreResult<bool>;
}

pub trait FeaturePackStore: Send + Sync {
    fn insert_feature_pack(&self, fp: FeaturePackDraft) -> CoreResult<()>;
    fn get_feature_pack(
        &self,
        episode_object_key: &str,
        pack_version: i64,
    ) -> CoreResult<Option<FeaturePack>>;
}

#[derive(Debug, Clone, Default)]
pub struct FeaturePackDraft {
    pub episode_object_key: String,
    pub pack_version: i64,
    pub object_key: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct JobRow {
    pub job_id: String,

    pub kind: i64,
    pub priority: i64,
    pub state: String,

    pub total_games: i64,

    pub completed_games: i64,

    pub dispatched_games: i64,

    pub spec_json: String,
    pub created_at: i64,
    pub updated_at: i64,

    pub next_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobTaskLink {
    pub task_id: String,
    pub job_id: String,
    pub games: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobQuery {
    pub job_id: Option<String>,

    pub state: Option<String>,

    pub kind: Option<i64>,

    pub limit: Option<i64>,

    pub offset: i64,
}

impl JobQuery {
    pub fn all() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct JobPage {
    pub items: Vec<JobRow>,

    pub total: i64,
}

pub trait JobStore: Send + Sync {
    fn insert_job(&self, row: JobRow) -> CoreResult<()>;
    fn get_job(&self, job_id: &str) -> CoreResult<Option<JobRow>>;

    fn list_jobs(&self, query: &JobQuery) -> CoreResult<JobPage>;

    fn update_job_state(&self, job_id: &str, state: &str, now: i64) -> CoreResult<bool>;

    fn incr_job_dispatched(&self, job_id: &str, games: i64, now: i64) -> CoreResult<JobRow>;

    fn incr_job_completed(&self, job_id: &str, games: i64, now: i64) -> CoreResult<JobRow>;
    fn insert_job_task(&self, task_id: &str, job_id: &str, games: i64) -> CoreResult<()>;
    fn get_job_task(&self, task_id: &str) -> CoreResult<Option<JobTaskLink>>;

    fn delete_job_task(&self, task_id: &str) -> CoreResult<()>;

    fn update_job_spec(&self, job_id: &str, spec_json: &str, now: i64) -> CoreResult<()>;
}

pub trait ObjectSigner: Send + Sync {
    fn presign_put(&self, key: &str, ttl_secs: i64) -> CoreResult<String>;

    fn presign_get(&self, key: &str, ttl_secs: i64) -> CoreResult<String>;
}

pub trait Clock: Send + Sync {
    fn now(&self) -> i64;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }
}

pub trait StoreStats: Send + Sync {
    fn counts(&self) -> CoreResult<StoreCounts> {
        let _ = self;
        Err(CoreError::Internal(
            "counts 仅 bqs-store::Store 实现（[api 需求]）".into(),
        ))
    }
}

#[derive(Clone)]
pub struct Ports {
    pub settings: Arc<dyn SettingsStore>,
    pub networks: Arc<dyn NetworkStore>,
    pub matches: Arc<dyn MatchStore>,
    pub stats: Arc<dyn StoreStats>,
    pub episodes: Arc<dyn EpisodeStore>,
    pub workers: Arc<dyn WorkerStore>,
    pub feature_packs: Arc<dyn FeaturePackStore>,
    pub jobs: Arc<dyn JobStore>,
}

use crate::CoreError;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct StoreCounts {
    pub networks: i64,
    pub candidates: i64,
    pub matches_running: i64,
    pub episodes: i64,
    pub episode_games: i64,
    pub workers: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct KindStats {
    pub episodes: i64,
    pub games: i64,
    pub steps: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct EpisodeRateEstimate {
    pub games: i64,
    pub episodes: i64,
    pub span_seconds: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct EpisodeDataStats {
    pub total: BTreeMap<i64, KindStats>,
    pub last_1h: BTreeMap<i64, KindStats>,
    pub last_24h: BTreeMap<i64, KindStats>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct GenerationStats {
    pub since: i64,

    pub candidate: String,
    pub episodes: i64,
    pub games: i64,
    pub steps: i64,
}
