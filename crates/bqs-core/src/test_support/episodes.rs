use std::sync::Mutex;

use crate::{
    ports::EpisodeStore,
    types::{Episode, EpisodeDraft},
    CoreError, CoreResult,
};

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
        self.inner
            .lock()
            .unwrap()
            .list
            .iter()
            .find(|e| e.task_id == task_id)
            .cloned()
    }
}

impl EpisodeStore for FakeEpisodes {
    fn get_episode_by_task(&self, task_id: &str) -> CoreResult<Option<Episode>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .list
            .iter()
            .find(|e| e.task_id == task_id)
            .cloned())
    }

    fn insert_episode(&self, draft: EpisodeDraft) -> CoreResult<i64> {
        let mut g = self.inner.lock().unwrap();
        if g.list
            .iter()
            .any(|e| !draft.task_id.is_empty() && e.task_id == draft.task_id)
        {
            return Err(CoreError::Store(format!(
                "duplicate task_id {}",
                draft.task_id
            )));
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

    fn list_episode_keys(
        &self,
        after_key: &str,
        limit: i64,
        kind: Option<i64>,
    ) -> CoreResult<Vec<String>> {
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
        Ok(pairs
            .into_iter()
            .map(|(_, k)| k)
            .take(limit as usize)
            .collect())
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
