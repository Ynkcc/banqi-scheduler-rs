use std::collections::HashMap;
use std::sync::Mutex;

use crate::{ports::WorkerStore, types::Worker, CoreError, CoreResult};

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
        ip: &str,
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
        if !ip.is_empty() {
            w.ip = ip.to_string();
        }
        Ok(())
    }

    fn worker_version(&self, id: &str) -> CoreResult<Option<String>> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .get(id)
            .map(|w| w.client_version.clone()))
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

    fn delete_worker(&self, id: &str) -> CoreResult<bool> {
        Ok(self.map.lock().unwrap().remove(id).is_some())
    }
}
