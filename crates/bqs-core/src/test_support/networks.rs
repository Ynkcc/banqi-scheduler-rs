use std::collections::HashMap;
use std::sync::Mutex;

use crate::{
    ports::NetworkStore,
    types::{network_status, Network},
    CoreError, CoreResult,
};

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
        self.inner
            .lock()
            .unwrap()
            .nets
            .get(sha)
            .map(|n| n.status.clone())
    }

    pub fn contains(&self, sha: &str) -> bool {
        self.inner.lock().unwrap().nets.contains_key(sha)
    }

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
                status: network_status::CANDIDATE.to_string(),
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
                n.status = network_status::ARCHIVED.to_string();
            }
        }
        if let Some(n) = g.nets.get_mut(sha) {
            n.is_best = true;
            n.status = network_status::BEST.to_string();
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
