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

    pub fn get_network(&self, sha: &str) -> CoreResult<Option<Network>> {
        NetworkStore::get_network(self, sha)
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
    fn get_network(&self, sha: &str) -> CoreResult<Option<Network>> {
        Ok(self.inner.lock().unwrap().nets.get(sha).cloned())
    }

    fn list_networks(&self) -> CoreResult<Vec<Network>> {
        let mut out: Vec<Network> = self.inner.lock().unwrap().nets.values().cloned().collect();
        out.sort_by(|a, b| a.created_at.cmp(&b.created_at));
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

    fn update_network_status(&self, sha: &str, status: &str) -> CoreResult<()> {
        let mut g = self.inner.lock().unwrap();
        match g.nets.get_mut(sha) {
            Some(n) => {
                n.status = status.to_string();
                Ok(())
            }
            None => Err(CoreError::Store(format!("network {sha} 不存在"))),
        }
    }
}
