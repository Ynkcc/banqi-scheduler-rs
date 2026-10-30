use std::collections::HashMap;
use std::sync::Mutex;

use crate::{CoreResult, ports::TrainerStore, types::Trainer};

#[derive(Debug, Default)]
pub struct FakeTrainers {
    list: Mutex<Vec<Trainer>>,

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
        Ok((
            beyond.iter().copied().max().unwrap_or(0),
            beyond.len() as i64,
        ))
    }
}
