use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{ports::TrainerRegistryStore, types::Trainer, CoreResult};

use super::trainers::FakeTrainers;

#[derive(Debug, Default)]
pub struct FakeRegistry {
    map: Mutex<HashMap<String, Trainer>>,

    mirror: Mutex<Option<Arc<FakeTrainers>>>,
}

impl FakeRegistry {
    pub fn set_mirror(&self, t: Arc<FakeTrainers>) {
        *self.mirror.lock().unwrap() = Some(t);
    }

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
