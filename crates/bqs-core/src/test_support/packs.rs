use std::collections::HashMap;
use std::sync::Mutex;

use crate::{
    CoreResult,
    ports::{FeaturePackDraft, FeaturePackStore},
    types::FeaturePack,
};

#[derive(Debug, Default)]
pub struct FakePacks {
    map: Mutex<HashMap<(String, i64), FeaturePack>>,
}

impl FakePacks {
    pub fn count(&self) -> usize {
        self.map.lock().unwrap().len()
    }

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

        map.entry((fp.episode_object_key.clone(), fp.pack_version))
            .or_insert_with(|| FeaturePack {
                id: 0,
                episode_object_key: fp.episode_object_key,
                pack_version: fp.pack_version,
                object_key: fp.object_key,
                created_at: fp.created_at,
            });
        Ok(())
    }

    fn get_feature_pack(
        &self,
        episode_object_key: &str,
        pack_version: i64,
    ) -> CoreResult<Option<FeaturePack>> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .get(&(episode_object_key.to_string(), pack_version))
            .cloned())
    }
}
