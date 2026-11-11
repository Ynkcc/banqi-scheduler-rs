use std::collections::HashMap;
use std::sync::Mutex;

use crate::{CoreError, CoreResult, ports::SettingsStore};

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
        self.map
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    fn inc_setting(&self, key: &str) -> CoreResult<i64> {
        let mut map = self.map.lock().unwrap();
        let next = map
            .get(key)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
            + 1;
        map.insert(key.to_string(), next.to_string());
        Ok(next)
    }
}
