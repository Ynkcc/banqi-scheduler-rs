mod episodes;
mod featurepacks;
mod jobs;
#[cfg(test)]
mod jobs_tests;
mod matches;
mod networks;
mod trainers;
mod workers;

use std::sync::{Arc, Mutex};

use bqs_core::ports::{self, Clock, SystemClock};
use bqs_core::{CoreError, CoreResult};
use rusqlite::{Connection, OptionalExtension};

#[cfg(test)]
mod migration_tests;
pub mod migrations;

pub use episodes::{EPISODE_PAGE_DEFAULT_LIMIT, EPISODE_PAGE_MAX_LIMIT, clamp_episode_page_limit};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("migration v{version} ({name}): {message}")]
    Migration {
        version: i64,
        name: &'static str,
        message: String,
    },
    #[error("unexpected state: {0}")]
    Unexpected(String),
}

impl From<StoreError> for CoreError {
    fn from(e: StoreError) -> Self {
        CoreError::Store(e.to_string())
    }
}

pub struct Store {
    conn: Mutex<Connection>,

    clock: Arc<dyn Clock>,
}

impl Store {
    pub fn open(path: &str) -> Result<Arc<Self>, StoreError> {
        Self::open_with_clock(path, Arc::new(SystemClock))
    }

    pub fn open_with_clock(path: &str, clock: Arc<dyn Clock>) -> Result<Arc<Self>, StoreError> {
        let conn = Connection::open(path).map_err(|e| StoreError::Migration {
            version: -1,
            name: "open",
            message: format!("open sqlite {path}: {e}"),
        })?;

        let _journal: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        conn.execute_batch("PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;")?;
        migrations::run(&conn)?;
        tracing::info!(
            "[store] SQLite 已就绪: path={path} journal=wal busy_timeout=5000 foreign_keys=on"
        );
        Ok(Arc::new(Store {
            conn: Mutex::new(conn),
            clock,
        }))
    }

    pub(crate) fn now(&self) -> i64 {
        self.clock.now()
    }

    pub(crate) fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, StoreError>,
    ) -> CoreResult<T> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        f(&conn).map_err(CoreError::from)
    }
}

pub(crate) fn tx_read_modify_write<T>(
    conn: &Connection,
    f: impl FnOnce(&Connection) -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    conn.execute_batch("BEGIN").map_err(StoreError::from)?;
    match f(conn) {
        Ok(v) => {
            conn.execute_batch("COMMIT").map_err(StoreError::from)?;
            Ok(v)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

const _: () = {
    fn _assert_store_impls_all_ports() {
        fn assert_impl<T>()
        where
            T: ports::SettingsStore
                + ports::NetworkStore
                + ports::MatchStore
                + ports::StoreStats
                + ports::TrainerStore
                + ports::EpisodeStore
                + ports::WorkerStore
                + ports::TrainerRegistryStore
                + ports::FeaturePackStore
                + ports::JobStore,
        {
        }
        assert_impl::<Store>();
    }
};

impl ports::StoreStats for Store {
    fn counts(&self) -> CoreResult<ports::StoreCounts> {
        self.with_conn(|conn| {
            let one = |q: &str| -> Result<i64, StoreError> {
                conn.query_row(q, [], |row| row.get::<_, i64>(0))
                    .map_err(StoreError::from)
            };
            Ok(ports::StoreCounts {
                networks: one("SELECT COUNT(*) FROM networks")?,
                candidates: one("SELECT COUNT(*) FROM networks WHERE status='candidate'")?,
                matches_running: one("SELECT COUNT(*) FROM matches WHERE status='running'")?,
                episodes: one("SELECT COUNT(*) FROM episodes")?,
                workers: one("SELECT COUNT(*) FROM workers")?,
                episode_games: one("SELECT COALESCE(SUM(game_count),0) FROM episodes")?,
            })
        })
    }
}

impl ports::SettingsStore for Store {
    fn get_setting(&self, key: &str) -> CoreResult<Option<String>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT value FROM settings WHERE key=?")
                .map_err(StoreError::from)?;
            stmt.query_row(rusqlite::params![key], |row| row.get::<_, String>(0))
                .optional()
                .map_err(StoreError::from)
        })
    }

    fn get_int_setting(&self, key: &str, default: i64) -> CoreResult<i64> {
        match self.get_setting(key)? {
            None => Ok(default),
            Some(v) if v.is_empty() => Ok(default),
            Some(v) => v
                .parse::<i64>()
                .map_err(|_| CoreError::Store(format!("setting {key}={v:?} 无法解析为整数"))),
        }
    }

    fn set_setting(&self, key: &str, value: &str) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO settings (key, value) VALUES (?,?)
				 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                rusqlite::params![key, value],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }

    fn inc_setting(&self, key: &str) -> CoreResult<i64> {
        self.with_conn(|conn| {
            tx_read_modify_write(conn, |conn| {
                let cur: i64 = conn
                    .query_row(
                        "SELECT COALESCE((SELECT CAST(value AS INTEGER) FROM settings WHERE key = ?), 0)",
                        rusqlite::params![key],
                        |row| row.get(0),
                    )
                    .map_err(StoreError::from)?;
                let next = cur + 1;
                conn.execute(
                    "INSERT INTO settings (key, value) VALUES (?, ?)
					 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    rusqlite::params![key, next.to_string()],
                )
                .map_err(StoreError::from)?;
                Ok(next)
            })
        })
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    use super::Store;
    use std::sync::Arc;

    use bqs_core::test_support::FakeClock;

    pub(crate) fn open_temp_store() -> (tempfile::TempDir, Arc<Store>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.db");
        let store = Store::open(path.to_str().expect("utf8 path")).expect("open store");
        (dir, store)
    }

    pub(crate) fn open_temp_store_at(unix: i64) -> (tempfile::TempDir, Arc<Store>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.db");
        let store = Store::open_with_clock(path.to_str().expect("utf8 path"), FakeClock::new(unix))
            .expect("open store");
        (dir, store)
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::open_temp_store;
    use bqs_core::CoreResult;
    use bqs_core::ports::{SettingsStore, SystemClock};

    #[test]
    fn settings_roundtrip_and_int_defaults() {
        let (_dir, s) = open_temp_store();

        assert_eq!(s.get_setting("nope").unwrap(), None);
        assert_eq!(s.get_int_setting("nope", 42).unwrap(), 42);

        s.set_setting("k", "v1").unwrap();
        s.set_setting("k", "v2").unwrap();
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("v2"));

        s.set_setting("empty", "").unwrap();
        assert_eq!(s.get_int_setting("empty", 7).unwrap(), 7);

        s.set_setting("bad", "abc").unwrap();
        assert!(s.get_int_setting("bad", 0).is_err());
    }

    #[test]
    fn inc_setting_monotonic() {
        let (_dir, s) = open_temp_store();
        assert_eq!(s.inc_setting("counter").unwrap(), 1);
        assert_eq!(s.inc_setting("counter").unwrap(), 2);
        s.set_setting("preset", "10").unwrap();
        assert_eq!(s.inc_setting("preset").unwrap(), 11);
    }

    #[test]
    fn reopen_is_idempotent() -> CoreResult<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        {
            let _s = super::Store::open_with_clock(
                path.to_str().unwrap(),
                std::sync::Arc::new(SystemClock),
            )?;
        }
        {
            let s = super::Store::open(path.to_str().unwrap())?;
            s.set_setting("after", "reopen")?;
            assert_eq!(s.get_setting("after")?.as_deref(), Some("reopen"));
        }
        Ok(())
    }
}
