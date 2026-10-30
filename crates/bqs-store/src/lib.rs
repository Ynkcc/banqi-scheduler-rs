//! 【所有权：infra 成员】
//!
//! 存储适配器：SQLite（rusqlite，WAL）实现 `bqs_core::ports` 全部端口。
//!
//! 参考实现（行为真源）：`banqi-scheduler/internal/store/`（store.go / migrate.go /
//! networks.go / matches.go / eval.go / episodes.go / workers.go / trainers.go / eval_queue.go）。
//!
//! 硬性要求：
//! - **迁移链 v1..v8 与 Go 版语义一致**（`schema_migrations` 记账、单事务补跑、
//!   失败拒绝启动），保证可直接打开 Go 版创建的数据库文件；
//! - 连接参数：WAL + busy_timeout 5s + foreign_keys ON；
//! - `episodes.task_id` 唯一索引（幂等保护）创建失败时降级为应用层查重、不阻断启动；
//! - 并发模型：`std::sync::Mutex<Connection>` 单写者（负载特征：全部为短事务）；
//! - `add_match_games` 的六桶累加必须在单语句内原子完成，bool 返回值语义须与
//!   Go 版 `AddMatchGames` 一致并注释说明；
//! - `migrate` 之外不得执行任何 schema 变更；新增 schema 只追加迁移项。

mod episodes;
mod eval;
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

pub mod migrations;
#[cfg(test)]
mod migration_tests;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("migration v{version} ({name}): {message}")]
    Migration { version: i64, name: &'static str, message: String },
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
    /// 全部可计时行为的时钟来源（Go 版散落的 time.Now() 的显式注入形态）。
    clock: Arc<dyn Clock>,
}

impl Store {
    /// 打开（或创建）数据库并执行迁移链。
    pub fn open(path: &str) -> Result<Arc<Self>, StoreError> {
        Self::open_with_clock(path, Arc::new(SystemClock))
    }

    /// 测试 / 装配根可注入时钟。
    pub fn open_with_clock(path: &str, clock: Arc<dyn Clock>) -> Result<Arc<Self>, StoreError> {
        let conn = Connection::open(path).map_err(|e| {
            StoreError::Migration { version: -1, name: "open", message: format!("open sqlite {path}: {e}") }
        })?;
        // 连接参数与 Go 版 Open 的 DSN pragma 一致：WAL + busy_timeout 5s + 外键约束。
        // journal_mode 返回值须读取（WAL 持久化设置）。
        let _journal: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        conn.execute_batch("PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;")?;
        migrations::run(&conn)?;
        tracing::info!("[store] SQLite 已就绪: path={path} journal=wal busy_timeout=5000 foreign_keys=on");
        Ok(Arc::new(Store { conn: Mutex::new(conn), clock }))
    }

    /// 当前时钟（子模块复用）。
    pub(crate) fn now(&self) -> i64 {
        self.clock.now()
    }

    /// 锁内执行（内部便捷封装；全部短事务，持锁开销可忽略）。
    pub(crate) fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> Result<T, StoreError>) -> CoreResult<T> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        f(&conn).map_err(CoreError::from)
    }
}

// ---------------------------------------------------------------------------
// 编译期断言：Store 满足全部存储端口（ObjectSigner 由 bqs-storage 实现）。
// ---------------------------------------------------------------------------

const _: () = {
    fn _assert_store_impls_all_ports() {
        fn assert_impl<T>()
        where
            T: ports::SettingsStore
                + ports::NetworkStore
                + ports::MatchStore
                + ports::EvalStore
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

// ---------------------------------------------------------------------------
// SettingsStore：settings 表（Control 热更状态与节流计数器共用）。
// 对照 Go 版 store.go 的 GetSetting / GetIntSetting / SetSetting / IncSetting。
// ---------------------------------------------------------------------------

impl ports::SettingsStore for Store {
    /// 读取运行时设置；键不存在时返回 None（Go 版返回空串）。
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

    /// 缺失时返回 default；值非整数视为故障（配置被外部污染）。
    fn get_int_setting(&self, key: &str, default: i64) -> CoreResult<i64> {
        match self.get_setting(key)? {
            None => Ok(default),
            Some(v) if v.is_empty() => Ok(default),
            Some(v) => v.parse::<i64>().map_err(|_| {
                CoreError::Store(format!("setting {key}={v:?} 无法解析为整数"))
            }),
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

    /// 原子地将键值视为整数自增；用于 eval_promotions 等单调计数器。
    /// 不存在的键从 0 起计（与 Go 版 IncSetting 一致：事务内读 COALESCE 后 +1 回写）。
    fn inc_setting(&self, key: &str) -> CoreResult<i64> {
        self.with_conn(|conn| {
            // 单写者 + 单事务：读改写序列化（SQLite WAL 单写者即可保证原子性）。
            conn.execute_batch("BEGIN").map_err(StoreError::from)?;
            let read = conn
                .query_row(
                    "SELECT COALESCE((SELECT CAST(value AS INTEGER) FROM settings WHERE key = ?), 0)",
                    rusqlite::params![key],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(StoreError::from);
            match read {
                Ok(cur) => {
                    let next = cur + 1;
                    let write = conn
                        .execute(
                            "INSERT INTO settings (key, value) VALUES (?, ?)
							 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            rusqlite::params![key, next.to_string()],
                        )
                        .map_err(StoreError::from);
                    match write {
                        Ok(_) => conn.execute_batch("COMMIT").map_err(StoreError::from)?,
                        Err(e) => {
                            let _ = conn.execute_batch("ROLLBACK");
                            return Err(e);
                        }
                    }
                    Ok(next)
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    Err(e)
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// 测试基建与单测
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod testutil {
    use super::Store;
    use bqs_core::ports::Clock;
    use std::sync::Arc;

    /// 固定时钟（时间窗口类查询的确定性测试）。
    struct FixedClock(i64);
    impl Clock for FixedClock {
        fn now(&self) -> i64 {
            self.0
        }
    }

    /// 临时库（tempfile 落盘，Drop 自动清理）。
    pub(crate) fn open_temp_store() -> (tempfile::TempDir, Arc<Store>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.db");
        let store = Store::open(path.to_str().expect("utf8 path")).expect("open store");
        (dir, store)
    }

    /// 固定时钟版临时库（unix = 注入的固定时间点）。
    pub(crate) fn open_temp_store_at(unix: i64) -> (tempfile::TempDir, Arc<Store>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.db");
        let store = Store::open_with_clock(path.to_str().expect("utf8 path"), Arc::new(FixedClock(unix)))
            .expect("open store");
        (dir, store)
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::open_temp_store;
    use bqs_core::ports::{SettingsStore, SystemClock};
    use bqs_core::CoreResult;

    /// 表驱动：settings 读写往返 + 缺失默认值 + 非整数控判错。
    #[test]
    fn settings_roundtrip_and_int_defaults() {
        let (_dir, s) = open_temp_store();

        // 缺失键
        assert_eq!(s.get_setting("nope").unwrap(), None);
        assert_eq!(s.get_int_setting("nope", 42).unwrap(), 42);

        // 写读往返
        s.set_setting("k", "v1").unwrap();
        s.set_setting("k", "v2").unwrap(); // upsert 覆盖
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("v2"));

        // 空串值 → 默认值（Go 版 GetSetting 缺失返回空串口径）
        s.set_setting("empty", "").unwrap();
        assert_eq!(s.get_int_setting("empty", 7).unwrap(), 7);

        // 非整数 → 故障
        s.set_setting("bad", "abc").unwrap();
        assert!(s.get_int_setting("bad", 0).is_err());
    }

    /// inc_setting 原子自增：缺失键从 0 起计，连续自增不丢更新。
    #[test]
    fn inc_setting_monotonic() {
        let (_dir, s) = open_temp_store();
        assert_eq!(s.inc_setting("counter").unwrap(), 1);
        assert_eq!(s.inc_setting("counter").unwrap(), 2);
        s.set_setting("preset", "10").unwrap();
        assert_eq!(s.inc_setting("preset").unwrap(), 11);
    }

    /// 打开 Go 版风格的既有库文件（迁移链幂等：二次打开收敛同一终态）。
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
