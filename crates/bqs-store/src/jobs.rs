//! 【所有权：infra 成员】jobs / job_tasks 表适配器（统一 Job 机制持久层）。
//!
//! 参考同目录 eval.rs / episodes.rs 的写法。要点：
//! - `incr_job_completed` / `incr_job_dispatched` 必须单事务读改写（先落库后
//!   终态的保序约定依赖单语句/单事务原子性）；
//! - `list_jobs` 排序 = 派发源选择顺序：priority DESC、created_at ASC；
//! - `job_tasks` 行在聚合完成后由调用方删除（CAS 防双计，映射只服务上报路径）。

use bqs_core::ports::{JobRow, JobStore, JobTaskLink};
use bqs_core::CoreResult;
use rusqlite::OptionalExtension;

use crate::{Store, StoreError};

fn row_from(
    job_id: String,
    kind: i64,
    priority: i64,
    state: String,
    total_games: i64,
    completed_games: i64,
    dispatched_games: i64,
    spec_json: String,
    created_at: i64,
    updated_at: i64,
    next_seq: i64,
) -> JobRow {
    JobRow {
        job_id,
        kind,
        priority,
        state,
        total_games,
        completed_games,
        dispatched_games,
        spec_json,
        created_at,
        updated_at,
        next_seq,
    }
}

impl JobStore for Store {
    fn insert_job(&self, row: JobRow) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO jobs (job_id, kind, priority, state, total_games, completed_games,
				 dispatched_games, spec_json, created_at, updated_at, next_seq)
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    row.job_id,
                    row.kind,
                    row.priority,
                    row.state,
                    row.total_games,
                    row.completed_games,
                    row.dispatched_games,
                    row.spec_json,
                    row.created_at,
                    row.updated_at,
                    row.next_seq,
                ],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }

    fn get_job(&self, job_id: &str) -> CoreResult<Option<JobRow>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT job_id, kind, priority, state, total_games, completed_games,
					 dispatched_games, spec_json, created_at, updated_at, next_seq
					 FROM jobs WHERE job_id = ?1",
                )
                .map_err(StoreError::from)?;
            stmt.query_row(rusqlite::params![job_id], |row| {
                Ok(row_from(
                    row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?,
                    row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?,
                    row.get(10)?,
                ))
            })
            .optional()
            .map_err(StoreError::from)
        })
    }

    fn list_jobs(&self, state: Option<&str>, kind: Option<i64>) -> CoreResult<Vec<JobRow>> {
        self.with_conn(|conn| {
            let mut sql = String::from(
                "SELECT job_id, kind, priority, state, total_games, completed_games,
				 dispatched_games, spec_json, created_at, updated_at, next_seq FROM jobs",
            );
            let mut conds: Vec<String> = Vec::new();
            if state.is_some() {
                conds.push("state = ?1".into());
            }
            if kind.is_some() {
                conds.push(format!("kind = ?{}", conds.len() + 1));
            }
            if !conds.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&conds.join(" AND "));
            }
            sql.push_str(" ORDER BY priority DESC, created_at ASC");
            let mut stmt = conn.prepare(&sql).map_err(StoreError::from)?;
            let state_p = state.map(|s| s.to_string());
            let kind_p = kind;
            let map = |row: &rusqlite::Row| {
                Ok(row_from(
                    row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?,
                    row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?,
                    row.get(10)?,
                ))
            };
            let rows = match (state_p.as_deref(), kind_p) {
                (Some(s), Some(k)) => stmt
                    .query_map(rusqlite::params![s, k], map)
                    .map_err(StoreError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(StoreError::from)?,
                (Some(s), None) => stmt
                    .query_map(rusqlite::params![s], map)
                    .map_err(StoreError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(StoreError::from)?,
                (None, Some(k)) => stmt
                    .query_map(rusqlite::params![k], map)
                    .map_err(StoreError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(StoreError::from)?,
                (None, None) => stmt
                    .query_map([], map)
                    .map_err(StoreError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(StoreError::from)?,
            };
            Ok(rows)
        })
    }

    fn update_job_state(&self, job_id: &str, state: &str, now: i64) -> CoreResult<bool> {
        self.with_conn(|conn| {
            let n = conn
                .execute(
                    "UPDATE jobs SET state = ?2, updated_at = ?3 WHERE job_id = ?1",
                    rusqlite::params![job_id, state, now],
                )
                .map_err(StoreError::from)?;
            Ok(n > 0)
        })
    }

    fn incr_job_dispatched(&self, job_id: &str, games: i64, now: i64) -> CoreResult<JobRow> {
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN").map_err(StoreError::from)?;
            let write = conn.execute(
                "UPDATE jobs SET dispatched_games = dispatched_games + ?2, next_seq = next_seq + 1,
				 updated_at = ?3 WHERE job_id = ?1",
                rusqlite::params![job_id, games, now],
            )
            .map_err(StoreError::from);
            if let Err(e) = write {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(StoreError::from(e).into());
            }
            let read = Self::read_job_conn(conn, job_id);
            match read {
                Ok(row) => {
                    conn.execute_batch("COMMIT").map_err(StoreError::from)?;
                    row.ok_or_else(|| {
                        StoreError::Unexpected(format!("job {job_id} 派发记账后消失"))
                    })
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    Err(e.into())
                }
            }
        })
    }

    fn incr_job_completed(&self, job_id: &str, games: i64, now: i64) -> CoreResult<JobRow> {
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN").map_err(StoreError::from)?;
            let write = conn.execute(
                "UPDATE jobs SET completed_games = completed_games + ?2, updated_at = ?3
				 WHERE job_id = ?1",
                rusqlite::params![job_id, games, now],
            )
            .map_err(StoreError::from);
            if let Err(e) = write {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(StoreError::from(e).into());
            }
            let read = Self::read_job_conn(conn, job_id);
            match read {
                Ok(row) => {
                    conn.execute_batch("COMMIT").map_err(StoreError::from)?;
                    row.ok_or_else(|| {
                        StoreError::Unexpected(format!("job {job_id} 聚合后消失"))
                    })
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    Err(e.into())
                }
            }
        })
    }

    fn insert_job_task(&self, task_id: &str, job_id: &str, games: i64) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT OR REPLACE INTO job_tasks (task_id, job_id, games) VALUES (?1, ?2, ?3)",
                rusqlite::params![task_id, job_id, games],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }

    fn get_job_task(&self, task_id: &str) -> CoreResult<Option<JobTaskLink>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT task_id, job_id, games FROM job_tasks WHERE task_id = ?1")
                .map_err(StoreError::from)?;
            stmt.query_row(rusqlite::params![task_id], |row| {
                Ok(JobTaskLink { task_id: row.get(0)?, job_id: row.get(1)?, games: row.get(2)? })
            })
            .optional()
            .map_err(StoreError::from)
        })
    }

    fn delete_job_task(&self, task_id: &str) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute("DELETE FROM job_tasks WHERE task_id = ?1", rusqlite::params![task_id])
                .map_err(StoreError::from)?;
            Ok(())
        })
    }
}

impl Store {
    fn read_job_conn(
        conn: &rusqlite::Connection,
        job_id: &str,
    ) -> Result<Option<JobRow>, StoreError> {
        let mut stmt = conn
            .prepare(
                "SELECT job_id, kind, priority, state, total_games, completed_games,
				 dispatched_games, spec_json, created_at, updated_at, next_seq
				 FROM jobs WHERE job_id = ?1",
            )
            .map_err(StoreError::from)?;
        stmt.query_row(rusqlite::params![job_id], |row| {
            Ok(row_from(
                row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?,
                row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?,
                row.get(10)?,
            ))
        })
        .optional()
        .map_err(StoreError::from)
    }
}
