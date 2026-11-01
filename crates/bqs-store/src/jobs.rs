use bqs_core::ports::{JobPage, JobQuery, JobRow, JobStore, JobTaskLink};
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
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            })
            .optional()
            .map_err(StoreError::from)
        })
    }

    fn list_jobs(&self, query: &JobQuery) -> CoreResult<JobPage> {
        self.with_conn(|conn| {
            let mut where_sql = String::new();
            let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            if let Some(id) = query.job_id.as_deref().filter(|s| !s.is_empty()) {
                where_sql.push_str(if args.is_empty() { " WHERE" } else { " AND" });
                where_sql.push_str(" job_id = ?");
                args.push(Box::new(id.to_string()));
            }
            if let Some(s) = query.state.as_deref().filter(|s| !s.is_empty()) {
                where_sql.push_str(if args.is_empty() { " WHERE" } else { " AND" });
                where_sql.push_str(" state = ?");
                args.push(Box::new(s.to_string()));
            }
            if let Some(k) = query.kind {
                where_sql.push_str(if args.is_empty() { " WHERE" } else { " AND" });
                where_sql.push_str(" kind = ?");
                args.push(Box::new(k));
            }

            let count_sql = format!("SELECT COUNT(*) FROM jobs{where_sql}");
            let total: i64 = conn
                .query_row(
                    &count_sql,
                    rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())),
                    |row| row.get(0),
                )
                .map_err(StoreError::from)?;

            let mut sql = format!(
                "SELECT job_id, kind, priority, state, total_games, completed_games,
				 dispatched_games, spec_json, created_at, updated_at, next_seq FROM jobs{where_sql}
				 ORDER BY priority DESC, created_at ASC"
            );

            if let Some(l) = query.limit.filter(|l| *l > 0) {
                sql.push_str(&format!(" LIMIT {l} OFFSET {}", query.offset));
            }

            let mut stmt = conn.prepare(&sql).map_err(StoreError::from)?;
            let map = |row: &rusqlite::Row| {
                Ok(row_from(
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            };
            let rows = stmt
                .query_map(
                    rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())),
                    map,
                )
                .map_err(StoreError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)?;
            Ok(JobPage { items: rows, total })
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
            crate::tx_read_modify_write(conn, |conn| {
                conn.execute(
                    "UPDATE jobs SET dispatched_games = dispatched_games + ?2, next_seq = next_seq + 1,
					 updated_at = ?3 WHERE job_id = ?1",
                    rusqlite::params![job_id, games, now],
                )
                .map_err(StoreError::from)?;
                Self::read_job_conn(conn, job_id)?.ok_or_else(|| {
                    StoreError::Unexpected(format!("job {job_id} 派发记账后消失"))
                })
            })
        })
    }

    fn incr_job_completed(&self, job_id: &str, games: i64, now: i64) -> CoreResult<JobRow> {
        self.with_conn(|conn| {
            crate::tx_read_modify_write(conn, |conn| {
                conn.execute(
                    "UPDATE jobs SET completed_games = completed_games + ?2, updated_at = ?3
					 WHERE job_id = ?1",
                    rusqlite::params![job_id, games, now],
                )
                .map_err(StoreError::from)?;
                Self::read_job_conn(conn, job_id)?
                    .ok_or_else(|| StoreError::Unexpected(format!("job {job_id} 聚合后消失")))
            })
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
                Ok(JobTaskLink {
                    task_id: row.get(0)?,
                    job_id: row.get(1)?,
                    games: row.get(2)?,
                })
            })
            .optional()
            .map_err(StoreError::from)
        })
    }

    fn delete_job_task(&self, task_id: &str) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "DELETE FROM job_tasks WHERE task_id = ?1",
                rusqlite::params![task_id],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }

    fn update_job_spec(&self, job_id: &str, spec_json: &str, now: i64) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE jobs SET spec_json = ?2, updated_at = ?3 WHERE job_id = ?1",
                rusqlite::params![job_id, spec_json, now],
            )
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
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
                row.get(10)?,
            ))
        })
        .optional()
        .map_err(StoreError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bqs_core::ports::JobStore as JS;

    #[test]
    fn update_job_spec_rewrites_echo_and_timestamp() {
        let (_dir, s) = crate::testutil::open_temp_store();
        JS::insert_job(
            &*s,
            JobRow {
                job_id: "j1".into(),
                kind: 3,
                priority: 80,
                state: "running".into(),
                total_games: 20,
                completed_games: 0,
                dispatched_games: 0,
                spec_json: r#"{"rating":{"match_id":0}}"#.into(),
                created_at: 1,
                updated_at: 1,
                next_seq: 0,
            },
        )
        .unwrap();
        JS::update_job_spec(&*s, "j1", r#"{"rating":{"match_id":7}}"#, 99).unwrap();
        let row = JS::get_job(&*s, "j1").unwrap().unwrap();
        assert!(row.spec_json.contains("\"match_id\":7"));
        assert_eq!(row.updated_at, 99);

        JS::update_job_spec(&*s, "ghost", "{}", 100).unwrap();
    }
}
