//! 【所有权：infra 成员】workers 表端口：worker 心跳登记与查询。
//! SQL 逐条对照 Go 版 internal/store/workers.go。

use bqs_core::ports::WorkerStore;
use bqs_core::types::Worker;
use bqs_core::CoreResult;

use crate::{Store, StoreError};

use rusqlite::OptionalExtension;

/// workers 表的统一列序（对照 Go 版 workerColumns）。
const WORKER_COLUMNS: &str =
    "id, last_seen, threads, completed_games, client_version, memory_mb, note, health_status, last_error";

fn scan_worker_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Worker> {
    Ok(Worker {
        id: row.get(0)?,
        last_seen: row.get(1)?,
        threads: row.get(2)?,
        completed_games: row.get(3)?,
        client_version: row.get(4)?,
        memory_mb: row.get(5)?,
        note: row.get(6)?,
        health_status: row.get(7)?,
        last_error: row.get(8)?,
    })
}

impl WorkerStore for Store {
    /// UPSERT 单行心跳。last_seen 取自注入 Clock（Go 版 time.Now().Unix()）。
    ///
    /// 健康字段空串 = 旧客户端：CASE 保留上次上报值，避免旧客户端心跳清掉状态
    /// （对照 Go 版 TouchWorker 的 COALESCE 语义）。心跳不写 note 列，备注保留。
    fn touch_worker(
        &self,
        id: &str,
        threads: i64,
        completed_games: i64,
        client_version: &str,
        memory_mb: i64,
        health_status: &str,
        last_error: &str,
    ) -> CoreResult<()> {
        let now = self.now();
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO workers (id, last_seen, threads, completed_games, client_version, \
					memory_mb, health_status, last_error) VALUES (?,?,?,?,?,?,?,?)
				 ON CONFLICT(id) DO UPDATE SET last_seen=excluded.last_seen, threads=excluded.threads, \
					completed_games=excluded.completed_games, client_version=excluded.client_version, \
					memory_mb=excluded.memory_mb, \
					health_status=CASE WHEN excluded.health_status='' THEN workers.health_status ELSE excluded.health_status END, \
					last_error=CASE WHEN excluded.last_error='' THEN workers.last_error ELSE excluded.last_error END",
                rusqlite::params![id, now, threads, completed_games, client_version, memory_mb, health_status, last_error],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }

    /// GetTask 版本管控：该 worker 上次声明的 client_version；未见过该 worker 返回 None。
    fn worker_version(&self, id: &str) -> CoreResult<Option<String>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT client_version FROM workers WHERE id=?",
                rusqlite::params![id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    fn list_workers(&self) -> CoreResult<Vec<Worker>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {WORKER_COLUMNS} FROM workers ORDER BY last_seen DESC"
                ))
                .map_err(StoreError::from)?;
            let rows = stmt
                .query_map([], scan_worker_row)
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
        })
    }

    /// 运维备注（PUT /api/workers/{id}/note）。
    /// worker 首次心跳上报前不存在——业务拒绝不是错误，这里幂等 no-op（对照
    /// Go 版返回 sql.ErrNoRows 由 api 层转 404；Rust 端把该拒绝交给 api 层用
    /// list/get 判断，store 层保持「Err 只表示真故障」的约定）。
    fn set_worker_note(&self, id: &str, note: &str) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE workers SET note=? WHERE id=?",
                rusqlite::params![note, id],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use bqs_core::ports::WorkerStore as WS;

    #[test]
    fn touch_upsert_and_note_preserved() {
        let (_dir, s) = crate::testutil::open_temp_store();

        WS::touch_worker(&*s, "w1", 4, 10, "v1", 8192, "ok", "").unwrap();
        let w = WS::list_workers(&*s).unwrap().remove(0);
        assert_eq!(
            (w.threads, w.completed_games, w.client_version.as_str(), w.memory_mb),
            (4, 10, "v1", 8192)
        );
        assert_eq!(w.health_status, "ok");

        // 备注：心跳不覆盖
        WS::set_worker_note(&*s, "w1", "gpu-box").unwrap();
        WS::touch_worker(&*s, "w1", 8, 20, "v2", 16384, "ok", "").unwrap();
        let w = WS::list_workers(&*s).unwrap().remove(0);
        assert_eq!(w.note, "gpu-box");
        assert_eq!(w.completed_games, 20);

        // 健康字段空串 = 旧客户端：保留上次上报值
        WS::touch_worker(&*s, "w1", 8, 21, "v2", 16384, "", "").unwrap();
        let w = WS::list_workers(&*s).unwrap().remove(0);
        assert_eq!(w.health_status, "ok");

        // 非空则覆盖
        WS::touch_worker(&*s, "w1", 8, 22, "v2", 16384, "degraded", "oom").unwrap();
        let w = WS::list_workers(&*s).unwrap().remove(0);
        assert_eq!(w.health_status, "degraded");
        assert_eq!(w.last_error, "oom");

        // 版本声明
        assert_eq!(WS::worker_version(&*s, "w1").unwrap().as_deref(), Some("v2"));
        assert_eq!(WS::worker_version(&*s, "ghost").unwrap(), None);
        // 不存在 worker 的备注更新为 no-op（业务拒绝不报错）
        WS::set_worker_note(&*s, "ghost", "x").unwrap();
    }
}
