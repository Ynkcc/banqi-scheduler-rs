//! 【所有权：infra 成员】networks 表端口：best 指针读写、登记、晋级与状态标记。
//! SQL 逐条对照 Go 版 internal/store/networks.go，语义一致。

use bqs_core::ports::NetworkStore;
use bqs_core::types::{Network, network_status};
use bqs_core::CoreResult;

use crate::{Store, StoreError};

use rusqlite::OptionalExtension;

/// networks 表的统一列序（对照 Go 版 networkColumns）。
const NETWORK_COLUMNS: &str =
    "sha, COALESCE(parent_sha,''), created_at, is_best, status, COALESCE(notes,''), format";

fn scan_network_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Network> {
    Ok(Network {
        sha: row.get(0)?,
        parent_sha: row.get(1)?,
        created_at: row.get(2)?,
        is_best: row.get::<_, i64>(3)? != 0,
        status: row.get(4)?,
        notes: row.get(5)?,
        format: row.get(6)?,
    })
}

impl NetworkStore for Store {
    fn get_best(&self) -> CoreResult<Option<Network>> {
        self.with_conn(|conn| {
            conn.query_row(
                &format!("SELECT {NETWORK_COLUMNS} FROM networks WHERE is_best=1 LIMIT 1"),
                [],
                scan_network_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    fn get_network(&self, sha: &str) -> CoreResult<Option<Network>> {
        self.with_conn(|conn| {
            conn.query_row(
                &format!("SELECT {NETWORK_COLUMNS} FROM networks WHERE sha=?"),
                rusqlite::params![sha],
                scan_network_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// 该权重格式最新登记的网络（数据类别换道时用作产数据方）。
    /// 对照 Go 版 LatestNetworkByFormat：按登记时间倒序，rowid 破同秒平局。
    fn latest_network_by_format(&self, format: &str) -> CoreResult<Option<Network>> {
        self.with_conn(|conn| {
            conn.query_row(
                &format!(
                    "SELECT {NETWORK_COLUMNS} FROM networks WHERE format=? \
					 ORDER BY created_at DESC, rowid DESC LIMIT 1"
                ),
                rusqlite::params![format],
                scan_network_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// 全部网络列表（best 优先；WebUI /api/networks）。
    fn list_networks(&self) -> CoreResult<Vec<Network>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {NETWORK_COLUMNS} FROM networks ORDER BY is_best DESC, created_at DESC"
                ))
                .map_err(StoreError::from)?;
            let rows = stmt
                .query_map([], scan_network_row)
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
        })
    }

    /// 登记新网络；返回 true = 该 sha 首次登记（触发 gatekeeper 建对打）。
    /// 对照 Go 版 RegisterNetwork：INSERT OR IGNORE，RowsAffected>0 即首次登记。
    fn register_network(
        &self,
        sha: &str,
        parent_sha: Option<&str>,
        notes: Option<&str>,
        format: &str,
    ) -> CoreResult<bool> {
        let now = self.now();
        self.with_conn(|conn| {
            let n = conn
                .execute(
                    "INSERT OR IGNORE INTO networks (sha, parent_sha, created_at, status, notes, format)
					 VALUES (?,?,?,'candidate',?,?)",
                    rusqlite::params![sha, parent_sha, now, notes, format],
                )
                .map_err(StoreError::from)?;
            Ok(n > 0)
        })
    }

    /// best 指针事务切换（含旧 best 归档语义）。
    /// 对照 Go 版 PromoteBest：单事务内旧 best 降级为 archived，目标置 best。
    fn promote_best(&self, sha: &str) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN").map_err(StoreError::from)?;
            let demote = conn
                .execute(
                    "UPDATE networks SET is_best=0, status=? WHERE is_best=1",
                    rusqlite::params![network_status::ARCHIVED],
                )
                .map_err(StoreError::from)
                .and_then(|_| {
                    conn.execute(
                        "UPDATE networks SET is_best=1, status=? WHERE sha=?",
                        rusqlite::params![network_status::BEST, sha],
                    )
                    .map_err(StoreError::from)
                });
            match demote {
                Ok(_) => conn.execute_batch("COMMIT").map_err(StoreError::from)?,
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    return Err(e.into());
                }
            }
            Ok(())
        })
    }

    fn update_network_status(&self, sha: &str, status: &str) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE networks SET status=? WHERE sha=?",
                rusqlite::params![status, sha],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use bqs_core::types::network_status;

    #[test]
    fn register_and_promote_roundtrip() {
        let (_dir, s) = crate::testutil::open_temp_store();
        use bqs_core::ports::NetworkStore as NS;

        // 首次登记 = true，重复登记 = false（幂等）
        assert!(NS::register_network(&*s, "aaa", None, Some("gen1"), "onnx").unwrap());
        assert!(!NS::register_network(&*s, "aaa", None, Some("gen1"), "onnx").unwrap());
        assert!(NS::register_network(&*s, "bbb", Some("aaa"), None, "onnx").unwrap());

        // 未晋级：无 best
        assert!(NS::get_best(&*s).unwrap().is_none());
        assert_eq!(
            NS::get_network(&*s, "aaa").unwrap().unwrap().status,
            network_status::CANDIDATE
        );

        // 晋级 bbb：旧 best（无）不报错；bbb 变 best
        NS::promote_best(&*s, "bbb").unwrap();
        assert_eq!(NS::get_best(&*s).unwrap().unwrap().sha, "bbb");

        // 再晋级 aaa：bbb 归档
        NS::promote_best(&*s, "aaa").unwrap();
        let best = NS::get_best(&*s).unwrap().unwrap();
        assert_eq!(best.sha, "aaa");
        assert_eq!(
            NS::get_network(&*s, "bbb").unwrap().unwrap().status,
            network_status::ARCHIVED
        );

        // 状态更新
        NS::update_network_status(&*s, "bbb", network_status::REJECTED).unwrap();
        assert_eq!(
            NS::get_network(&*s, "bbb").unwrap().unwrap().status,
            network_status::REJECTED
        );

        // 列表：best 优先，其余按登记时间倒序
        let list = NS::list_networks(&*s).unwrap();
        assert_eq!(list[0].sha, "aaa");
        assert_eq!(list.len(), 2);

        // latest_network_by_format：同秒登记按 rowid 破平（rowid DESC），bbb 登记更晚
        assert_eq!(NS::latest_network_by_format(&*s, "onnx").unwrap().unwrap().sha, "bbb");
        assert!(NS::latest_network_by_format(&*s, "nnue").unwrap().is_none());
    }
}
