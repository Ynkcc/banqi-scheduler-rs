use bqs_core::CoreResult;
use bqs_core::ports::NetworkStore;
use bqs_core::types::Network;

use crate::{Store, StoreError};

use rusqlite::OptionalExtension;

const NETWORK_COLUMNS: &str =
    "sha, COALESCE(parent_sha,''), created_at, status, COALESCE(notes,''), format";

fn scan_network_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Network> {
    Ok(Network {
        sha: row.get(0)?,
        parent_sha: row.get(1)?,
        created_at: row.get(2)?,
        status: row.get(3)?,
        notes: row.get(4)?,
        format: row.get(5)?,
    })
}

impl NetworkStore for Store {
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

    fn list_networks(&self) -> CoreResult<Vec<Network>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {NETWORK_COLUMNS} FROM networks ORDER BY created_at DESC"
                ))
                .map_err(StoreError::from)?;
            let rows = stmt
                .query_map([], scan_network_row)
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
    }

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
    fn register_and_status_roundtrip() {
        let (_dir, s) = crate::testutil::open_temp_store();
        use bqs_core::ports::NetworkStore as NS;

        assert!(NS::register_network(&*s, "aaa", None, Some("gen1"), "onnx").unwrap());
        assert!(!NS::register_network(&*s, "aaa", None, Some("gen1"), "onnx").unwrap());
        assert!(NS::register_network(&*s, "bbb", Some("aaa"), None, "onnx").unwrap());

        assert_eq!(
            NS::get_network(&*s, "aaa").unwrap().unwrap().status,
            network_status::CANDIDATE
        );

        NS::update_network_status(&*s, "bbb", network_status::REJECTED).unwrap();
        assert_eq!(
            NS::get_network(&*s, "bbb").unwrap().unwrap().status,
            network_status::REJECTED
        );

        let list = NS::list_networks(&*s).unwrap();
        assert_eq!(list.len(), 2);

        assert!(NS::get_network(&*s, "ghost").unwrap().is_none());
    }
}
