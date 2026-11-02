use bqs_core::ports::MatchStore;
use bqs_core::types::{match_status, Match};
use bqs_core::CoreResult;
use bqs_judgment::Hexanomial;

use crate::{Store, StoreError};

use rusqlite::OptionalExtension;

const MATCH_COLUMNS: &str = "id, candidate, opponent, status, pair_ll, pair_ld, pair_dd, pair_wl, pair_dw, pair_ww, num_games, target_games";

fn scan_match_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Match> {
    let p = |i: usize| -> rusqlite::Result<i64> { row.get(4 + i) };

    let s = |i: usize| -> rusqlite::Result<String> {
        row.get::<_, Option<String>>(i)
            .map(|v| v.unwrap_or_default())
    };
    Ok(Match {
        id: row.get(0)?,
        candidate: s(1)?,
        opponent: s(2)?,
        status: row.get(3)?,
        pairs: Hexanomial([p(0)?, p(1)?, p(2)?, p(3)?, p(4)?, p(5)?]),
        num_games: row.get(10)?,
        target_games: row.get(11)?,
    })
}

impl MatchStore for Store {
    fn create_match(&self, candidate: &str, opponent: &str, target_games: i64) -> CoreResult<i64> {
        let candidate = if candidate.is_empty() {
            None
        } else {
            Some(candidate)
        };
        let opponent = if opponent.is_empty() {
            None
        } else {
            Some(opponent)
        };
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO matches (candidate, opponent, target_games) VALUES (?,?,?)",
                rusqlite::params![candidate, opponent, target_games],
            )
            .map_err(StoreError::from)?;
            Ok(conn.last_insert_rowid())
        })
    }

    fn get_match(&self, id: i64) -> CoreResult<Option<Match>> {
        self.with_conn(|conn| {
            conn.query_row(
                &format!("SELECT {MATCH_COLUMNS} FROM matches WHERE id=?"),
                rusqlite::params![id],
                scan_match_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    fn list_matches(&self) -> CoreResult<Vec<Match>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {MATCH_COLUMNS} FROM matches
					 ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, id DESC"
                ))
                .map_err(StoreError::from)?;
            let rows = stmt
                .query_map([], scan_match_row)
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
    }

    fn add_match_games(&self, id: i64, delta: Hexanomial, games: i64) -> CoreResult<(Match, bool)> {
        self.with_conn(|conn| {
            conn.query_row(
                &format!(
                    "UPDATE matches SET
						pair_ll=pair_ll+?, pair_ld=pair_ld+?, pair_dd=pair_dd+?,
						pair_wl=pair_wl+?, pair_dw=pair_dw+?, pair_ww=pair_ww+?,
						num_games=num_games+?
					 WHERE id=? AND status='running'
					 RETURNING {MATCH_COLUMNS}"
                ),
                rusqlite::params![
                    delta.0[0], delta.0[1], delta.0[2], delta.0[3], delta.0[4], delta.0[5], games,
                    id
                ],
                scan_match_row,
            )
            .optional()
            .map(|opt| match opt {
                Some(m) => (m, true),
                None => (zero_match(), false),
            })
            .map_err(StoreError::from)
        })
    }

    fn conclude_match(&self, id: i64) -> CoreResult<bool> {
        self.with_conn(|conn| {
            let n = conn
                .execute(
                    "UPDATE matches SET status=? WHERE id=? AND status=?",
                    rusqlite::params![match_status::CONCLUDED, id, match_status::RUNNING],
                )
                .map_err(StoreError::from)?;
            Ok(n > 0)
        })
    }

    fn cancel_match(&self, id: i64) -> CoreResult<bool> {
        self.with_conn(|conn| {
            let n = conn
                .execute(
                    "UPDATE matches SET status=? WHERE id=? AND status=?",
                    rusqlite::params![match_status::CANCELLED, id, match_status::RUNNING],
                )
                .map_err(StoreError::from)?;
            Ok(n > 0)
        })
    }
}

fn zero_match() -> Match {
    Match {
        id: 0,
        candidate: String::new(),
        opponent: String::new(),
        status: match_status::RUNNING.to_string(),
        pairs: Hexanomial::ZERO,
        num_games: 0,
        target_games: 0,
    }
}

#[cfg(test)]
mod tests {
    use bqs_core::ports::MatchStore as MS;
    use bqs_core::types::match_status;
    use bqs_judgment::Hexanomial;

    #[test]
    fn create_query_and_list() {
        let (_dir, s) = crate::testutil::open_temp_store();
        use bqs_core::ports::NetworkStore as NS;

        NS::register_network(&*s, "c1", None, None, "onnx").unwrap();
        NS::register_network(&*s, "o1", None, None, "onnx").unwrap();

        let id = MS::create_match(&*s, "c1", "o1", 100).unwrap();
        let m = MS::get_match(&*s, id).unwrap().unwrap();
        assert_eq!(m.candidate, "c1");
        assert_eq!(m.target_games, 100);
        assert!(m.running());

        let id2 = MS::create_match(&*s, "c1", "o1", 50).unwrap();
        MS::conclude_match(&*s, id).unwrap();

        let list = MS::list_matches(&*s).unwrap();
        assert_eq!(list[0].id, id2);
        assert_eq!(list[1].id, id);
        assert_eq!(MS::get_match(&*s, 999).unwrap(), None);
    }

    #[test]
    fn create_match_nullable_side_for_non_network() {
        let (_dir, s) = crate::testutil::open_temp_store();
        use bqs_core::ports::NetworkStore as NS;
        NS::register_network(&*s, "c1", None, None, "onnx").unwrap();

        let id = MS::create_match(&*s, "c1", "", 10).unwrap();
        let m = MS::get_match(&*s, id).unwrap().unwrap();
        assert_eq!(m.candidate, "c1");
        assert_eq!(m.opponent, "", "非网络 side 读侧归一为空串");
        assert!(m.running());

        {
            let conn = s.conn.lock().unwrap();
            let opp: Option<String> = conn
                .query_row("SELECT opponent FROM matches WHERE id=?", [id], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(opp, None, "非网络 side 的 sha 应存 NULL");
        }

        let id2 = MS::create_match(&*s, "", "c1", 10).unwrap();
        let m2 = MS::get_match(&*s, id2).unwrap().unwrap();
        assert_eq!(m2.candidate, "");
        assert_eq!(m2.opponent, "c1");

        assert!(MS::create_match(&*s, "c1", "ghost", 10).is_err());
        assert!(MS::create_match(&*s, "ghost", "c1", 10).is_err());
    }

    #[test]
    fn add_games_atomic_and_running_gate() {
        let (_dir, s) = crate::testutil::open_temp_store();
        use bqs_core::ports::NetworkStore as NS;
        NS::register_network(&*s, "c1", None, None, "onnx").unwrap();
        NS::register_network(&*s, "o1", None, None, "onnx").unwrap();
        let id = MS::create_match(&*s, "c1", "o1", 10).unwrap();

        let (m, ok) = MS::add_match_games(&*s, id, Hexanomial([1, 2, 3, 4, 5, 6]), 21).unwrap();
        assert!(ok);
        assert_eq!(m.pairs, Hexanomial([1, 2, 3, 4, 5, 6]));
        assert_eq!(m.num_games, 21);

        let (m, ok) = MS::add_match_games(&*s, id, Hexanomial([1, 0, 0, 0, 0, 0]), 1).unwrap();
        assert!(ok);
        assert_eq!(m.pairs, Hexanomial([2, 2, 3, 4, 5, 6]));
        assert_eq!(m.num_games, 22);

        assert!(MS::conclude_match(&*s, id).unwrap());
        assert!(!MS::conclude_match(&*s, id).unwrap());
        let (m, ok) = MS::add_match_games(&*s, id, Hexanomial::ZERO, 1).unwrap();
        assert!(!ok);
        assert_eq!(m.num_games, 0);

        let (_, ok) = MS::add_match_games(&*s, 999, Hexanomial::ZERO, 1).unwrap();
        assert!(!ok);
    }

    #[test]
    fn conclude_is_cas() {
        let (_dir, s) = crate::testutil::open_temp_store();
        use bqs_core::ports::NetworkStore as NS;
        NS::register_network(&*s, "c1", None, None, "onnx").unwrap();
        NS::register_network(&*s, "o1", None, None, "onnx").unwrap();
        let id = MS::create_match(&*s, "c1", "o1", 10).unwrap();
        assert!(MS::conclude_match(&*s, id).unwrap());
        assert!(!MS::conclude_match(&*s, id).unwrap());
        assert!(!MS::conclude_match(&*s, 424242).unwrap());
        assert_eq!(
            MS::get_match(&*s, id).unwrap().unwrap().status,
            match_status::CONCLUDED
        );
    }

    #[test]
    fn cancel_match_is_cas_and_gates_aggregation() {
        let (_dir, s) = crate::testutil::open_temp_store();
        use bqs_core::ports::NetworkStore as NS;
        NS::register_network(&*s, "c1", None, None, "onnx").unwrap();
        NS::register_network(&*s, "o1", None, None, "onnx").unwrap();
        let id = MS::create_match(&*s, "c1", "o1", 10).unwrap();
        assert!(MS::cancel_match(&*s, id).unwrap());
        assert!(!MS::cancel_match(&*s, id).unwrap(), "二次取消 CAS false");
        assert!(
            !MS::cancel_match(&*s, 424242).unwrap(),
            "不存在 match false"
        );
        assert_eq!(
            MS::get_match(&*s, id).unwrap().unwrap().status,
            match_status::CANCELLED
        );
        let (_, ok) = MS::add_match_games(&*s, id, Hexanomial::ZERO, 1).unwrap();
        assert!(!ok, "cancelled 对打拒绝累加");

        assert!(!MS::conclude_match(&*s, id).unwrap());
    }
}
