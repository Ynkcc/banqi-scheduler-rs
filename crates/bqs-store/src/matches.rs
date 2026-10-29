//! 【所有权：infra 成员】matches 表端口：gatekeeper 对打的创建、查询、
//! 六桶原子累加与条件结转。SQL 逐条对照 Go 版 internal/store/matches.go。

use bqs_core::ports::MatchStore;
use bqs_core::types::{Match, match_status};
use bqs_core::CoreResult;
use bqs_judgment::Hexanomial;

use crate::{Store, StoreError};

use rusqlite::OptionalExtension;

/// matches 表的统一列序（对照 Go 版 matchColumns：[LL, LD, DD, WL, DW, WW]）。
const MATCH_COLUMNS: &str =
    "id, candidate, opponent, status, pair_ll, pair_ld, pair_dd, pair_wl, pair_dw, pair_ww, num_games, target_games";

fn scan_match_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Match> {
    let p = |i: usize| -> rusqlite::Result<i64> { row.get(4 + i) };
    Ok(Match {
        id: row.get(0)?,
        candidate: row.get(1)?,
        opponent: row.get(2)?,
        status: row.get(3)?,
        pairs: Hexanomial([p(0)?, p(1)?, p(2)?, p(3)?, p(4)?, p(5)?]),
        num_games: row.get(10)?,
        target_games: row.get(11)?,
    })
}

impl MatchStore for Store {
    fn create_match(&self, candidate: &str, opponent: &str, target_games: i64) -> CoreResult<i64> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO matches (candidate, opponent, target_games) VALUES (?,?,?)",
                rusqlite::params![candidate, opponent, target_games],
            )
            .map_err(StoreError::from)?;
            Ok(conn.last_insert_rowid())
        })
    }

    /// 进行中的对打（selfplay 混入查询；取最早一个，按 id 升序）。
    fn pending_match(&self) -> CoreResult<Option<Match>> {
        self.with_conn(|conn| {
            conn.query_row(
                &format!(
                    "SELECT {MATCH_COLUMNS} FROM matches WHERE status=? ORDER BY id LIMIT 1"
                ),
                rusqlite::params![match_status::RUNNING],
                scan_match_row,
            )
            .optional()
            .map_err(StoreError::from)
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

    /// 对打列表（WebUI /api/matches）：running 优先，其余按 id 倒序。
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
            rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
        })
    }

    /// 原子累加六桶成对计数与局数。
    ///
    /// **bool 返回值语义（对照 Go 版 `AddMatchGames`）**：true = 累加已应用且返回的
    /// Match 是累加后的完整行快照（judgeMatch 判停用）；false = match 不存在或
    /// 已终结（`WHERE status='running'` 拒绝对已完结 match 继续累加），此时返回
    /// 零值 Match（Go 版返回 (nil, false, nil)——Rust 端口无 Option，调用方仅应在
    /// true 分支消费 Match）。累加下推为单条 SQL `UPDATE ... RETURNING`，
    /// 多 worker 并发为同一 match 上报（混入模式常态）时无「后写覆盖先写」丢失。
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
                    delta.0[0], delta.0[1], delta.0[2], delta.0[3], delta.0[4], delta.0[5],
                    games, id
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

    /// 条件结转（CAS）：仅当 match 仍为 running 时置 concluded。
    /// 返回本次调用是否完成了结转——并发双判停时只有一个调用方拿到 true，
    /// 晋级/拒绝副作用只执行一次（防双晋级）。对照 Go 版 ConcludeMatch
    /// （RowsAffected>0）。match 不存在时同样返回 false（与 Go 版一致）。
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
}

/// add_match_games false 分支的零值占位（调用方不应消费其内容）。
/// （Match 定义于 bqs-core，此处局部构造以满足端口签名。）
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
        // 外键约束开启：matches REFERENCES networks(sha)，须先登记。
        NS::register_network(&*s, "c1", None, None, "onnx").unwrap();
        NS::register_network(&*s, "o1", None, None, "onnx").unwrap();

        let id = MS::create_match(&*s, "c1", "o1", 100).unwrap();
        let m = MS::get_match(&*s, id).unwrap().unwrap();
        assert_eq!(m.candidate, "c1");
        assert_eq!(m.target_games, 100);
        assert!(m.running());

        // pending_match：最早一个 running
        let id2 = MS::create_match(&*s, "c1", "o1", 50).unwrap();
        assert_eq!(MS::pending_match(&*s).unwrap().unwrap().id, id);
        MS::conclude_match(&*s, id).unwrap();
        assert_eq!(MS::pending_match(&*s).unwrap().unwrap().id, id2);

        // 列表：running 优先，concluded 按 id 倒序
        let list = MS::list_matches(&*s).unwrap();
        assert_eq!(list[0].id, id2);
        assert_eq!(list[1].id, id);
        assert_eq!(MS::get_match(&*s, 999).unwrap(), None);
    }

    /// add_match_games 原子累加：running 可累加；concluded 拒绝（bool=false）；
    /// 不存在的 id 亦为 false。
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

        // 累加（RETURNING 快照为累加后状态）
        let (m, ok) = MS::add_match_games(&*s, id, Hexanomial([1, 0, 0, 0, 0, 0]), 1).unwrap();
        assert!(ok);
        assert_eq!(m.pairs, Hexanomial([2, 2, 3, 4, 5, 6]));
        assert_eq!(m.num_games, 22);

        // 结转后拒绝继续累加
        assert!(MS::conclude_match(&*s, id).unwrap());
        assert!(!MS::conclude_match(&*s, id).unwrap()); // CAS：二次结转 false
        let (m, ok) = MS::add_match_games(&*s, id, Hexanomial::ZERO, 1).unwrap();
        assert!(!ok);
        assert_eq!(m.num_games, 0); // false 分支返回零值占位，不携带行数据

        // 不存在的 match
        let (_, ok) = MS::add_match_games(&*s, 999, Hexanomial::ZERO, 1).unwrap();
        assert!(!ok);
    }

    /// conclude_match 返回值：仅在 running→concluded 转换时 true（防双晋级）。
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
}
