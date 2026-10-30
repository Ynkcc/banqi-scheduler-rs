//! 【所有权：infra 成员】eval 端口：eval_queue（流式评估作业）与 eval_results
//! （绝对强度评估趋势）。SQL 逐条对照 Go 版 internal/store/eval_queue.go / eval.go。

use bqs_core::ports::{EvalStore, StoreCounts};
use bqs_core::types::{EvalDelta, EvalQueueEntry, EvalResult};
use bqs_core::CoreResult;

use crate::{Store, StoreError};

use rusqlite::OptionalExtension;

/// eval_results 表的统一列序（对照 Go 版 evalResultColumns）。
const EVAL_RESULT_COLUMNS: &str = "id, network_sha, opponent_spec, wins, draws, losses, \
     red_wins, black_wins, draw_no_capture, draw_truncation, num_games, avg_moves, created_at";

/// eval_queue 全列序（对照 Go 版 LoadEvalQueue 的 14 列 + v10 追加的 opponent_sha）。
const EVAL_QUEUE_FULL_COLUMNS: &str = "network_sha, opponent_spec, target_games, \
     per_task_games, attempts, wins, draws, losses, \
     red_wins, black_wins, draw_no_capture, draw_truncation, num_games, created_at, \
     opponent_sha";

fn scan_queue_full(row: &rusqlite::Row<'_>) -> rusqlite::Result<EvalQueueEntry> {
    Ok(EvalQueueEntry {
        network_sha: row.get(0)?,
        opponent_spec: row.get(1)?,
        target_games: row.get(2)?,
        per_task_games: row.get(3)?,
        attempts: row.get(4)?,
        wins: row.get(5)?,
        draws: row.get(6)?,
        losses: row.get(7)?,
        red_wins: row.get(8)?,
        black_wins: row.get(9)?,
        draw_no_capture: row.get(10)?,
        draw_truncation: row.get(11)?,
        num_games: row.get(12)?,
        created_at: row.get(13)?,
        opponent_sha: row.get(14)?,
    })
}

fn scan_eval_result(row: &rusqlite::Row<'_>) -> rusqlite::Result<EvalResult> {
    Ok(EvalResult {
        id: row.get(0)?,
        network_sha: row.get(1)?,
        opponent_spec: row.get(2)?,
        wins: row.get(3)?,
        draws: row.get(4)?,
        losses: row.get(5)?,
        red_wins: row.get(6)?,
        black_wins: row.get(7)?,
        draw_no_capture: row.get(8)?,
        draw_truncation: row.get(9)?,
        num_games: row.get(10)?,
        avg_moves: row.get(11)?,
        created_at: row.get(12)?,
    })
}

impl EvalStore for Store {
    /// 回填进程内待办队列（含全部未收尾任务），按 created_at / sha / spec 排序。
    fn load_eval_queue(&self) -> CoreResult<Vec<EvalQueueEntry>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {EVAL_QUEUE_FULL_COLUMNS} FROM eval_queue \
					 ORDER BY created_at, network_sha, opponent_spec"
                ))
                .map_err(StoreError::from)?;
            let rows = stmt
                .query_map([], scan_queue_full)
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
        })
    }

    /// 入队评估作业（INSERT OR IGNORE：已在队即 no-op）；返回 false = 已存在（幂等）。
    fn enqueue_eval_job(
        &self,
        network_sha: &str,
        spec: &str,
        opponent_sha: &str,
        target_games: i64,
        per_task_games: i64,
    ) -> CoreResult<bool> {
        let now = self.now();
        self.with_conn(|conn| {
            let n = conn
                .execute(
                    "INSERT OR IGNORE INTO eval_queue
						(network_sha, opponent_spec, target_games, per_task_games, attempts, created_at, opponent_sha)
					 VALUES (?,?,?,?,0,?,?)",
                    rusqlite::params![network_sha, spec, target_games, per_task_games, now, opponent_sha],
                )
                .map_err(StoreError::from)?;
            Ok(n > 0)
        })
    }

    /// 收尾时移除待办行。缺席视为成功：避免和「先删后插入」竞态产生误导。
    fn delete_eval_job(&self, network_sha: &str, spec: &str) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "DELETE FROM eval_queue WHERE network_sha = ? AND opponent_spec = ?",
                rusqlite::params![network_sha, spec],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }

    /// 每次下发 attempts++（无进度下发计数，上限判定在调度层完成）。
    /// 行不存在为真故障（对照 Go 版 IncEvalJobAttempts 的 ErrNoRows 路径）。
    fn inc_eval_job_attempts(&self, network_sha: &str, spec: &str) -> CoreResult<i64> {
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN").map_err(StoreError::from)?;
            let read = conn
                .query_row(
                    "SELECT attempts FROM eval_queue WHERE network_sha = ? AND opponent_spec = ?",
                    rusqlite::params![network_sha, spec],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(StoreError::from);
            match read {
                Ok(cur) => {
                    let next = cur + 1;
                    let write = conn
                        .execute(
                            "UPDATE eval_queue SET attempts = ? WHERE network_sha = ? AND opponent_spec = ?",
                            rusqlite::params![next, network_sha, spec],
                        )
                        .map_err(StoreError::from);
                    match write {
                        Ok(_) => conn.execute_batch("COMMIT").map_err(StoreError::from)?,
                        Err(e) => {
                            let _ = conn.execute_batch("ROLLBACK");
                            return Err(e.into());
                        }
                    }
                    Ok(next)
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    Err(e.into())
                }
            }
        })
    }

    /// 评估任务上报增量累加（原子 UPDATE，含 attempts 清零——有进度即视为健康）。
    /// 返回累加后的行，供调度层判定「累计 ≥ target 即收尾」。
    /// 行不存在（已被收尾/丢弃的迟到上报）返回错误，调用方按忽略处理。
    fn accumulate_eval_job(
        &self,
        network_sha: &str,
        spec: &str,
        delta: EvalDelta,
    ) -> CoreResult<EvalQueueEntry> {
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN").map_err(StoreError::from)?;
            let res = (|| -> Result<EvalQueueEntry, StoreError> {
                let n = conn.execute(
                    "UPDATE eval_queue SET
						wins = wins + ?, draws = draws + ?, losses = losses + ?,
						red_wins = red_wins + ?, black_wins = black_wins + ?,
						draw_no_capture = draw_no_capture + ?, draw_truncation = draw_truncation + ?,
						num_games = num_games + ?, attempts = 0
					 WHERE network_sha = ? AND opponent_spec = ?",
                    rusqlite::params![
                        delta.wins, delta.draws, delta.losses,
                        delta.red_wins, delta.black_wins, delta.draw_no_capture, delta.draw_truncation,
                        delta.games, network_sha, spec
                    ],
                )?;
                if n == 0 {
                    return Err(StoreError::Sqlite(rusqlite::Error::QueryReturnedNoRows));
                }
                conn.query_row(
                    "SELECT target_games, per_task_games, attempts, wins, draws, losses,
						red_wins, black_wins, draw_no_capture, draw_truncation, num_games
					 FROM eval_queue WHERE network_sha = ? AND opponent_spec = ?",
                    rusqlite::params![network_sha, spec],
                    |row| {
                        Ok(EvalQueueEntry {
                            network_sha: network_sha.to_string(),
                            opponent_spec: spec.to_string(),
                            opponent_sha: String::new(),
                            target_games: row.get(0)?,
                            per_task_games: row.get(1)?,
                            attempts: row.get(2)?,
                            wins: row.get(3)?,
                            draws: row.get(4)?,
                            losses: row.get(5)?,
                            red_wins: row.get(6)?,
                            black_wins: row.get(7)?,
                            draw_no_capture: row.get(8)?,
                            draw_truncation: row.get(9)?,
                            num_games: row.get(10)?,
                            created_at: 0,
                        })
                    },
                )
                .map_err(StoreError::from)
            })();
            match res {
                Ok(e) => {
                    conn.execute_batch("COMMIT").map_err(StoreError::from)?;
                    Ok(e)
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    Err(e.into())
                }
            }
        })
    }

    /// 读取一条待办的当前累加状态；不存在返回 None。
    fn get_eval_job(&self, network_sha: &str, spec: &str) -> CoreResult<Option<EvalQueueEntry>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT target_games, per_task_games, attempts, wins, draws, losses,
					red_wins, black_wins, draw_no_capture, draw_truncation, num_games, created_at
				 FROM eval_queue WHERE network_sha = ? AND opponent_spec = ?",
                rusqlite::params![network_sha, spec],
                |row| {
                    Ok(EvalQueueEntry {
                        network_sha: network_sha.to_string(),
                        opponent_spec: spec.to_string(),
                        opponent_sha: String::new(),
                        target_games: row.get(0)?,
                        per_task_games: row.get(1)?,
                        attempts: row.get(2)?,
                        wins: row.get(3)?,
                        draws: row.get(4)?,
                        losses: row.get(5)?,
                        red_wins: row.get(6)?,
                        black_wins: row.get(7)?,
                        draw_no_capture: row.get(8)?,
                        draw_truncation: row.get(9)?,
                        num_games: row.get(10)?,
                        created_at: row.get(11)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// 聚合落库（UNIQUE(network_sha, opponent_spec) 覆盖去重：同版本的重复评估
    /// 覆盖旧值，避免趋势序列出现重复点）。
    fn upsert_eval_result(&self, r: EvalResult) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO eval_results (network_sha, opponent_spec, wins, draws, losses,
					red_wins, black_wins, draw_no_capture, draw_truncation, num_games, avg_moves, created_at)
				 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
				 ON CONFLICT(network_sha, opponent_spec) DO UPDATE SET
					wins=excluded.wins, draws=excluded.draws, losses=excluded.losses,
					red_wins=excluded.red_wins, black_wins=excluded.black_wins,
					draw_no_capture=excluded.draw_no_capture, draw_truncation=excluded.draw_truncation,
					num_games=excluded.num_games, avg_moves=excluded.avg_moves,
					created_at=excluded.created_at",
                rusqlite::params![
                    r.network_sha, r.opponent_spec, r.wins, r.draws, r.losses,
                    r.red_wins, r.black_wins, r.draw_no_capture, r.draw_truncation,
                    r.num_games, r.avg_moves, r.created_at
                ],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }

    fn get_eval_result(
        &self,
        network_sha: &str,
        opponent_spec: &str,
    ) -> CoreResult<Option<EvalResult>> {
        self.with_conn(|conn| {
            conn.query_row(
                &format!(
                    "SELECT {EVAL_RESULT_COLUMNS} FROM eval_results \
					 WHERE network_sha=? AND opponent_spec=?"
                ),
                rusqlite::params![network_sha, opponent_spec],
                scan_eval_result,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// 按对手的版本序列（升序，老→新，WebUI 趋势图与「连续 N 次无提升」判据的数据源）。
    fn eval_trend(&self, opponent_spec: &str, limit: i64) -> CoreResult<Vec<EvalResult>> {
        let limit = if limit <= 0 { 20 } else { limit };
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {EVAL_RESULT_COLUMNS} FROM (
						SELECT {EVAL_RESULT_COLUMNS} FROM eval_results
						WHERE opponent_spec=? ORDER BY id DESC LIMIT ?
					) ORDER BY id ASC"
                ))
                .map_err(StoreError::from)?;
            let rows = stmt
                .query_map(rusqlite::params![opponent_spec, limit], scan_eval_result)
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
        })
    }

    // ---- [api 需求] 追加（SQL 对照 Go internal/store 的对应函数）----

    /// WebUI 首页跨表计数（[api 需求] Go: store.Counts，六项 COUNT + episode_games SUM）。
    fn counts(&self) -> CoreResult<StoreCounts> {
        self.with_conn(|conn| {
            let one = |q: &str| -> Result<i64, StoreError> {
                conn.query_row(q, [], |row| row.get::<_, i64>(0)).map_err(StoreError::from)
            };
            Ok(StoreCounts {
                networks: one("SELECT COUNT(*) FROM networks")?,
                candidates: one("SELECT COUNT(*) FROM networks WHERE status='candidate'")?,
                matches_running: one("SELECT COUNT(*) FROM matches WHERE status='running'")?,
                episodes: one("SELECT COUNT(*) FROM episodes")?,
                workers: one("SELECT COUNT(*) FROM workers")?,
                eval_results: one("SELECT COUNT(*) FROM eval_results")?,
                episode_games: one("SELECT COALESCE(SUM(game_count),0) FROM episodes")?,
            })
        })
    }

    /// 评估明细倒序列表（[api 需求] Go: store.ListEvalResults；新→旧，limit<=0 不限）。
    fn list_eval_results(&self, limit: i64) -> CoreResult<Vec<EvalResult>> {
        self.with_conn(|conn| {
            let query = if limit > 0 {
                format!("SELECT {EVAL_RESULT_COLUMNS} FROM eval_results ORDER BY id DESC LIMIT ?")
            } else {
                format!("SELECT {EVAL_RESULT_COLUMNS} FROM eval_results ORDER BY id DESC")
            };
            let mut stmt = conn.prepare(&query).map_err(StoreError::from)?;
            let rows = if limit > 0 {
                stmt.query_map(rusqlite::params![limit], scan_eval_result)
                    .map_err(StoreError::from)?
            } else {
                stmt.query_map([], scan_eval_result).map_err(StoreError::from)?
            };
            rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
        })
    }

    /// 出现过的对手标识（[api 需求] Go: store.ListEvalOpponents，按首次出现顺序，
    /// 供 WebUI 分组展示）。
    fn list_eval_opponents(&self) -> CoreResult<Vec<String>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT opponent_spec FROM eval_results GROUP BY opponent_spec ORDER BY MIN(id)",
                )
                .map_err(StoreError::from)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
        })
    }
}

#[cfg(test)]
mod tests {
    use bqs_core::ports::EvalStore as ES;
    use bqs_core::types::{EvalDelta, EvalResult};

    fn result(sha: &str, spec: &str, wins: i64, created_at: i64) -> EvalResult {
        EvalResult {
            id: 0,
            network_sha: sha.into(),
            opponent_spec: spec.into(),
            wins,
            draws: 0,
            losses: 0,
            num_games: wins,
            avg_moves: 0.0,
            created_at,
            red_wins: 0,
            black_wins: 0,
            draw_no_capture: 0,
            draw_truncation: 0,
        }
    }

    #[test]
    fn queue_enqueue_accumulate_delete() {
        let (_dir, s) = crate::testutil::open_temp_store();

        // 入队幂等
        assert!(ES::enqueue_eval_job(&*s, "n1", "rule:x", "", 100, 20).unwrap());
        assert!(!ES::enqueue_eval_job(&*s, "n1", "rule:x", "", 100, 20).unwrap());

        // 累加增量（attempts 清零）
        ES::inc_eval_job_attempts(&*s, "n1", "rule:x").unwrap();
        ES::inc_eval_job_attempts(&*s, "n1", "rule:x").unwrap();
        let e = ES::accumulate_eval_job(
            &*s,
            "n1",
            "rule:x",
            EvalDelta { wins: 3, draws: 1, losses: 2, games: 6, red_wins: 2, black_wins: 1, draw_no_capture: 1, draw_truncation: 0 },
        )
        .unwrap();
        assert_eq!((e.wins, e.draws, e.losses, e.num_games), (3, 1, 2, 6));
        assert_eq!(e.attempts, 0); // 有进度即清零
        assert_eq!(e.red_wins, 2);
        assert_eq!(e.target_games, 100);

        // 读取
        let g = ES::get_eval_job(&*s, "n1", "rule:x").unwrap().unwrap();
        assert_eq!(g.num_games, 6);
        assert!(ES::get_eval_job(&*s, "n1", "nope").unwrap().is_none());

        // 迟到上报（行已删）为故障
        ES::delete_eval_job(&*s, "n1", "rule:x").unwrap();
        assert!(ES::accumulate_eval_job(&*s, "n1", "rule:x", EvalDelta::default()).is_err());
        assert!(ES::inc_eval_job_attempts(&*s, "n1", "rule:x").is_err());
        // 删除缺席视为成功
        ES::delete_eval_job(&*s, "n1", "rule:x").unwrap();

        // load 排序：created_at, sha, spec
        ES::enqueue_eval_job(&*s, "b", "rule", "", 10, 5).unwrap();
        ES::enqueue_eval_job(&*s, "a", "rule", "", 10, 5).unwrap();
        let all = ES::load_eval_queue(&*s).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].network_sha, "a");
    }

    #[test]
    fn results_upsert_and_trend() {
        let (_dir, s) = crate::testutil::open_temp_store();

        ES::upsert_eval_result(&*s, result("n1", "rule:x", 60, 100)).unwrap();
        ES::upsert_eval_result(&*s, result("n2", "rule:x", 70, 200)).unwrap();
        // 同 (sha, spec) 覆盖（去重）
        ES::upsert_eval_result(&*s, result("n1", "rule:x", 65, 300)).unwrap();

        let r = ES::get_eval_result(&*s, "n1", "rule:x").unwrap().unwrap();
        assert_eq!(r.wins, 65);
        assert_eq!(r.created_at, 300);
        assert!(ES::get_eval_result(&*s, "n1", "nope").unwrap().is_none());

        // trend：升序（老→新），limit 截取最近 N 条再反转为升序
        ES::upsert_eval_result(&*s, result("n3", "rule:x", 80, 400)).unwrap();
        let trend = ES::eval_trend(&*s, "rule:x", 2).unwrap();
        assert_eq!(trend.len(), 2);
        assert_eq!(trend[0].network_sha, "n2");
        assert_eq!(trend[1].network_sha, "n3");
        // limit<=0 → 默认 20
        assert_eq!(ES::eval_trend(&*s, "rule:x", 0).unwrap().len(), 3);
        assert!(ES::eval_trend(&*s, "other", 5).unwrap().is_empty());
    }

    /// [api 需求] list_eval_results / list_eval_opponents（Go: ListEvalResults / ListEvalOpponents）。
    #[test]
    fn list_results_and_opponents() {
        let (_dir, s) = crate::testutil::open_temp_store();
        use bqs_core::types::EvalResult as ER;

        let mk = |sha: &str, spec: &str, wins: i64| ER {
            id: 0,
            network_sha: sha.into(),
            opponent_spec: spec.into(),
            wins,
            draws: 0,
            losses: 0,
            num_games: wins,
            avg_moves: 0.0,
            created_at: 1,
            red_wins: 0,
            black_wins: 0,
            draw_no_capture: 0,
            draw_truncation: 0,
        };
        ES::upsert_eval_result(&*s, mk("n1", "rule:x", 60)).unwrap();
        ES::upsert_eval_result(&*s, mk("n2", "rule:y", 70)).unwrap();
        ES::upsert_eval_result(&*s, mk("n3", "rule:x", 80)).unwrap();

        // 倒序（新→旧 = id DESC）
        let all = ES::list_eval_results(&*s, 0).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].network_sha, "n3");
        assert_eq!(all[2].network_sha, "n1");
        // limit 截取
        let top = ES::list_eval_results(&*s, 2).unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].network_sha, "n3");

        // 对手去重，按首次出现顺序（MIN(id)）：rule:x 先于 rule:y
        let opponents = ES::list_eval_opponents(&*s).unwrap();
        assert_eq!(opponents, vec!["rule:x", "rule:y"]);
    }

    /// [api 需求] counts（Go: store.Counts，七项跨表计数）。
    #[test]
    fn counts_across_tables() {
        let (_dir, s) = crate::testutil::open_temp_store();
        use bqs_core::ports::{EpisodeStore as ES2, MatchStore as MS, NetworkStore as NS, WorkerStore as WS};
        use bqs_core::types::EpisodeDraft;

        NS::register_network(&*s, "best1", None, None, "onnx").unwrap();
        NS::register_network(&*s, "cand1", None, None, "onnx").unwrap();
        NS::register_network(&*s, "cand2", None, None, "onnx").unwrap();
        NS::promote_best(&*s, "best1").unwrap(); // best1 → best，其余仍 candidate
        let m = MS::create_match(&*s, "cand1", "best1", 10).unwrap();
        MS::create_match(&*s, "cand2", "best1", 10).unwrap();
        MS::conclude_match(&*s, m).unwrap();
        for i in 0..3 {
            ES2::insert_episode(
                &*s,
                EpisodeDraft { task_id: format!("tc-{i}"), game_count: 4, ..Default::default() },
            )
            .unwrap();
        }
        WS::touch_worker(&*s, "w1", 4, 10, "v1", 8192, "ok", "").unwrap();

        let c = ES::counts(&*s).unwrap();
        assert_eq!(c.networks, 3);
        assert_eq!(c.candidates, 2); // best1 已晋级 best
        assert_eq!(c.matches_running, 1);
        assert_eq!(c.episodes, 3);
        assert_eq!(c.episode_games, 12);
        assert_eq!(c.workers, 1);
        assert_eq!(c.eval_results, 0);
    }
}
