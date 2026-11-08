#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    #[test]
    fn legacy_go_db_upgrades_to_v8() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 CREATE TABLE networks (
                    sha TEXT PRIMARY KEY, parent_sha TEXT, created_at INTEGER NOT NULL,
                    is_best INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'candidate', notes TEXT
                 );
                 CREATE TABLE matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate TEXT NOT NULL REFERENCES networks(sha),
                    opponent TEXT NOT NULL REFERENCES networks(sha),
                    status TEXT NOT NULL DEFAULT 'running',
                    pair_ll INTEGER NOT NULL DEFAULT 0, pair_ld INTEGER NOT NULL DEFAULT 0,
                    pair_dd INTEGER NOT NULL DEFAULT 0, pair_dw INTEGER NOT NULL DEFAULT 0,
                    pair_ww INTEGER NOT NULL DEFAULT 0,
                    num_games INTEGER NOT NULL DEFAULT 0, target_games INTEGER NOT NULL
                 );
                 CREATE TABLE eval_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, network_sha TEXT NOT NULL,
                    opponent_spec TEXT NOT NULL, wins INTEGER NOT NULL DEFAULT 0,
                    draws INTEGER NOT NULL DEFAULT 0, losses INTEGER NOT NULL DEFAULT 0,
                    num_games INTEGER NOT NULL DEFAULT 0, avg_moves REAL NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL, UNIQUE(network_sha, opponent_spec)
                 );
                 CREATE TABLE episodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, worker_id TEXT NOT NULL,
                    task_id TEXT NOT NULL, network_sha TEXT NOT NULL,
                    game_count INTEGER NOT NULL, total_steps INTEGER NOT NULL,
                    winner INTEGER NOT NULL, object_key TEXT NOT NULL, created_at INTEGER NOT NULL
                 );
                 CREATE TABLE workers (
                    id TEXT PRIMARY KEY, last_seen INTEGER NOT NULL, threads INTEGER NOT NULL DEFAULT 0,
                    completed_games INTEGER NOT NULL DEFAULT 0
                 );
                 CREATE TABLE trainers (
                    id TEXT PRIMARY KEY, last_seen INTEGER NOT NULL, variant TEXT NOT NULL DEFAULT '',
                    kind INTEGER NOT NULL DEFAULT 0, consumed_key TEXT NOT NULL DEFAULT '',
                    consumed_id INTEGER NOT NULL DEFAULT 0, samples_consumed INTEGER NOT NULL DEFAULT 0,
                    backlog INTEGER NOT NULL DEFAULT 0, buffered INTEGER NOT NULL DEFAULT 0
                 );
                 CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 -- 旧分片 eval_queue（两片：一片 done 带计数，一片未完成）
                 CREATE TABLE eval_queue (
                    network_sha TEXT NOT NULL, opponent_spec TEXT NOT NULL,
                    shard_idx INTEGER NOT NULL, shard_total INTEGER NOT NULL, shard_games INTEGER NOT NULL,
                    done INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
                    wins INTEGER NOT NULL DEFAULT 0, draws INTEGER NOT NULL DEFAULT 0,
                    losses INTEGER NOT NULL DEFAULT 0, num_games INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL, PRIMARY KEY (network_sha, opponent_spec, shard_idx)
                 );
                 INSERT INTO networks (sha, parent_sha, created_at, is_best, status, notes)
                    VALUES ('old-best', NULL, 100, 1, 'best', 'gen0');
                 INSERT INTO matches (candidate, opponent, target_games, pair_dd, num_games)
                    VALUES ('old-best', 'old-best', 10, 3, 6);
                 INSERT INTO episodes (worker_id, task_id, network_sha, game_count, total_steps, winner, object_key, created_at)
                    VALUES ('w0', 'legacy-task', 'old-best', 2, 40, 1, 'legacy/key.epb.gz', 100);
                 INSERT INTO eval_queue VALUES
                    ('old-cand', 'rule:x', 0, 2, 8, 1, 0, 3, 2, 3, 8, 50),
                    ('old-cand', 'rule:x', 1, 2, 8, 0, 0, 0, 0, 0, 0, 60);
                 ",
            )
            .unwrap();
        }

        let store = crate::Store::open(path.to_str().unwrap()).unwrap();

        let conn = store.conn.lock().unwrap();

        let versions: Vec<i64> = {
            let mut stmt = conn
                .prepare("SELECT version FROM schema_migrations ORDER BY version")
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(
            versions,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
        );

        for (table, col) in [
            ("workers", "note"),
            ("workers", "health_status"),
            ("trainers", "progress"),
            ("networks", "format"),
            ("episodes", "kind"),
            ("episodes", "opponent_sha"),
            ("episodes", "match_id"),
            ("episodes", "red_wins"),
            ("episodes", "draw_truncation"),
            ("episodes", "seed_base"),
            ("episodes", "pair_seed_refresh"),
            ("episodes", "label_object_key"),
            ("episodes", "label_sha256"),
            ("episodes", "label_size"),
            ("matches", "pair_wl"),
        ] {
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?",
                    rusqlite::params![table, col],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "{table}.{col} 应已补列");
        }

        let dropped: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('eval_queue','eval_results')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dropped, 0, "eval 表应已在 v12 被 drop");

        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM episodes WHERE task_id='legacy-task'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
        let best_col: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('networks') WHERE name='is_best'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(best_col, 0, "v17 后 networks.is_best 列应已移除");
        let old_best: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM networks WHERE sha='old-best'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(old_best, 1, "v17 后旧网络行数据应保留");
        let wl: i64 = conn
            .query_row(
                "SELECT pair_wl FROM matches WHERE candidate='old-best'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(wl, 0, "旧 WL 不可恢复，置 0");

        let (num_games, pair_dd): (i64, i64) = conn
            .query_row(
                "SELECT num_games, pair_dd FROM matches WHERE candidate='old-best'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((num_games, pair_dd), (6, 3), "v11 重建后对发行数据无损");

        let notnull: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('matches')
				 WHERE name IN ('candidate','opponent') AND \"notnull\" = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(notnull, 0, "v11 后 matches sha 列应为可空");
    }

    #[test]
    fn duplicate_task_id_degrades_gracefully() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dup.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE episodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, worker_id TEXT NOT NULL,
                    task_id TEXT NOT NULL, network_sha TEXT NOT NULL,
                    game_count INTEGER NOT NULL, total_steps INTEGER NOT NULL,
                    winner INTEGER NOT NULL, object_key TEXT NOT NULL, created_at INTEGER NOT NULL
                 );
                 INSERT INTO episodes (worker_id, task_id, network_sha, game_count, total_steps, winner, object_key, created_at)
                    VALUES ('w', 'same-task', 'n', 1, 1, 0, 'k1', 1), ('w', 'same-task', 'n', 1, 1, 0, 'k2', 2);",
            )
            .unwrap();
        }

        let store = crate::Store::open(path.to_str().unwrap()).unwrap();

        use bqs_core::ports::EpisodeStore as _;
        let e = store.get_episode_by_task("same-task").unwrap().unwrap();
        assert_eq!(e.object_key, "k2");
    }

    #[test]
    fn legacy_whole_package_eval_queue_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("whole.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE networks (sha TEXT PRIMARY KEY, parent_sha TEXT, created_at INTEGER NOT NULL,
                    is_best INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'candidate', notes TEXT);
                 CREATE TABLE matches (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate TEXT NOT NULL REFERENCES networks(sha), opponent TEXT NOT NULL REFERENCES networks(sha),
                    status TEXT NOT NULL DEFAULT 'running', pair_ll INTEGER NOT NULL DEFAULT 0,
                    pair_ld INTEGER NOT NULL DEFAULT 0, pair_dd INTEGER NOT NULL DEFAULT 0,
                    pair_dw INTEGER NOT NULL DEFAULT 0, pair_ww INTEGER NOT NULL DEFAULT 0,
                    num_games INTEGER NOT NULL DEFAULT 0, target_games INTEGER NOT NULL);
                 CREATE TABLE eval_results (id INTEGER PRIMARY KEY AUTOINCREMENT, network_sha TEXT NOT NULL,
                    opponent_spec TEXT NOT NULL, wins INTEGER NOT NULL DEFAULT 0, draws INTEGER NOT NULL DEFAULT 0,
                    losses INTEGER NOT NULL DEFAULT 0, num_games INTEGER NOT NULL DEFAULT 0,
                    avg_moves REAL NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, UNIQUE(network_sha, opponent_spec));
                 CREATE TABLE episodes (id INTEGER PRIMARY KEY AUTOINCREMENT, worker_id TEXT NOT NULL,
                    task_id TEXT NOT NULL, network_sha TEXT NOT NULL, game_count INTEGER NOT NULL,
                    total_steps INTEGER NOT NULL, winner INTEGER NOT NULL, object_key TEXT NOT NULL,
                    created_at INTEGER NOT NULL);
                 CREATE TABLE workers (id TEXT PRIMARY KEY, last_seen INTEGER NOT NULL,
                    threads INTEGER NOT NULL DEFAULT 0, completed_games INTEGER NOT NULL DEFAULT 0);
                 CREATE TABLE trainers (id TEXT PRIMARY KEY, last_seen INTEGER NOT NULL, variant TEXT NOT NULL DEFAULT '',
                    kind INTEGER NOT NULL DEFAULT 0, consumed_key TEXT NOT NULL DEFAULT '',
                    consumed_id INTEGER NOT NULL DEFAULT 0, samples_consumed INTEGER NOT NULL DEFAULT 0,
                    backlog INTEGER NOT NULL DEFAULT 0, buffered INTEGER NOT NULL DEFAULT 0);
                 CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 CREATE TABLE eval_queue (network_sha TEXT NOT NULL, opponent_spec TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL,
                    PRIMARY KEY (network_sha, opponent_spec));
                 INSERT INTO eval_queue VALUES ('n', 'rule:y', 4, 77);
                 INSERT INTO settings VALUES ('eval_promotions', '9');",
            )
            .unwrap();
        }
        let _store = crate::Store::open(path.to_str().unwrap()).unwrap();
        let conn = _store.conn.lock().unwrap();
        let dropped: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('eval_queue','eval_results')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dropped, 0, "eval 表应已在 v12 被 drop");
        let stale_settings: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM settings WHERE key LIKE 'eval%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stale_settings, 0, "eval_* 设置键应已清理");
    }
}
