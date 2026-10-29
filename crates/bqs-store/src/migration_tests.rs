//! 迁移链单测（追加在文件尾）：模拟 Go 版历史旧库从 v0 补跑全链。

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    /// 模拟 Go 版旧库（迁移链启用前的探测式 schema）从 v0 补跑全链：
    /// - 旧 networks / episodes / workers / trainers / matches（缺补列）；
    /// - 旧 eval_queue 为**分片结构**（有 shard_idx，无 target_games）；
    /// - 无 schema_migrations 记账表。
    /// 打开后应收敛到 v8 终态且数据按 v3 折叠语义保留。
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

        // Rust 版打开 Go 版旧库：补跑 v1..v8 全链。
        let store = crate::Store::open(path.to_str().unwrap()).unwrap();

        let conn = store.conn.lock().unwrap();
        // 记账表 v1..v8 全部登记
        let versions: Vec<i64> = {
            let mut stmt = conn.prepare("SELECT version FROM schema_migrations ORDER BY version").unwrap();
            stmt.query_map([], |r| r.get(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
        };
        assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8]);

        // 列探测补列终态：逐表抽查 v2/v4/v5/v6/v8 补的列
        for (table, col) in [
            ("workers", "note"), ("workers", "health_status"), ("trainers", "progress"),
            ("networks", "format"), ("episodes", "kind"),
            ("episodes", "opponent_sha"), ("episodes", "match_id"),
            ("episodes", "red_wins"), ("episodes", "draw_truncation"),
            ("eval_results", "red_wins"), ("matches", "pair_wl"),
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

        // v3 分片折叠：done 分片计数求和（wins=3 draws=2 losses=3 num_games=8），
        // target = 片数 × 每片局数 = 2×8 = 16，attempts 求和 = 0，created_at 取 MIN。
        let row: (i64, i64, i64, i64, i64, i64, i64) = conn
            .query_row(
                "SELECT target_games, per_task_games, attempts, wins, draws, losses, num_games
                 FROM eval_queue WHERE network_sha='old-cand' AND opponent_spec='rule:x'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
            )
            .unwrap();
        assert_eq!(row, (16, 8, 0, 3, 2, 3, 8));

        // 旧数据保留
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM episodes WHERE task_id='legacy-task'", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 1);
        let best: i64 = conn.query_row("SELECT COUNT(*) FROM networks WHERE sha='old-best' AND is_best=1", [], |r| r.get(0)).unwrap();
        assert_eq!(best, 1);
        let wl: i64 = conn.query_row("SELECT pair_wl FROM matches WHERE candidate='old-best'", [], |r| r.get(0)).unwrap();
        assert_eq!(wl, 0, "旧 WL 不可恢复，置 0");
    }

    /// 旧库含重复 task_id（数据污染）：唯一索引创建失败仅告警，不阻断启动。
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
        // 打开不报错（唯一索引创建失败降级告警）
        let store = crate::Store::open(path.to_str().unwrap()).unwrap();
        // GetEpisodeByTask 仍工作（取最新一条）
        use bqs_core::ports::EpisodeStore as _;
        let e = store.get_episode_by_task("same-task").unwrap().unwrap();
        assert_eq!(e.object_key, "k2");
    }

    /// 旧 eval_queue 整包结构（无 shard_idx 无 target_games）→ 重建后 target=0，
    /// 调度层首扫按 0 局收尾清除。
    #[test]
    fn legacy_whole_package_eval_queue() {
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
                 INSERT INTO eval_queue VALUES ('n', 'rule:y', 4, 77);",
            )
            .unwrap();
        }
        let _store = crate::Store::open(path.to_str().unwrap()).unwrap();
        let conn = _store.conn.lock().unwrap();
        let row: (i64, i64) = conn
            .query_row(
                "SELECT target_games, attempts FROM eval_queue WHERE network_sha='n' AND opponent_spec='rule:y'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, (0, 4), "整包行 target=0，attempts 保留");
    }
}
