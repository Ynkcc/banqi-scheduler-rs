use rusqlite::Connection;

use crate::StoreError;

struct Migration {
    version: i64,
    name: &'static str,
    up: fn(&Connection) -> Result<(), StoreError>,
}

static MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "base tables",
        up: migrate_base_tables,
    },
    Migration {
        version: 2,
        name: "add columns",
        up: migrate_add_columns,
    },
    Migration {
        version: 3,
        name: "eval_queue streaming",
        up: migrate_eval_queue_streaming,
    },
    Migration {
        version: 4,
        name: "eval detail breakdown",
        up: migrate_eval_detail_breakdown,
    },
    Migration {
        version: 5,
        name: "episode opponent & match link",
        up: migrate_episode_opponent,
    },
    Migration {
        version: 6,
        name: "episode outcome breakdown",
        up: migrate_episode_breakdown,
    },
    Migration {
        version: 7,
        name: "feature packs",
        up: migrate_feature_packs,
    },
    Migration {
        version: 8,
        name: "matches hexanomial",
        up: migrate_matches_hexanomial,
    },
    Migration {
        version: 9,
        name: "unified jobs",
        up: migrate_jobs,
    },
    Migration {
        version: 10,
        name: "eval model opponent sha",
        up: migrate_eval_opponent_sha,
    },
    Migration {
        version: 11,
        name: "match sides nullable sha",
        up: migrate_match_sides_nullable,
    },
    Migration {
        version: 12,
        name: "drop eval tables",
        up: migrate_drop_eval_tables,
    },
    Migration {
        version: 13,
        name: "episode game identity index",
        up: migrate_episode_games,
    },
];

fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool, StoreError> {
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?",
            rusqlite::params![table, column],
            |row| row.get(0),
        )
        .map_err(|e| StoreError::Migration {
            version: -1,
            name: "column_exists",
            message: format!("pragma_table_info({table}): {e}"),
        })?;
    Ok(n > 0)
}

fn add_column_if_missing(
    conn: &Connection,
    table: &str,
    column: &str,
    ddl: &str,
) -> Result<(), StoreError> {
    if column_exists(conn, table, column)? {
        return Ok(());
    }
    conn.execute_batch(ddl).map_err(|e| StoreError::Migration {
        version: -1,
        name: "add_column_if_missing",
        message: format!("add {table}.{column}: {e}"),
    })
}

fn migrate_base_tables(conn: &Connection) -> Result<(), StoreError> {
    let stmts: &[&str] = &[
        "CREATE TABLE IF NOT EXISTS networks (
			sha TEXT PRIMARY KEY,
			parent_sha TEXT,
			created_at INTEGER NOT NULL,
			is_best INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'candidate',
			notes TEXT,
			format TEXT NOT NULL DEFAULT 'onnx'
		)",
        "CREATE TABLE IF NOT EXISTS matches (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			candidate TEXT REFERENCES networks(sha),
			opponent TEXT REFERENCES networks(sha),
			status TEXT NOT NULL DEFAULT 'running',
			pair_ll INTEGER NOT NULL DEFAULT 0,
			pair_ld INTEGER NOT NULL DEFAULT 0,
			pair_dd INTEGER NOT NULL DEFAULT 0,
			pair_wl INTEGER NOT NULL DEFAULT 0,
			pair_dw INTEGER NOT NULL DEFAULT 0,
			pair_ww INTEGER NOT NULL DEFAULT 0,
			num_games INTEGER NOT NULL DEFAULT 0,
			target_games INTEGER NOT NULL
		)",
        "CREATE TABLE IF NOT EXISTS eval_results (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			network_sha TEXT NOT NULL,
			opponent_spec TEXT NOT NULL,
			wins INTEGER NOT NULL DEFAULT 0,
			draws INTEGER NOT NULL DEFAULT 0,
			losses INTEGER NOT NULL DEFAULT 0,
			num_games INTEGER NOT NULL DEFAULT 0,
			avg_moves REAL NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			UNIQUE(network_sha, opponent_spec)
		)",
        "CREATE INDEX IF NOT EXISTS idx_eval_results_spec ON eval_results(opponent_spec, id)",
        "CREATE TABLE IF NOT EXISTS episodes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			worker_id TEXT NOT NULL,
			task_id TEXT NOT NULL,
			network_sha TEXT NOT NULL,
			game_count INTEGER NOT NULL,
			total_steps INTEGER NOT NULL,
			winner INTEGER NOT NULL,
			object_key TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			kind INTEGER NOT NULL DEFAULT 0
		)",
        "CREATE INDEX IF NOT EXISTS idx_episodes_network ON episodes(network_sha)",
        "CREATE INDEX IF NOT EXISTS idx_episodes_object_key ON episodes(object_key)",
        "CREATE TABLE IF NOT EXISTS workers (
			id TEXT PRIMARY KEY,
			last_seen INTEGER NOT NULL,
			threads INTEGER NOT NULL DEFAULT 0,
			completed_games INTEGER NOT NULL DEFAULT 0,
			client_version TEXT NOT NULL DEFAULT '',
			memory_mb INTEGER NOT NULL DEFAULT 0,
			health_status TEXT NOT NULL DEFAULT '',
			last_error TEXT NOT NULL DEFAULT ''
		)",
        "CREATE TABLE IF NOT EXISTS trainers (
			id TEXT PRIMARY KEY,
			last_seen INTEGER NOT NULL,
			variant TEXT NOT NULL DEFAULT '',
			kind INTEGER NOT NULL DEFAULT 0,
			consumed_key TEXT NOT NULL DEFAULT '',
			consumed_id INTEGER NOT NULL DEFAULT 0,
			samples_consumed INTEGER NOT NULL DEFAULT 0,
			backlog INTEGER NOT NULL DEFAULT 0,
			buffered INTEGER NOT NULL DEFAULT 0,
			health_status TEXT NOT NULL DEFAULT '',
			last_error TEXT NOT NULL DEFAULT '',
			progress TEXT NOT NULL DEFAULT ''
		)",
        "CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)",
        "CREATE TABLE IF NOT EXISTS eval_queue (
			network_sha TEXT NOT NULL,
			opponent_spec TEXT NOT NULL,
			target_games INTEGER NOT NULL,
			per_task_games INTEGER NOT NULL DEFAULT 0,
			attempts INTEGER NOT NULL DEFAULT 0,
			wins INTEGER NOT NULL DEFAULT 0,
			draws INTEGER NOT NULL DEFAULT 0,
			losses INTEGER NOT NULL DEFAULT 0,
			num_games INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (network_sha, opponent_spec)
		)",
    ];
    for q in stmts {
        conn.execute_batch(q).map_err(|e| StoreError::Migration {
            version: 1,
            name: "base tables",
            message: format!("exec {q:?}: {e}"),
        })?;
    }
    Ok(())
}

fn migrate_add_columns(conn: &Connection) -> Result<(), StoreError> {
    let add_column: &[(&str, &str, &str)] = &[
        (
            "workers",
            "client_version",
            "ALTER TABLE workers ADD COLUMN client_version TEXT NOT NULL DEFAULT ''",
        ),
        (
            "workers",
            "memory_mb",
            "ALTER TABLE workers ADD COLUMN memory_mb INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "workers",
            "note",
            "ALTER TABLE workers ADD COLUMN note TEXT NOT NULL DEFAULT ''",
        ),
        (
            "workers",
            "health_status",
            "ALTER TABLE workers ADD COLUMN health_status TEXT NOT NULL DEFAULT ''",
        ),
        (
            "workers",
            "last_error",
            "ALTER TABLE workers ADD COLUMN last_error TEXT NOT NULL DEFAULT ''",
        ),
        (
            "trainers",
            "progress",
            "ALTER TABLE trainers ADD COLUMN progress TEXT NOT NULL DEFAULT ''",
        ),
        (
            "networks",
            "format",
            "ALTER TABLE networks ADD COLUMN format TEXT NOT NULL DEFAULT 'onnx'",
        ),
        (
            "episodes",
            "kind",
            "ALTER TABLE episodes ADD COLUMN kind INTEGER NOT NULL DEFAULT 0",
        ),
    ];
    for (table, name, ddl) in add_column {
        add_column_if_missing(conn, table, name, ddl)?;
    }

    conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_episodes_kind ON episodes(kind)")
        .map_err(|e| StoreError::Migration {
            version: 2,
            name: "add columns",
            message: format!("exec idx_episodes_kind: {e}"),
        })?;
    Ok(())
}

fn migrate_eval_queue_streaming(conn: &Connection) -> Result<(), StoreError> {
    if column_exists(conn, "eval_queue", "target_games")? {
        return Ok(());
    }

    let sharded = column_exists(conn, "eval_queue", "shard_idx").unwrap_or(false);
    let mut stmts: Vec<&str> = vec![
        "ALTER TABLE eval_queue RENAME TO eval_queue_legacy",
        "CREATE TABLE eval_queue (
			network_sha TEXT NOT NULL,
			opponent_spec TEXT NOT NULL,
			target_games INTEGER NOT NULL,
			per_task_games INTEGER NOT NULL DEFAULT 0,
			attempts INTEGER NOT NULL DEFAULT 0,
			wins INTEGER NOT NULL DEFAULT 0,
			draws INTEGER NOT NULL DEFAULT 0,
			losses INTEGER NOT NULL DEFAULT 0,
			num_games INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (network_sha, opponent_spec)
		)",
    ];
    if sharded {
        stmts.push(
            "INSERT INTO eval_queue
			(network_sha, opponent_spec, target_games, per_task_games, attempts, wins, draws, losses, num_games, created_at)
			SELECT network_sha, opponent_spec, MAX(shard_total)*MAX(shard_games), MAX(shard_games),
				SUM(attempts),
				COALESCE(SUM(CASE WHEN done=1 THEN wins END),0),
				COALESCE(SUM(CASE WHEN done=1 THEN draws END),0),
				COALESCE(SUM(CASE WHEN done=1 THEN losses END),0),
				COALESCE(SUM(CASE WHEN done=1 THEN num_games END),0),
				MIN(created_at)
			FROM eval_queue_legacy GROUP BY network_sha, opponent_spec",
        );
    } else {
        stmts.push(
            "INSERT INTO eval_queue
			(network_sha, opponent_spec, target_games, per_task_games, attempts, created_at)
			SELECT network_sha, opponent_spec, 0, 0, attempts, created_at FROM eval_queue_legacy",
        );
    }
    stmts.push("DROP TABLE eval_queue_legacy");
    for q in stmts {
        conn.execute_batch(q).map_err(|e| StoreError::Migration {
            version: 3,
            name: "eval_queue streaming",
            message: format!("migrate eval_queue to streamed schema: {e}"),
        })?;
    }
    tracing::info!(
        "[store] eval_queue 已迁移为流式累加结构（分片进度按 (network, spec) 折叠保留）"
    );
    Ok(())
}

fn migrate_eval_detail_breakdown(conn: &Connection) -> Result<(), StoreError> {
    for table in ["eval_results", "eval_queue"] {
        for col in [
            "red_wins",
            "black_wins",
            "draw_no_capture",
            "draw_truncation",
        ] {
            let ddl = format!("ALTER TABLE {table} ADD COLUMN {col} INTEGER NOT NULL DEFAULT 0");
            add_column_if_missing(conn, table, col, &ddl)?;
        }
    }
    Ok(())
}

fn migrate_episode_opponent(conn: &Connection) -> Result<(), StoreError> {
    for col in ["opponent_sha", "opponent_spec"] {
        let ddl = format!("ALTER TABLE episodes ADD COLUMN {col} TEXT NOT NULL DEFAULT ''");
        add_column_if_missing(conn, "episodes", col, &ddl)?;
    }
    if !column_exists(conn, "episodes", "match_id")? {
        conn.execute_batch("ALTER TABLE episodes ADD COLUMN match_id INTEGER NOT NULL DEFAULT 0")
            .map_err(|e| StoreError::Migration {
                version: 5,
                name: "episode opponent & match link",
                message: format!("add episodes.match_id: {e}"),
            })?;
        conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_episodes_match ON episodes(match_id)")
            .map_err(|e| StoreError::Migration {
                version: 5,
                name: "episode opponent & match link",
                message: format!("index episodes.match_id: {e}"),
            })?;
    }
    Ok(())
}

fn migrate_episode_breakdown(conn: &Connection) -> Result<(), StoreError> {
    for col in [
        "red_wins",
        "black_wins",
        "draw_no_capture",
        "draw_truncation",
    ] {
        let ddl = format!("ALTER TABLE episodes ADD COLUMN {col} INTEGER NOT NULL DEFAULT 0");
        add_column_if_missing(conn, "episodes", col, &ddl)?;
    }
    Ok(())
}

fn migrate_episode_games(conn: &Connection) -> Result<(), StoreError> {
    for (col, ddl) in [
        (
            "seed_base",
            "ALTER TABLE episodes ADD COLUMN seed_base INTEGER",
        ),
        (
            "pairs_parallel",
            "ALTER TABLE episodes ADD COLUMN pairs_parallel INTEGER",
        ),
        (
            "pair_seed_refresh",
            "ALTER TABLE episodes ADD COLUMN pair_seed_refresh INTEGER",
        ),
    ] {
        add_column_if_missing(conn, "episodes", col, ddl)?;
    }

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS episode_games (
			episode_id INTEGER NOT NULL,
			game_index INTEGER NOT NULL,
			pair_index INTEGER NOT NULL DEFAULT 0,
			pair_position INTEGER NOT NULL DEFAULT 0,
			game_seed INTEGER,
			a_is_red INTEGER NOT NULL DEFAULT 0,
			seed_refreshed INTEGER NOT NULL DEFAULT 0,
			winner INTEGER,
			steps INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (episode_id, game_index)
		)",
    )
    .map_err(|e| StoreError::Migration {
        version: 13,
        name: "episode game identity index",
        message: format!("create episode_games: {e}"),
    })?;
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_episode_games_seed ON episode_games(game_seed)",
    )
    .map_err(|e| StoreError::Migration {
        version: 13,
        name: "episode game identity index",
        message: format!("create idx_episode_games_seed: {e}"),
    })?;
    Ok(())
}

fn migrate_feature_packs(conn: &Connection) -> Result<(), StoreError> {
    let stmts: &[&str] = &[
        "CREATE TABLE IF NOT EXISTS feature_packs (
			episode_object_key TEXT NOT NULL,
			pack_version INTEGER NOT NULL,
			object_key TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (episode_object_key, pack_version)
		)",
        "CREATE INDEX IF NOT EXISTS idx_feature_packs_created ON feature_packs(created_at)",
    ];
    for q in stmts {
        conn.execute_batch(q).map_err(|e| StoreError::Migration {
            version: 7,
            name: "feature packs",
            message: format!("exec {q:?}: {e}"),
        })?;
    }
    Ok(())
}

fn migrate_matches_hexanomial(conn: &Connection) -> Result<(), StoreError> {
    add_column_if_missing(
        conn,
        "matches",
        "pair_wl",
        "ALTER TABLE matches ADD COLUMN pair_wl INTEGER NOT NULL DEFAULT 0",
    )
}

fn migrate_eval_opponent_sha(conn: &Connection) -> Result<(), StoreError> {
    if column_exists(conn, "eval_queue", "opponent_sha")? {
        return Ok(());
    }
    conn.execute_batch("ALTER TABLE eval_queue ADD COLUMN opponent_sha TEXT NOT NULL DEFAULT '';")
        .map_err(|e| StoreError::Migration {
            version: 10,
            name: "eval model opponent sha",
            message: format!("add column: {e}"),
        })
}

fn migrate_match_sides_nullable(conn: &Connection) -> Result<(), StoreError> {
    let notnull: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('matches')
			 WHERE name IN ('candidate','opponent') AND \"notnull\" = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|e| StoreError::Migration {
            version: 11,
            name: "match sides nullable sha",
            message: format!("inspect matches columns: {e}"),
        })?;
    if notnull == 0 {
        return Ok(());
    }

    conn.execute_batch(
        "CREATE TABLE matches_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			candidate TEXT REFERENCES networks(sha),
			opponent TEXT REFERENCES networks(sha),
			status TEXT NOT NULL DEFAULT 'running',
			pair_ll INTEGER NOT NULL DEFAULT 0,
			pair_ld INTEGER NOT NULL DEFAULT 0,
			pair_dd INTEGER NOT NULL DEFAULT 0,
			pair_wl INTEGER NOT NULL DEFAULT 0,
			pair_dw INTEGER NOT NULL DEFAULT 0,
			pair_ww INTEGER NOT NULL DEFAULT 0,
			num_games INTEGER NOT NULL DEFAULT 0,
			target_games INTEGER NOT NULL
		);
		INSERT INTO matches_new
			(id, candidate, opponent, status, pair_ll, pair_ld, pair_dd, pair_wl, pair_dw, pair_ww, num_games, target_games)
		SELECT id, candidate, opponent, status, pair_ll, pair_ld, pair_dd, pair_wl, pair_dw, pair_ww, num_games, target_games
		FROM matches;
		DROP TABLE matches;
		ALTER TABLE matches_new RENAME TO matches;",
    )
    .map_err(|e| StoreError::Migration {
        version: 11,
        name: "match sides nullable sha",
        message: format!("rebuild matches: {e}"),
    })
}

fn migrate_drop_eval_tables(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "DROP INDEX IF EXISTS idx_eval_results_spec;
		 DROP TABLE IF EXISTS eval_results;
		 DROP TABLE IF EXISTS eval_queue;
		 DELETE FROM settings WHERE key LIKE 'eval%';
		 DELETE FROM job_tasks WHERE job_id IN (SELECT job_id FROM jobs WHERE kind = 2);
		 DELETE FROM jobs WHERE kind = 2;",
    )
    .map_err(|e| StoreError::Migration {
        version: 12,
        name: "drop eval tables",
        message: format!("drop eval schema: {e}"),
    })?;
    tracing::info!("[store] eval 机制已移除：eval_results / eval_queue 表与 JOB_EVAL job 行已清理");
    Ok(())
}

fn migrate_jobs(conn: &Connection) -> Result<(), StoreError> {
    let stmts: &[&str] = &[
        "CREATE TABLE IF NOT EXISTS jobs (
			job_id TEXT PRIMARY KEY,
			kind INTEGER NOT NULL,
			priority INTEGER NOT NULL DEFAULT 0,
			state TEXT NOT NULL DEFAULT 'running',
			total_games INTEGER NOT NULL DEFAULT 0,
			completed_games INTEGER NOT NULL DEFAULT 0,
			dispatched_games INTEGER NOT NULL DEFAULT 0,
			spec_json TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			next_seq INTEGER NOT NULL DEFAULT 0
		)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state, priority DESC, created_at)",
        "CREATE TABLE IF NOT EXISTS job_tasks (
			task_id TEXT PRIMARY KEY,
			job_id TEXT NOT NULL,
			games INTEGER NOT NULL
		)",
        "CREATE INDEX IF NOT EXISTS idx_job_tasks_job ON job_tasks(job_id)",
    ];
    for q in stmts {
        conn.execute_batch(q).map_err(|e| StoreError::Migration {
            version: 9,
            name: "unified jobs",
            message: format!("exec {q:?}: {e}"),
        })?;
    }
    Ok(())
}

pub fn run(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at INTEGER NOT NULL
		)",
    )
    .map_err(|e| StoreError::Migration {
        version: -1,
        name: "schema_migrations",
        message: format!("create schema_migrations: {e}"),
    })?;

    let applied: Vec<i64> = {
        let mut stmt = conn
            .prepare("SELECT version FROM schema_migrations")
            .map_err(|e| StoreError::Migration {
                version: -1,
                name: "schema_migrations",
                message: format!("read schema_migrations: {e}"),
            })?;
        let rows = stmt
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(|e| StoreError::Migration {
                version: -1,
                name: "schema_migrations",
                message: format!("read schema_migrations: {e}"),
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| StoreError::Migration {
                version: -1,
                name: "schema_migrations",
                message: format!("scan schema_migrations: {e}"),
            })?
    };

    for m in MIGRATIONS {
        if applied.contains(&m.version) {
            continue;
        }

        conn.execute_batch("BEGIN")
            .map_err(|e| StoreError::Migration {
                version: m.version,
                name: m.name,
                message: format!("begin: {e}"),
            })?;
        if let Err(e) = (m.up)(conn).and_then(|_| {
            conn.execute_batch(&format!(
                "INSERT INTO schema_migrations (version, name, applied_at)
				 VALUES ({}, '{}', strftime('%s','now'))",
                m.version, m.name
            ))
            .map_err(|err| StoreError::Migration {
                version: m.version,
                name: m.name,
                message: format!("record migration: {err}"),
            })
        }) {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
        conn.execute_batch("COMMIT")
            .map_err(|e| StoreError::Migration {
                version: m.version,
                name: m.name,
                message: format!("commit: {e}"),
            })?;
        tracing::info!("[store] schema v{} 已应用: {}", m.version, m.name);
    }

    if let Err(e) = conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_episodes_task_id_unique ON episodes(task_id)",
    ) {
        tracing::warn!(
            "[store] ⚠️ 创建 episodes.task_id 唯一索引失败（{e}）；现有数据可能含重复 task_id，幂等保护降级为应用层查重"
        );
    }
    Ok(())
}
