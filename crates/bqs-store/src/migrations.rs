//! 【所有权：infra 成员】版本化迁移链 v1..v8（语义与 Go 版 migrate.go 一致）。
//!
//! - `schema_migrations(version, name, applied_at)` 记账；
//! - 启动时按版本号单事务补跑未应用项，失败拒绝启动；
//! - v1 基础建表 / v2 补列 / v3 eval_queue 流式重建 / v4 评估明细四分 /
//!   v5 episode 对手与 match 关联 / v6 episode 逐局四分 / v7 feature_packs /
//!   v8 matches 六桶化。
//!
//! 每个迁移项必须幂等友好（IF NOT EXISTS / pragma_table_info 列探测），
//! 使「历史旧库从 v0 补跑全链」与「新库从 v1 起步」收敛到同一终态。
//! 建表 SQL 逐字段对照 Go 版 `migrateBaseTables` / `migrateEvalQueueStreaming`
//! （internal/store/migrate.go），保证可直接打开 Go 版创建的库文件。

use rusqlite::Connection;

use crate::StoreError;

/// 单个 schema 版本：up 在给定事务内执行该版本的语句序列（可含探测逻辑）。
struct Migration {
    version: i64,
    name: &'static str,
    up: fn(&Connection) -> Result<(), StoreError>,
}

/// 按版本号升序。追加新版本时必须使用尚未占用的下一个版本号。
static MIGRATIONS: &[Migration] = &[
    // v1 基础建表：全部表与基础索引（IF NOT EXISTS，历史库重复执行无害）。
    Migration { version: 1, name: "base tables", up: migrate_base_tables },
    // v2 补列：历史库缺列时 ALTER TABLE 补上（列探测防重复），
    // 并创建依赖补列的索引（老库上先建索引会因 no such column 打不开）。
    Migration { version: 2, name: "add columns", up: migrate_add_columns },
    // v3 eval_queue 流式化：历史两种旧结构（分片 / 整包）重建为流式累加结构。
    Migration { version: 3, name: "eval_queue streaming", up: migrate_eval_queue_streaming },
    // v4 评估明细四分：红/黑方获胜 + 平局按成因（未吃子 / 步数截断）拆分列。
    Migration { version: 4, name: "eval detail breakdown", up: migrate_eval_detail_breakdown },
    // v5 episode 对局双方与对打关联：opponent_sha / opponent_spec / match_id。
    Migration { version: 5, name: "episode opponent & match link", up: migrate_episode_opponent },
    // v6 episode 逐局四分：红/黑方获胜 + 平局按成因（未吃子 / 步数截断）。
    Migration { version: 6, name: "episode outcome breakdown", up: migrate_episode_breakdown },
    // v7 特征包登记表：记录批次 × 特征格式版本（v2 数据契约）。
    Migration { version: 7, name: "feature packs", up: migrate_feature_packs },
    // v8 matches 表六桶化：新增 pair_wl（一胜一负混合对）。旧数据 WL 不可恢复，置 0。
    Migration { version: 8, name: "matches hexanomial", up: migrate_matches_hexanomial },
];

/// 列探测：查询表是否已含指定列（table-valued PRAGMA，SQLite 3.16+）。
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

/// 列不存在则补列（v2/v4/v5/v6 共用的探测式补列）。
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

// ---------------------------------------------------------------------------
// v1：基础建表（逐字段对照 Go 版 migrateBaseTables）
// ---------------------------------------------------------------------------

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
			candidate TEXT NOT NULL REFERENCES networks(sha),
			opponent TEXT NOT NULL REFERENCES networks(sha),
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
        // trainers：trainer（消费端）心跳快照（生成-消费平衡的感知数据源；
        // last_seen 跨重启保留以快速恢复判定）。
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
        // eval_queue：流式生产模型——一个 (network, spec) 一行，target_games 为
        // 目标局数；上报增量累加进本行，累计 ≥ target 即聚合出一条 eval_results 并删行。
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

// ---------------------------------------------------------------------------
// v2：历史库补列（列探测防重复）+ 依赖补列的索引
// ---------------------------------------------------------------------------

fn migrate_add_columns(conn: &Connection) -> Result<(), StoreError> {
    // (表, 列, 补列 DDL)
    let add_column: &[(&str, &str, &str)] = &[
        ("workers", "client_version", "ALTER TABLE workers ADD COLUMN client_version TEXT NOT NULL DEFAULT ''"),
        ("workers", "memory_mb", "ALTER TABLE workers ADD COLUMN memory_mb INTEGER NOT NULL DEFAULT 0"),
        ("workers", "note", "ALTER TABLE workers ADD COLUMN note TEXT NOT NULL DEFAULT ''"),
        ("workers", "health_status", "ALTER TABLE workers ADD COLUMN health_status TEXT NOT NULL DEFAULT ''"),
        ("workers", "last_error", "ALTER TABLE workers ADD COLUMN last_error TEXT NOT NULL DEFAULT ''"),
        ("trainers", "progress", "ALTER TABLE trainers ADD COLUMN progress TEXT NOT NULL DEFAULT ''"),
        ("networks", "format", "ALTER TABLE networks ADD COLUMN format TEXT NOT NULL DEFAULT 'onnx'"),
        ("episodes", "kind", "ALTER TABLE episodes ADD COLUMN kind INTEGER NOT NULL DEFAULT 0"),
    ];
    for (table, name, ddl) in add_column {
        add_column_if_missing(conn, table, name, ddl)?;
    }
    // 依赖补列的索引必须在补列之后创建。
    conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_episodes_kind ON episodes(kind)")
        .map_err(|e| StoreError::Migration {
            version: 2,
            name: "add columns",
            message: format!("exec idx_episodes_kind: {e}"),
        })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// v3：eval_queue 流式化升级（历史两种旧结构重建）
// ---------------------------------------------------------------------------

fn migrate_eval_queue_streaming(conn: &Connection) -> Result<(), StoreError> {
    if column_exists(conn, "eval_queue", "target_games")? {
        return Ok(());
    }
    // 分片结构（有 shard_idx 列）与整包结构（无 shard_idx）。
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
        // 分片行按 (network, spec) 折叠：done 分片计数求和为已累加进度，
        // target = 片数 × 每片局数。
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
        // 整包行 target=0：调度层首扫即按 0 局收尾清除，不留垃圾。
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
    tracing::info!("[store] eval_queue 已迁移为流式累加结构（分片进度按 (network, spec) 折叠保留）");
    Ok(())
}

// ---------------------------------------------------------------------------
// v4：eval_results / eval_queue 补评估明细四分列
// ---------------------------------------------------------------------------

fn migrate_eval_detail_breakdown(conn: &Connection) -> Result<(), StoreError> {
    for table in ["eval_results", "eval_queue"] {
        for col in ["red_wins", "black_wins", "draw_no_capture", "draw_truncation"] {
            let ddl = format!("ALTER TABLE {table} ADD COLUMN {col} INTEGER NOT NULL DEFAULT 0");
            add_column_if_missing(conn, table, col, &ddl)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// v5：episodes 补对手与 match 关联列
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// v6：episodes 补逐局四分列
// ---------------------------------------------------------------------------

fn migrate_episode_breakdown(conn: &Connection) -> Result<(), StoreError> {
    for col in ["red_wins", "black_wins", "draw_no_capture", "draw_truncation"] {
        let ddl = format!("ALTER TABLE episodes ADD COLUMN {col} INTEGER NOT NULL DEFAULT 0");
        add_column_if_missing(conn, "episodes", col, &ddl)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// v7：特征包登记表（记录批次 × 特征格式版本 唯一）
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// v8：matches 表补 pair_wl 列（六桶化）
// ---------------------------------------------------------------------------

fn migrate_matches_hexanomial(conn: &Connection) -> Result<(), StoreError> {
    add_column_if_missing(
        conn,
        "matches",
        "pair_wl",
        "ALTER TABLE matches ADD COLUMN pair_wl INTEGER NOT NULL DEFAULT 0",
    )
}

// ---------------------------------------------------------------------------
// 迁移链主流程
// ---------------------------------------------------------------------------

/// 执行版本化迁移链：单事务逐版本补跑未应用的迁移，失败即拒绝启动。
pub fn run(conn: &Connection) -> Result<(), StoreError> {
    // 记账表本体先建（不进迁移链：链本身依赖它判断版本）。
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
        // 每版本单事务：失败回滚整个版本并拒绝启动。
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
        conn.execute_batch("COMMIT").map_err(|e| StoreError::Migration {
            version: m.version,
            name: m.name,
            message: format!("commit: {e}"),
        })?;
        tracing::info!("[store] schema v{} 已应用: {}", m.version, m.name);
    }

    // 幂等保护：episodes.task_id 唯一约束，防止 ReportEpisode 重试产生重复行。
    // 现有数据已有重复 task_id 时建索引会失败——这是数据污染的信号，降级为
    // 应用层查重（GetEpisodeByTask 仍工作），不阻断启动。故此项不入迁移链。
    if let Err(e) =
        conn.execute_batch("CREATE UNIQUE INDEX IF NOT EXISTS idx_episodes_task_id_unique ON episodes(task_id)")
    {
        tracing::warn!(
            "[store] ⚠️ 创建 episodes.task_id 唯一索引失败（{e}）；现有数据可能含重复 task_id，幂等保护降级为应用层查重"
        );
    }
    Ok(())
}
