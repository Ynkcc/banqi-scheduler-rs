//! 【所有权：infra 成员】episodes 表端口：episode 登记与 trainer 消费面。
//! SQL 逐条对照 Go 版 internal/store/episodes.go（与 trainers.go 的
//! EpisodeIDByKey）。

use bqs_core::ports::{
    EpisodeDataStats, EpisodeRateEstimate, EpisodeStore, GenerationStats, KindStats,
};
use bqs_core::types::{Episode, EpisodeDraft};
use bqs_core::CoreResult;

use std::collections::BTreeMap;

use crate::{Store, StoreError};

use rusqlite::OptionalExtension;

/// episodes.kind 取值口径与 pb.DataKind 对齐：0=resnet 1=nnue 2=none。
/// none = 不产训练数据的对局记录（eval 评估任务回放快照），不进消费管道。
pub(crate) const KIND_NONE: i64 = 2;

/// episodes 表的统一列序（对照 Go 版 episodeColumns）。
const EPISODE_COLUMNS: &str = "id, worker_id, task_id, network_sha, game_count, total_steps, winner, \
     object_key, created_at, kind, opponent_sha, opponent_spec, match_id, \
     red_wins, black_wins, draw_no_capture, draw_truncation";

fn scan_episode_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Episode> {
    Ok(Episode {
        id: row.get(0)?,
        worker_id: row.get(1)?,
        task_id: row.get(2)?,
        network_sha: row.get(3)?,
        game_count: row.get(4)?,
        total_steps: row.get(5)?,
        winner: row.get(6)?,
        object_key: row.get(7)?,
        created_at: row.get(8)?,
        kind: row.get(9)?,
        opponent_sha: row.get(10)?,
        opponent_spec: row.get(11)?,
        match_id: row.get(12)?,
        red_wins: row.get(13)?,
        black_wins: row.get(14)?,
        draw_no_capture: row.get(15)?,
        draw_truncation: row.get(16)?,
    })
}

impl EpisodeStore for Store {
    /// 幂等查重（ReportEpisode 重试路径）。
    /// 对照 Go 版 GetEpisodeByTask：多条并存取最新一条（ORDER BY id DESC LIMIT 1）。
    fn get_episode_by_task(&self, task_id: &str) -> CoreResult<Option<Episode>> {
        self.with_conn(|conn| {
            conn.query_row(
                &format!("SELECT {EPISODE_COLUMNS} FROM episodes WHERE task_id = ? ORDER BY id DESC LIMIT 1"),
                rusqlite::params![task_id],
                scan_episode_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// 登记新 episode，返回数据库分配的 id。
    /// created_at 由调用方随 draft 传入（取自注入 Clock；Go 版此处为 time.Now）。
    fn insert_episode(&self, draft: EpisodeDraft) -> CoreResult<i64> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO episodes (worker_id, task_id, network_sha, game_count, total_steps, \
					winner, object_key, created_at, kind, opponent_sha, opponent_spec, match_id, \
					red_wins, black_wins, draw_no_capture, draw_truncation)
				 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rusqlite::params![
                    draft.worker_id, draft.task_id, draft.network_sha, draft.game_count,
                    draft.total_steps, draft.winner, draft.object_key, draft.created_at,
                    draft.kind, draft.opponent_sha, draft.opponent_spec, draft.match_id,
                    draft.red_wins, draft.black_wins, draft.draw_no_capture, draft.draw_truncation
                ],
            )
            .map_err(StoreError::from)?;
            Ok(conn.last_insert_rowid())
        })
    }

    /// 游标分页列出已登记的 episode 对象键（按登记顺序递增）。
    ///
    /// 对照 Go 版 ListEpisodeKeys：after_key 为上次返回的最后一个键，服务端据此
    /// 反查出该行的 id 再按 id 推进——对象键含随机段，字典序与登记顺序无关，
    /// 用字典序做游标会永久漏掉后写入的对象。kind >= 0 时只返回该数据类别
    /// （None = 不过滤具体类别，但始终排除 none 类回放快照）；limit<=0 或 >1000
    /// 时取默认 200。游标子查询独立于 kind 过滤：即使 after_key 属于另一类别，
    /// 仍能正确定位推进点。
    fn list_episode_keys(
        &self,
        after_key: &str,
        limit: i64,
        kind: Option<i64>,
    ) -> CoreResult<Vec<String>> {
        let limit = if limit <= 0 || limit > 1000 { 200 } else { limit };
        self.with_conn(|conn| {
            let mut query = format!(
                "SELECT object_key FROM episodes
				 WHERE id > COALESCE((SELECT id FROM episodes WHERE object_key = ?), 0) AND kind != ?"
            );
            if kind.is_some() {
                query.push_str(" AND kind = ?");
            }
            query.push_str(" ORDER BY id ASC LIMIT ?");
            let mut stmt = conn.prepare(&query).map_err(StoreError::from)?;
            match kind {
                // 游标子查询独立于本过滤条件：即使 after_key 属于另一类别，仍能正确定位推进点
                Some(k) => {
                    let rows = stmt
                        .query_map(
                            rusqlite::params![after_key, KIND_NONE, k, limit],
                            |row| row.get::<_, String>(0),
                        )
                        .map_err(StoreError::from)?;
                    rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
                }
                None => {
                    let rows = stmt
                        .query_map(
                            rusqlite::params![after_key, KIND_NONE, limit],
                            |row| row.get::<_, String>(0),
                        )
                        .map_err(StoreError::from)?;
                    rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
                }
            }
        })
    }

    /// 消费水位映射（TrainerHeartbeat.consumed_key → episodes.id）；键不存在返回 None。
    fn episode_id_by_key(&self, key: &str) -> CoreResult<Option<i64>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT id FROM episodes WHERE object_key=?",
                rusqlite::params![key],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// 元数据按 id 倒序分页（before_id<=0 表示取最新一页；WebUI /api/episodes）。
    /// 对照 Go 版 ListEpisodes：limit<=0 或 >500 时取默认 100。
    fn list_episodes_page(&self, before_id: i64, limit: i64) -> CoreResult<Vec<Episode>> {
        let limit = if limit <= 0 || limit > 500 { 100 } else { limit };
        self.with_conn(|conn| {
            let mut query = format!("SELECT {EPISODE_COLUMNS} FROM episodes");
            if before_id > 0 {
                query.push_str(" WHERE id < ?");
            }
            query.push_str(" ORDER BY id DESC LIMIT ?");
            let mut stmt = conn.prepare(&query).map_err(StoreError::from)?;
            let rows = if before_id > 0 {
                stmt.query_map(rusqlite::params![before_id, limit], scan_episode_row)
                    .map_err(StoreError::from)?
            } else {
                stmt.query_map(rusqlite::params![limit], scan_episode_row)
                    .map_err(StoreError::from)?
            };
            rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
        })
    }

    // ---- [api 需求] 追加（SQL 对照 Go internal/store/episodes.go 的对应函数）----

    /// 按 id 查单条 episode 元数据（[replay 需求] Go: store.GetEpisode）。
    fn get_episode(&self, id: i64) -> CoreResult<Option<Episode>> {
        self.with_conn(|conn| {
            conn.query_row(
                &format!("SELECT {EPISODE_COLUMNS} FROM episodes WHERE id = ?"),
                rusqlite::params![id],
                scan_episode_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }


    /// 按窗口聚合各 worker 登记的 episode 产出（[api 需求] Go: store.EpisodeStatsByWorker；
    /// 批数/局数/步数，window<=0 表示不限窗口。混合任务形态下 selfplay 与 rating
    /// 侧产数据同表登记，此处不做来源区分）。
    fn episode_stats_by_worker(&self, window_secs: i64) -> CoreResult<BTreeMap<String, KindStats>> {
        let mut out = BTreeMap::new();
        self.with_conn(|conn| {
            let mut query = String::from(
                "SELECT worker_id, COUNT(*), COALESCE(SUM(game_count),0), COALESCE(SUM(total_steps),0) FROM episodes",
            );
            if window_secs > 0 {
                query.push_str(" WHERE created_at > ?");
            }
            query.push_str(" GROUP BY worker_id");
            let mut stmt = conn.prepare(&query).map_err(StoreError::from)?;
            let scan = |row: &rusqlite::Row<'_>| {
                Ok((
                    row.get::<_, String>(0)?,
                    KindStats {
                        episodes: row.get(1)?,
                        games: row.get(2)?,
                        steps: row.get(3)?,
                    },
                ))
            };
            let rows = if window_secs > 0 {
                let cutoff = self.now() - window_secs;
                stmt.query_map(rusqlite::params![cutoff], scan)
                    .map_err(StoreError::from)?
            } else {
                stmt.query_map([], scan).map_err(StoreError::from)?
            };
            for row in rows {
                let (id, st) = row.map_err(StoreError::from)?;
                out.insert(id, st);
            }
            Ok(())
        })?;
        Ok(out)
    }

    /// 估算各 worker 的产出速率样本（[api 需求] Go: store.EpisodeRateEstimates）：
    /// 窗口内每 worker 取最近 samples_per_worker 批 episode（按批取样能平滑批次
    /// 间歇与任务切换带来的速率抖动），返回合计局数与首末批时间跨度；
    /// 调用方以 games/span 得到稳态局/h 估算。samples<=0 取默认 24。
    fn episode_rate_estimates(
        &self,
        window_secs: i64,
        samples_per_worker: i64,
    ) -> CoreResult<BTreeMap<String, EpisodeRateEstimate>> {
        let samples = if samples_per_worker <= 0 { 24 } else { samples_per_worker };
        let mut out = BTreeMap::new();
        self.with_conn(|conn| {
            let cutoff = self.now() - window_secs;
            let mut stmt = conn
                .prepare(
                    "SELECT worker_id, COUNT(*), COALESCE(SUM(game_count),0), MAX(created_at) - MIN(created_at)
					 FROM (
						SELECT worker_id, game_count, created_at,
						       ROW_NUMBER() OVER (PARTITION BY worker_id ORDER BY id DESC) AS rn
						FROM episodes WHERE created_at > ?
					 ) WHERE rn <= ?
					 GROUP BY worker_id",
                )
                .map_err(StoreError::from)?;
            let rows = stmt
                .query_map(rusqlite::params![cutoff, samples], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        EpisodeRateEstimate {
                            episodes: row.get(1)?,
                            games: row.get(2)?,
                            span_seconds: row.get(3)?,
                        },
                    ))
                })
                .map_err(StoreError::from)?;
            for row in rows {
                let (id, est) = row.map_err(StoreError::from)?;
                out.insert(id, est);
            }
            Ok(())
        })?;
        Ok(out)
    }

    /// 数据流入总览（[api 需求] Go: store.EpisodeDataStats 的三窗口部分）：
    /// 累计 / 近 1h / 近 24h 按数据类别分组的 COUNT/SUM（键 = kind 原值，
    /// 名称映射由 api 层做）。rate 观测以「新数据流入速度」为唯一目标，
    /// 不区分 selfplay / rating 来源。
    fn episode_data_stats(&self) -> CoreResult<EpisodeDataStats> {
        let mut total = BTreeMap::new();
        let mut last_1h = BTreeMap::new();
        let mut last_24h = BTreeMap::new();
        self.with_conn(|conn| {
            // fill(window, into)：window<=0 表示不限窗口（累计口径）。对照 Go fill 闭包。
            let fill = |window: i64,
                            into: &mut BTreeMap<i64, KindStats>|
             -> Result<(), StoreError> {
                let mut query = String::from(
                    "SELECT kind, COUNT(*), COALESCE(SUM(game_count),0), COALESCE(SUM(total_steps),0) FROM episodes",
                );
                if window > 0 {
                    query.push_str(" WHERE created_at > ?");
                }
                query.push_str(" GROUP BY kind");
                let mut stmt = conn.prepare(&query)?;
                let scan = |row: &rusqlite::Row<'_>| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        KindStats {
                            episodes: row.get(1)?,
                            games: row.get(2)?,
                            steps: row.get(3)?,
                        },
                    ))
                };
                let rows = if window > 0 {
                    let cutoff = self.now() - window;
                    stmt.query_map(rusqlite::params![cutoff], scan)?
                } else {
                    stmt.query_map([], scan)?
                };
                for row in rows {
                    let (kind, st) = row?;
                    into.insert(kind, st);
                }
                Ok(())
            };
            fill(0, &mut total)?;
            fill(3600, &mut last_1h)?;
            fill(86400, &mut last_24h)?;
            Ok(())
        })?;
        Ok(EpisodeDataStats { total, last_1h, last_24h })
    }

    /// 本代数据量（[api 需求] Go: store.GenerationDataStats）：以 networks 最新登记
    /// 时间为界汇总其后的 episode，排除 none 类对局记录（trainer fresh-only 模式
    /// 导出即清池，此窗口 = 下一候选的训练集）。尚无任何网络时本代从零起算
    /// （全零 + 空 candidate），边界未定。
    fn generation_data_stats(&self) -> CoreResult<GenerationStats> {
        self.with_conn(|conn| {
            let boundary: Option<(String, i64)> = conn
                .query_row(
                    "SELECT sha, created_at FROM networks ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()
                .map_err(StoreError::from)?;
            let Some((candidate, since)) = boundary else {
                return Ok(GenerationStats::default()); // 尚无任何网络：本代从零起算
            };
            let (episodes, games, steps): (i64, i64, i64) = conn
                .query_row(
                    "SELECT COUNT(*), COALESCE(SUM(game_count),0), COALESCE(SUM(total_steps),0)
					 FROM episodes WHERE created_at > ? AND kind != ?",
                    rusqlite::params![since, KIND_NONE],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(StoreError::from)?;
            Ok(GenerationStats { since, candidate, episodes, games, steps })
        })
    }
}

#[cfg(test)]
mod tests {
    use bqs_core::ports::EpisodeStore as ES;
    use bqs_core::types::EpisodeDraft;

    fn draft(task: &str, key: &str, kind: i64, created_at: i64) -> EpisodeDraft {
        EpisodeDraft {
            worker_id: "w1".into(),
            task_id: task.into(),
            network_sha: "n1".into(),
            game_count: 4,
            total_steps: 100,
            winner: 1,
            object_key: key.into(),
            created_at,
            kind,
            opponent_sha: String::new(),
            opponent_spec: "rule:x".into(),
            match_id: 0,
            red_wins: 1,
            black_wins: 2,
            draw_no_capture: 1,
            draw_truncation: 0,
        }
    }

    #[test]
    fn insert_get_by_task_and_page() {
        let (_dir, s) = crate::testutil::open_temp_store();

        let id1 = ES::insert_episode(&*s, draft("t1", "episodes/n1/a.epb.gz", 0, 100)).unwrap();
        let id2 = ES::insert_episode(&*s, draft("t2", "episodes/n1/b.epb.gz", 1, 200)).unwrap();
        assert_ne!(id1, id2);

        // 按 task 查重（幂等闸口）
        let e = ES::get_episode_by_task(&*s, "t1").unwrap().unwrap();
        assert_eq!(e.object_key, "episodes/n1/a.epb.gz");
        assert_eq!(e.created_at, 100);
        assert!(ES::get_episode_by_task(&*s, "nope").unwrap().is_none());

        // 按 key 反查 id（消费水位映射）
        assert_eq!(ES::episode_id_by_key(&*s, "episodes/n1/b.epb.gz").unwrap(), Some(id2));
        assert_eq!(ES::episode_id_by_key(&*s, "nope").unwrap(), None);

        // 按 id 倒序分页
        let page = ES::list_episodes_page(&*s, 0, 1).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, id2);
        let page = ES::list_episodes_page(&*s, id2, 10).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, id1);
        // 四分明细落库
        let e = ES::list_episodes_page(&*s, 0, 10).unwrap();
        let got = e.iter().find(|x| x.id == id1).unwrap();
        assert_eq!((got.red_wins, got.black_wins, got.draw_no_capture), (1, 2, 1));
    }

    /// 游标分页：after_key 反查 id 后按 id 递增；none 类恒排除；kind 过滤。
    #[test]
    fn list_keys_cursor_and_kind_filter() {
        let (_dir, s) = crate::testutil::open_temp_store();

        ES::insert_episode(&*s, draft("t1", "k/a", 0, 1)).unwrap();
        ES::insert_episode(&*s, draft("t2", "k/b", 1, 2)).unwrap();
        ES::insert_episode(&*s, draft("t3", "k/c", 2, 3)).unwrap(); // none 类
        ES::insert_episode(&*s, draft("t4", "k/d", 0, 4)).unwrap();

        // 全量（排除 none）：按 id 递增
        let keys = ES::list_episode_keys(&*s, "", 0, None).unwrap();
        assert_eq!(keys, vec!["k/a", "k/b", "k/d"]);

        // 从 b 之后推进（游标 = after_key 反查 id）
        let keys = ES::list_episode_keys(&*s, "k/b", 0, None).unwrap();
        assert_eq!(keys, vec!["k/d"]);

        // kind 过滤（只返回该类别）
        let keys = ES::list_episode_keys(&*s, "", 0, Some(1)).unwrap();
        assert_eq!(keys, vec!["k/b"]);
        // 游标子查询独立于过滤条件：即使 after_key 属于另一类别仍正确定位
        let keys = ES::list_episode_keys(&*s, "k/a", 0, Some(1)).unwrap();
        assert_eq!(keys, vec!["k/b"]);

        // limit 生效
        let keys = ES::list_episode_keys(&*s, "", 1, None).unwrap();
        assert_eq!(keys, vec!["k/a"]);

        // 未知游标 → 从头开始
        let keys = ES::list_episode_keys(&*s, "unknown-key", 0, None).unwrap();
        assert_eq!(keys, vec!["k/a", "k/b", "k/d"]);
    }

    /// [api 需求] episode_stats_by_worker / episode_rate_estimates
    /// （Go: EpisodeStatsByWorker / EpisodeRateEstimates）。
    #[test]
    fn stats_by_worker_and_rate_estimates() {
        // 固定时钟 now=2000（窗口查询的确定性）
        let (_dir, s) = crate::testutil::open_temp_store_at(2000);
        use bqs_core::types::EpisodeDraft;

        // created_at 显式受控：w1 三批（新→旧 1900/1500/1100），w2 一批 1800
        let mk = |task: &str, worker: &str, games: i64, at: i64| EpisodeDraft {
            task_id: task.into(),
            worker_id: worker.into(),
            game_count: games,
            total_steps: games * 10,
            created_at: at,
            ..Default::default()
        };
        ES::insert_episode(&*s, mk("a", "w1", 10, 1100)).unwrap();
        ES::insert_episode(&*s, mk("b", "w1", 20, 1500)).unwrap();
        ES::insert_episode(&*s, mk("c", "w1", 40, 1900)).unwrap();
        ES::insert_episode(&*s, mk("d", "w2", 8, 1800)).unwrap();

        // 累计口径（window<=0 不限）
        let st = ES::episode_stats_by_worker(&*s, 0).unwrap();
        assert_eq!(st["w1"], bqs_core::ports::KindStats { episodes: 3, games: 70, steps: 700 });
        assert_eq!(st["w2"].games, 8);

        // 窗口（created_at > 2000-600=1400 → 1500/1900 两批 + w2 的 1800）
        let st = ES::episode_stats_by_worker(&*s, 600).unwrap();
        assert_eq!(st["w1"], bqs_core::ports::KindStats { episodes: 2, games: 60, steps: 600 });
        assert_eq!(st["w2"].episodes, 1);

        // 速率样本（窗口 1000 → cutoff 1000，全部 4 批在内）：
        // 每 worker 取最近 2 批（按 id 倒序）→ w1 取 1900/1500（局数 60，跨度 400s）
        let est = ES::episode_rate_estimates(&*s, 1000, 2).unwrap();
        assert_eq!(
            est["w1"],
            bqs_core::ports::EpisodeRateEstimate { episodes: 2, games: 60, span_seconds: 400 }
        );
        assert_eq!(est["w2"].games, 8);
        // samples<=0 → 默认 24（w1 全部 3 批，跨度 800s）
        let est = ES::episode_rate_estimates(&*s, 1000, 0).unwrap();
        assert_eq!(est["w1"].episodes, 3);
        assert_eq!(est["w1"].span_seconds, 800);
    }

    /// [api 需求] episode_data_stats / generation_data_stats
    /// （Go: EpisodeDataStats / GenerationDataStats）。
    #[test]
    fn data_stats_and_generation_window() {
        // 固定时钟 now=2000；代际边界手工回拨到 1000
        let (_dir, s) = crate::testutil::open_temp_store_at(2000);
        use bqs_core::ports::NetworkStore as NS;
        use bqs_core::types::EpisodeDraft;

        // 代际边界：登记网络（clock=2000 登记）后回拨 created_at=1000
        NS::register_network(&*s, "cand", None, None, "onnx").unwrap();
        {
            let conn = s.conn.lock().unwrap();
            conn.execute("UPDATE networks SET created_at=1000 WHERE sha='cand'", []).unwrap();
        }
        let mk = |task: &str, games: i64, kind: i64, at: i64| EpisodeDraft {
            task_id: task.into(),
            game_count: games,
            total_steps: games * 10,
            kind,
            created_at: at,
            ..Default::default()
        };
        // 代际内：kind0 一批 + kind1 一批；none 类不进训练窗口；代际前一批不计
        ES::insert_episode(&*s, mk("g1", 10, 0, 1100)).unwrap();
        ES::insert_episode(&*s, mk("g2", 20, 1, 1200)).unwrap();
        ES::insert_episode(&*s, mk("g3", 40, 2, 1300)).unwrap(); // none：不进训练窗口
        ES::insert_episode(&*s, mk("g4", 80, 0, 900)).unwrap(); // 代际前

        // 数据流入总览（累计口径，全 kind 分组）
        let stats = ES::episode_data_stats(&*s).unwrap();
        assert_eq!(stats.total[&0].episodes, 2);
        assert_eq!(stats.total[&0].games, 90);
        assert_eq!(stats.total[&1].games, 20);
        assert_eq!(stats.total[&2].games, 40);

        // 本代窗口：created_at > 1000 且 kind != 2 → 10 + 20 = 30（步数 100+200）
        let generation = ES::generation_data_stats(&*s).unwrap();
        assert_eq!(generation.candidate, "cand");
        assert_eq!(generation.since, 1000);
        assert_eq!(generation.episodes, 2);
        assert_eq!(generation.games, 30);
        assert_eq!(generation.steps, 300);
    }

    /// [api 需求] generation_data_stats 无网络时全零（Go: 边界未定从零起算）。
    #[test]
    fn generation_stats_empty_db() {
        let (_dir, s) = crate::testutil::open_temp_store();
        let generation = ES::generation_data_stats(&*s).unwrap();
        assert_eq!(generation, bqs_core::ports::GenerationStats::default());
    }
}
