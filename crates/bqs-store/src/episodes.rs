use bqs_core::ports::{
    EpisodeDataStats, EpisodeRateEstimate, EpisodeStore, GenerationStats, KindStats,
};
use bqs_core::types::{Episode, EpisodeDraft, EpisodeGameRow};
use bqs_core::CoreResult;

use std::collections::BTreeMap;

use crate::{Store, StoreError};

use rusqlite::OptionalExtension;

pub(crate) const KIND_NONE: i64 = 2;

pub const EPISODE_PAGE_DEFAULT_LIMIT: i64 = 100;
pub const EPISODE_PAGE_MAX_LIMIT: i64 = 500;

pub fn clamp_episode_page_limit(limit: i64) -> i64 {
    if limit <= 0 || limit > EPISODE_PAGE_MAX_LIMIT {
        EPISODE_PAGE_DEFAULT_LIMIT
    } else {
        limit
    }
}

const EPISODE_COLUMNS: &str =
    "id, worker_id, task_id, network_sha, game_count, total_steps, winner, \
     object_key, created_at, kind, opponent_sha, opponent_spec, match_id, \
     red_wins, black_wins, draw_no_capture, draw_truncation, \
     seed_base, pairs_parallel, pair_seed_refresh, \
     label_object_key, label_sha256, label_size";

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
        seed_base: row.get(17)?,
        pairs_parallel: row.get::<_, Option<i64>>(18)?.map(|v| v != 0),
        pair_seed_refresh: row.get::<_, Option<i64>>(19)?.map(|v| v != 0),
        label_object_key: row.get(20)?,
        label_sha256: row.get(21)?,
        label_size: row.get(22)?,
    })
}

const GAME_COLUMNS: &str = "game_index, pair_index, pair_position, game_seed, \
     a_is_red, seed_refreshed, winner, steps";

fn scan_game_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EpisodeGameRow> {
    Ok(EpisodeGameRow {
        game_index: row.get(0)?,
        pair_index: row.get(1)?,
        pair_position: row.get(2)?,
        game_seed: row.get(3)?,
        a_is_red: row.get::<_, i64>(4)? != 0,
        seed_refreshed: row.get::<_, i64>(5)? != 0,
        winner: row.get(6)?,
        steps: row.get(7)?,
    })
}

impl EpisodeStore for Store {
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

    fn insert_episode(&self, draft: EpisodeDraft) -> CoreResult<i64> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO episodes (worker_id, task_id, network_sha, game_count, total_steps, \
					winner, object_key, created_at, kind, opponent_sha, opponent_spec, match_id, \
					red_wins, black_wins, draw_no_capture, draw_truncation, \
					seed_base, pairs_parallel, pair_seed_refresh, \
					label_object_key, label_sha256, label_size)
				 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rusqlite::params![
                    draft.worker_id,
                    draft.task_id,
                    draft.network_sha,
                    draft.game_count,
                    draft.total_steps,
                    draft.winner,
                    draft.object_key,
                    draft.created_at,
                    draft.kind,
                    draft.opponent_sha,
                    draft.opponent_spec,
                    draft.match_id,
                    draft.red_wins,
                    draft.black_wins,
                    draft.draw_no_capture,
                    draft.draw_truncation,
                    draft.seed_base,
                    draft.pairs_parallel.map(|v| v as i64),
                    draft.pair_seed_refresh.map(|v| v as i64),
                    draft.label_object_key,
                    draft.label_sha256,
                    draft.label_size,
                ],
            )
            .map_err(StoreError::from)?;
            let episode_id = conn.last_insert_rowid();

            for g in &draft.games {
                conn.execute(
                    "INSERT OR REPLACE INTO episode_games
					 (episode_id, game_index, pair_index, pair_position, game_seed,
					  a_is_red, seed_refreshed, winner, steps)
					 VALUES (?,?,?,?,?,?,?,?,?)",
                    rusqlite::params![
                        episode_id,
                        g.game_index,
                        g.pair_index,
                        g.pair_position,
                        g.game_seed,
                        g.a_is_red as i64,
                        g.seed_refreshed as i64,
                        g.winner,
                        g.steps,
                    ],
                )
                .map_err(StoreError::from)?;
            }
            Ok(episode_id)
        })
    }

    fn list_episode_rows(
        &self,
        after_key: &str,
        limit: i64,
        kind: Option<i64>,
    ) -> CoreResult<Vec<Episode>> {
        let limit = if limit <= 0 || limit > 1000 {
            200
        } else {
            limit
        };
        self.with_conn(|conn| {
            let mut query = format!(
                "SELECT {EPISODE_COLUMNS} FROM episodes
				 WHERE id > COALESCE((SELECT id FROM episodes WHERE object_key = ?), 0) AND kind != ?"
            );

            let mut args: Vec<Box<dyn rusqlite::ToSql>> =
                vec![Box::new(after_key.to_string()), Box::new(KIND_NONE)];
            if let Some(k) = kind {
                query.push_str(" AND kind = ?");
                args.push(Box::new(k));
            }
            query.push_str(" ORDER BY id ASC LIMIT ?");
            args.push(Box::new(limit));
            let mut stmt = conn.prepare(&query).map_err(StoreError::from)?;
            let rows = stmt
                .query_map(
                    rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())),
                    scan_episode_row,
                )
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
    }

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

    fn list_episodes_page(&self, before_id: i64, limit: i64) -> CoreResult<Vec<Episode>> {
        let limit = clamp_episode_page_limit(limit);
        self.with_conn(|conn| {
            let mut query = format!("SELECT {EPISODE_COLUMNS} FROM episodes");
            let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            if before_id > 0 {
                query.push_str(" WHERE id < ?");
                args.push(Box::new(before_id));
            }
            query.push_str(" ORDER BY id DESC LIMIT ?");
            args.push(Box::new(limit));
            let mut stmt = conn.prepare(&query).map_err(StoreError::from)?;
            let rows = stmt
                .query_map(
                    rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())),
                    scan_episode_row,
                )
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
    }

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

    fn list_episodes_by_match(
        &self,
        match_id: i64,
        before_id: i64,
        limit: i64,
    ) -> CoreResult<Vec<Episode>> {
        let limit = clamp_episode_page_limit(limit);
        self.with_conn(|conn| {
            let mut query = format!("SELECT {EPISODE_COLUMNS} FROM episodes WHERE match_id = ?");
            let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(match_id)];
            if before_id > 0 {
                query.push_str(" AND id < ?");
                args.push(Box::new(before_id));
            }
            query.push_str(" ORDER BY id DESC LIMIT ?");
            args.push(Box::new(limit));
            let mut stmt = conn.prepare(&query).map_err(StoreError::from)?;
            let rows = stmt
                .query_map(
                    rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())),
                    scan_episode_row,
                )
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
    }

    fn get_episode_games(&self, episode_id: i64) -> CoreResult<Vec<EpisodeGameRow>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {GAME_COLUMNS} FROM episode_games
					 WHERE episode_id = ? ORDER BY game_index ASC"
                ))
                .map_err(StoreError::from)?;
            let rows = stmt
                .query_map(rusqlite::params![episode_id], scan_game_row)
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
    }

    fn find_games_by_seed(&self, seed: u64) -> CoreResult<Vec<(Episode, EpisodeGameRow)>> {
        let seed_i = seed as i64;
        self.with_conn(|conn| {
            let ep_cols = EPISODE_COLUMNS
                .split(", ")
                .map(|c| format!("e.{c}"))
                .collect::<Vec<_>>()
                .join(", ");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {ep_cols}, eg.game_index, eg.pair_index, eg.pair_position,
					        eg.game_seed, eg.a_is_red, eg.seed_refreshed, eg.winner, eg.steps
					 FROM episode_games eg JOIN episodes e ON e.id = eg.episode_id
					 WHERE eg.game_seed = ? ORDER BY eg.rowid DESC LIMIT 100"
                ))
                .map_err(StoreError::from)?;
            let scan = |row: &rusqlite::Row<'_>| -> rusqlite::Result<(Episode, EpisodeGameRow)> {
                let ep = scan_episode_row(row)?;

                let game = EpisodeGameRow {
                    game_index: row.get(23)?,
                    pair_index: row.get(24)?,
                    pair_position: row.get(25)?,
                    game_seed: row.get(26)?,
                    a_is_red: row.get::<_, i64>(27)? != 0,
                    seed_refreshed: row.get::<_, i64>(28)? != 0,
                    winner: row.get(29)?,
                    steps: row.get(30)?,
                };
                Ok((ep, game))
            };
            let rows = stmt
                .query_map(rusqlite::params![seed_i], scan)
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
    }

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
            let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            if window_secs > 0 {
                args.push(Box::new(self.now() - window_secs));
            }
            let rows = stmt
                .query_map(
                    rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())),
                    scan,
                )
                .map_err(StoreError::from)?;
            for row in rows {
                let (id, st) = row.map_err(StoreError::from)?;
                out.insert(id, st);
            }
            Ok(())
        })?;
        Ok(out)
    }

    fn episode_rate_estimates(
        &self,
        window_secs: i64,
        samples_per_worker: i64,
    ) -> CoreResult<BTreeMap<String, EpisodeRateEstimate>> {
        let samples = if samples_per_worker <= 0 {
            24
        } else {
            samples_per_worker
        };
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

    fn episode_data_stats(&self) -> CoreResult<EpisodeDataStats> {
        let mut total = BTreeMap::new();
        let mut last_1h = BTreeMap::new();
        let mut last_24h = BTreeMap::new();
        self.with_conn(|conn| {

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
        Ok(EpisodeDataStats {
            total,
            last_1h,
            last_24h,
        })
    }

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
                return Ok(GenerationStats::default());
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
            ..Default::default()
        }
    }

    #[test]
    fn episode_games_store_and_seed_lookup() {
        use bqs_core::types::EpisodeGameRow;

        let (_dir, s) = crate::testutil::open_temp_store();

        let mut d = draft("t1", "episodes/n1/a.epb.gz", 0, 100);
        d.seed_base = Some(9000);
        d.pairs_parallel = Some(true);
        d.pair_seed_refresh = Some(false);
        d.games = vec![
            EpisodeGameRow {
                game_index: 0,
                pair_index: 0,
                pair_position: 0,
                game_seed: Some(9000),
                a_is_red: true,
                seed_refreshed: false,
                winner: Some(1),
                steps: 42,
            },
            EpisodeGameRow {
                game_index: 1,
                pair_index: 0,
                pair_position: 1,
                game_seed: Some(9000),
                a_is_red: false,
                seed_refreshed: false,
                winner: Some(-1),
                steps: 36,
            },
            EpisodeGameRow {
                game_index: 2,
                pair_index: 1,
                pair_position: 0,
                game_seed: Some(9001),
                a_is_red: true,
                seed_refreshed: false,
                winner: Some(0),
                steps: 50,
            },
        ];
        let ep_id = ES::insert_episode(&*s, d).unwrap();

        let games = ES::get_episode_games(&*s, ep_id).unwrap();
        assert_eq!(games.len(), 3);
        assert_eq!(games[1].pair_position, 1);
        assert!(!games[1].a_is_red, "对内第二局应换手（A 执黑）");

        let hits = ES::find_games_by_seed(&*s, 9000).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].0.id, ep_id);
        assert_eq!(hits[0].0.seed_base, Some(9000));
        assert_eq!(hits[0].0.pair_seed_refresh, Some(false));

        assert!(ES::find_games_by_seed(&*s, 777).unwrap().is_empty());
    }

    #[test]
    fn insert_get_by_task_and_page() {
        let (_dir, s) = crate::testutil::open_temp_store();

        let id1 = ES::insert_episode(&*s, draft("t1", "episodes/n1/a.epb.gz", 0, 100)).unwrap();
        let id2 = ES::insert_episode(&*s, draft("t2", "episodes/n1/b.epb.gz", 1, 200)).unwrap();
        assert_ne!(id1, id2);

        let e = ES::get_episode_by_task(&*s, "t1").unwrap().unwrap();
        assert_eq!(e.object_key, "episodes/n1/a.epb.gz");
        assert_eq!(e.created_at, 100);
        assert!(ES::get_episode_by_task(&*s, "nope").unwrap().is_none());

        assert_eq!(
            ES::episode_id_by_key(&*s, "episodes/n1/b.epb.gz").unwrap(),
            Some(id2)
        );
        assert_eq!(ES::episode_id_by_key(&*s, "nope").unwrap(), None);

        let page = ES::list_episodes_page(&*s, 0, 1).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, id2);
        let page = ES::list_episodes_page(&*s, id2, 10).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, id1);

        let e = ES::list_episodes_page(&*s, 0, 10).unwrap();
        let got = e.iter().find(|x| x.id == id1).unwrap();
        assert_eq!(
            (got.red_wins, got.black_wins, got.draw_no_capture),
            (1, 2, 1)
        );
    }

    #[test]
    fn list_rows_cursor_and_kind_filter() {
        let (_dir, s) = crate::testutil::open_temp_store();

        ES::insert_episode(&*s, draft("t1", "k/a", 0, 1)).unwrap();
        ES::insert_episode(&*s, draft("t2", "k/b", 1, 2)).unwrap();
        ES::insert_episode(&*s, draft("t3", "k/c", 2, 3)).unwrap();
        ES::insert_episode(&*s, draft("t4", "k/d", 0, 4)).unwrap();

        let keys = |rows: Vec<bqs_core::types::Episode>| -> Vec<String> {
            rows.into_iter().map(|e| e.object_key).collect()
        };
        let rows = ES::list_episode_rows(&*s, "", 0, None).unwrap();
        assert_eq!(keys(rows), vec!["k/a", "k/b", "k/d"]);

        let rows = ES::list_episode_rows(&*s, "k/b", 0, None).unwrap();
        assert_eq!(keys(rows), vec!["k/d"]);

        let rows = ES::list_episode_rows(&*s, "", 0, Some(1)).unwrap();
        assert_eq!(keys(rows), vec!["k/b"]);

        let rows = ES::list_episode_rows(&*s, "k/a", 0, Some(1)).unwrap();
        assert_eq!(keys(rows), vec!["k/b"]);

        let rows = ES::list_episode_rows(&*s, "", 1, None).unwrap();
        assert_eq!(keys(rows), vec!["k/a"]);

        let rows = ES::list_episode_rows(&*s, "unknown-key", 0, None).unwrap();
        assert_eq!(keys(rows), vec!["k/a", "k/b", "k/d"]);
    }

    #[test]
    fn label_pack_columns_roundtrip() {
        let (_dir, s) = crate::testutil::open_temp_store();

        let mut d = draft("t-label", "episodes/n1/with.lab.v1.lpb.gz", 0, 10);
        d.label_object_key = "episodes/n1/with.lab.v1.lpb.gz".into();
        d.label_sha256 = "deadbeef".into();
        d.label_size = 12345;
        ES::insert_episode(&*s, d).unwrap();

        ES::insert_episode(&*s, draft("t-nolabel", "episodes/n1/bare.epb.gz", 0, 11)).unwrap();

        let with = ES::get_episode_by_task(&*s, "t-label").unwrap().unwrap();
        assert_eq!(with.label_object_key, "episodes/n1/with.lab.v1.lpb.gz");
        assert_eq!(with.label_sha256, "deadbeef");
        assert_eq!(with.label_size, 12345);

        let bare = ES::get_episode_by_task(&*s, "t-nolabel").unwrap().unwrap();
        assert!(bare.label_object_key.is_empty());
        assert!(bare.label_sha256.is_empty());
        assert_eq!(bare.label_size, 0);

        let mut seeded = draft("t-seed", "episodes/n1/seed.epb.gz", 0, 12);
        seeded.games = vec![bqs_core::types::EpisodeGameRow {
            game_index: 0,
            pair_index: 0,
            pair_position: 0,
            game_seed: Some(4242),
            a_is_red: true,
            seed_refreshed: false,
            winner: Some(1),
            steps: 33,
        }];
        ES::insert_episode(&*s, seeded).unwrap();
        let hits = ES::find_games_by_seed(&*s, 4242).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1.steps, 33);
        assert_eq!(hits[0].1.winner, Some(1));
        assert!(hits[0].1.a_is_red);
    }

    #[test]
    fn stats_by_worker_and_rate_estimates() {
        let (_dir, s) = crate::testutil::open_temp_store_at(2000);
        use bqs_core::types::EpisodeDraft;

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

        let st = ES::episode_stats_by_worker(&*s, 0).unwrap();
        assert_eq!(
            st["w1"],
            bqs_core::ports::KindStats {
                episodes: 3,
                games: 70,
                steps: 700
            }
        );
        assert_eq!(st["w2"].games, 8);

        let st = ES::episode_stats_by_worker(&*s, 600).unwrap();
        assert_eq!(
            st["w1"],
            bqs_core::ports::KindStats {
                episodes: 2,
                games: 60,
                steps: 600
            }
        );
        assert_eq!(st["w2"].episodes, 1);

        let est = ES::episode_rate_estimates(&*s, 1000, 2).unwrap();
        assert_eq!(
            est["w1"],
            bqs_core::ports::EpisodeRateEstimate {
                episodes: 2,
                games: 60,
                span_seconds: 400
            }
        );
        assert_eq!(est["w2"].games, 8);

        let est = ES::episode_rate_estimates(&*s, 1000, 0).unwrap();
        assert_eq!(est["w1"].episodes, 3);
        assert_eq!(est["w1"].span_seconds, 800);
    }

    #[test]
    fn data_stats_and_generation_window() {
        let (_dir, s) = crate::testutil::open_temp_store_at(2000);
        use bqs_core::ports::NetworkStore as NS;
        use bqs_core::types::EpisodeDraft;

        NS::register_network(&*s, "cand", None, None, "onnx").unwrap();
        {
            let conn = s.conn.lock().unwrap();
            conn.execute("UPDATE networks SET created_at=1000 WHERE sha='cand'", [])
                .unwrap();
        }
        let mk = |task: &str, games: i64, kind: i64, at: i64| EpisodeDraft {
            task_id: task.into(),
            game_count: games,
            total_steps: games * 10,
            kind,
            created_at: at,
            ..Default::default()
        };

        ES::insert_episode(&*s, mk("g1", 10, 0, 1100)).unwrap();
        ES::insert_episode(&*s, mk("g2", 20, 1, 1200)).unwrap();
        ES::insert_episode(&*s, mk("g3", 40, 2, 1300)).unwrap();
        ES::insert_episode(&*s, mk("g4", 80, 0, 900)).unwrap();

        let stats = ES::episode_data_stats(&*s).unwrap();
        assert_eq!(stats.total[&0].episodes, 2);
        assert_eq!(stats.total[&0].games, 90);
        assert_eq!(stats.total[&1].games, 20);
        assert_eq!(stats.total[&2].games, 40);

        let generation = ES::generation_data_stats(&*s).unwrap();
        assert_eq!(generation.candidate, "cand");
        assert_eq!(generation.since, 1000);
        assert_eq!(generation.episodes, 2);
        assert_eq!(generation.games, 30);
        assert_eq!(generation.steps, 300);
    }

    #[test]
    fn generation_stats_empty_db() {
        let (_dir, s) = crate::testutil::open_temp_store();
        let generation = ES::generation_data_stats(&*s).unwrap();
        assert_eq!(generation, bqs_core::ports::GenerationStats::default());
    }
}
