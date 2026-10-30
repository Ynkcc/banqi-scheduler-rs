use bqs_core::ports::{TrainerRegistryStore, TrainerStore};
use bqs_core::types::Trainer;
use bqs_core::CoreResult;

use crate::{Store, StoreError};

use rusqlite::OptionalExtension;

const TRAINER_COLUMNS: &str = "id, last_seen, variant, kind, consumed_key, consumed_id, \
     samples_consumed, backlog, buffered, health_status, last_error, progress";

fn scan_trainer_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Trainer> {
    Ok(Trainer {
        id: row.get(0)?,
        last_seen: row.get(1)?,
        variant: row.get(2)?,
        kind: row.get(3)?,
        consumed_key: row.get(4)?,
        consumed_id: row.get(5)?,
        samples_consumed: row.get(6)?,
        backlog: row.get(7)?,
        buffered: row.get(8)?,
        health_status: row.get(9)?,
        last_error: row.get(10)?,
        progress: row.get(11)?,
    })
}

impl TrainerStore for Store {
    fn list_trainers(&self) -> CoreResult<Vec<Trainer>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {TRAINER_COLUMNS} FROM trainers ORDER BY last_seen DESC"
                ))
                .map_err(StoreError::from)?;
            let rows = stmt
                .query_map([], scan_trainer_row)
                .map_err(StoreError::from)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
    }

    fn kind_watermark(&self, kind: i64, after_id: i64) -> CoreResult<(i64, i64)> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT COALESCE(MAX(id),0), COUNT(*) FROM episodes WHERE kind=? AND id>?",
                rusqlite::params![kind, after_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(StoreError::from)
        })
    }
}

impl TrainerRegistryStore for Store {
    fn get_trainer(&self, id: &str) -> CoreResult<Option<Trainer>> {
        self.with_conn(|conn| {
            conn.query_row(
                &format!("SELECT {TRAINER_COLUMNS} FROM trainers WHERE id=?"),
                rusqlite::params![id],
                scan_trainer_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    fn touch_trainer(&self, t: Trainer) -> CoreResult<()> {
        let now = self.now();
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO trainers
					(id, last_seen, variant, kind, consumed_key, consumed_id, samples_consumed, backlog, buffered, health_status, last_error, progress)
				 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
				 ON CONFLICT(id) DO UPDATE SET
					last_seen=excluded.last_seen, variant=excluded.variant, kind=excluded.kind,
					consumed_key=excluded.consumed_key, consumed_id=excluded.consumed_id,
					samples_consumed=excluded.samples_consumed, backlog=excluded.backlog, buffered=excluded.buffered,
					health_status=CASE WHEN excluded.health_status='' THEN trainers.health_status ELSE excluded.health_status END,
					last_error=CASE WHEN excluded.last_error='' THEN trainers.last_error ELSE excluded.last_error END,
					progress=CASE WHEN excluded.progress='' THEN trainers.progress ELSE excluded.progress END",
                rusqlite::params![
                    t.id, now, t.variant, t.kind, t.consumed_key, t.consumed_id,
                    t.samples_consumed, t.backlog, t.buffered, t.health_status, t.last_error, t.progress
                ],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use bqs_core::ports::{TrainerRegistryStore as TRS, TrainerStore as TS};
    use bqs_core::types::Trainer;

    fn trainer(id: &str, kind: i64) -> Trainer {
        Trainer {
            id: id.into(),
            variant: "4x8".into(),
            kind,
            backlog: 5,
            buffered: 3,
            samples_consumed: 100,
            health_status: "ok".into(),
            ..Default::default()
        }
    }

    #[test]
    fn touch_get_list_and_health_preserved() {
        let (_dir, s) = crate::testutil::open_temp_store();

        TRS::touch_trainer(&*s, trainer("t1", 1)).unwrap();
        let t = TRS::get_trainer(&*s, "t1").unwrap().unwrap();
        assert_eq!((t.kind, t.backlog, t.samples_consumed), (1, 5, 100));

        TRS::touch_trainer(
            &*s,
            Trainer {
                id: "t1".into(),
                health_status: "".into(),
                ..trainer("t1", 1)
            },
        )
        .unwrap();
        assert_eq!(
            TRS::get_trainer(&*s, "t1").unwrap().unwrap().health_status,
            "ok"
        );

        let mut t2 = trainer("t1", 1);
        t2.health_status = "fatal".into();
        t2.last_error = "crash".into();
        t2.progress = "{\"round\":3}".into();
        TRS::touch_trainer(&*s, t2).unwrap();
        let t = TRS::get_trainer(&*s, "t1").unwrap().unwrap();
        assert_eq!(t.health_status, "fatal");
        assert_eq!(t.last_error, "crash");
        assert_eq!(t.progress, "{\"round\":3}");
        assert!(!t.healthy());

        assert!(TRS::get_trainer(&*s, "ghost").unwrap().is_none());
        assert_eq!(TS::list_trainers(&*s).unwrap().len(), 1);
    }

    #[test]
    fn kind_watermark_counts() {
        let (_dir, s) = crate::testutil::open_temp_store();
        use bqs_core::ports::EpisodeStore as ES;
        use bqs_core::types::EpisodeDraft;

        for (i, kind) in [1i64, 1, 0].into_iter().enumerate() {
            ES::insert_episode(
                &*s,
                EpisodeDraft {
                    task_id: format!("tw-{i}"),
                    kind,
                    ..Default::default()
                },
            )
            .unwrap();
        }

        let (max_id, beyond) = TS::kind_watermark(&*s, 1, 0).unwrap();
        assert!(max_id >= 2);
        assert_eq!(beyond, 2);
        let (max_id, beyond) = TS::kind_watermark(&*s, 1, max_id).unwrap();
        assert_eq!((max_id, beyond), (0, 0));
        let (max_id, beyond) = TS::kind_watermark(&*s, 0, 0).unwrap();
        assert_eq!(beyond, 1);
        assert!(max_id >= 1);
        let (_max, beyond) = TS::kind_watermark(&*s, 7, 0).unwrap();
        assert_eq!(beyond, 0);
    }
}
