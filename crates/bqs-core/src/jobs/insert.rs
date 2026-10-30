use bqs_proto::{JobKind, SubmitJobRequest};

use super::JobManager;
use super::spec::{
    CollectSpecJson, JobSpecJson, RatingSpecJson, SelfPlaySpecJson, spec_total,
};
use crate::CoreResult;

impl JobManager {
    fn insert_new_job(
        &self,
        kind: JobKind,
        priority: i64,
        total: i64,
        spec: &JobSpecJson,
    ) -> CoreResult<String> {
        let job_id = crate::sources::new_id();
        let now = self.now();
        self.store.insert_job(crate::ports::JobRow {
            job_id: job_id.clone(),
            kind: kind as i64,
            priority,
            state: "running".into(),
            total_games: total,
            completed_games: 0,
            dispatched_games: 0,
            spec_json: spec.to_json(),
            created_at: now,
            updated_at: now,
            next_seq: 0,
        })?;
        Ok(job_id)
    }

    pub fn submit(&self, req: &SubmitJobRequest) -> CoreResult<(bool, String, String)> {
        let Ok(kind) = JobKind::try_from(req.kind) else {
            return Ok((
                false,
                format!("invalid_job_kind={}", req.kind),
                String::new(),
            ));
        };
        let priority = if req.priority == 0 {
            self.default_priority(kind)
        } else {
            req.priority as i64
        };
        let spec: JobSpecJson = match kind {
            JobKind::JobCollect => {
                let Some(c) = &req.collect else {
                    return Ok((
                        false,
                        "collect spec required for JOB_COLLECT".into(),
                        String::new(),
                    ));
                };
                match self.validate_collect(c) {
                    Ok(()) => JobSpecJson::Collect(CollectSpecJson::from_proto(c.clone())),
                    Err(m) => return Ok((false, m, String::new())),
                }
            }
            JobKind::JobSelfplay => {
                let Some(s) = &req.selfplay else {
                    return Ok((
                        false,
                        "selfplay spec required for JOB_SELFPLAY".into(),
                        String::new(),
                    ));
                };
                match self.validate_selfplay(s) {
                    Ok(()) => JobSpecJson::SelfPlay(SelfPlaySpecJson::from_proto(s.clone())),
                    Err(m) => return Ok((false, m, String::new())),
                }
            }
            JobKind::JobRating => {
                let Some(r) = &req.rating else {
                    return Ok((
                        false,
                        "rating spec required for JOB_RATING".into(),
                        String::new(),
                    ));
                };

                let (side_a, side_b) = match self.validate_and_resolve_rating(r) {
                    Ok(v) => v,
                    Err(m) => return Ok((false, m, String::new())),
                };
                let mut spec = RatingSpecJson::from_proto(r.clone());
                spec.side_a = side_a.clone();
                spec.side_b = side_b.clone();

                let match_id = self.matches.create_match(
                    &side_a.network_sha,
                    &side_b.network_sha,
                    (spec.total_games / 2) as i64,
                )?;
                spec.match_id = match_id;
                tracing::info!(
                    "[job] rating match created match={} side_a={}:{} side_b={}:{} target_pairs={}",
                    match_id,
                    side_a.kind,
                    side_a.network_sha,
                    side_b.kind,
                    side_b.network_sha,
                    spec.total_games / 2
                );
                JobSpecJson::Rating(spec)
            }
        };
        let total = spec_total(&spec) as i64;
        let job_id = self.insert_new_job(kind, priority, total, &spec)?;
        tracing::info!(
            "[job] submitted requester={} id={} kind={} priority={} total={}",
            req.requester,
            job_id,
            kind.as_str_name(),
            priority,
            total
        );

        self.preemption_sweep(true);
        Ok((true, String::new(), job_id))
    }

    pub fn ensure_selfplay_job(&self) -> CoreResult<()> {
        let existing = self
            .store
            .list_jobs(None, Some(JobKind::JobSelfplay as i64))?;
        if existing
            .iter()
            .any(|j| j.state == "running" || j.state == "paused")
        {
            return Ok(());
        }
        let spec = SelfPlaySpecJson {
            variant: self.cfg.selfplay.variant.clone(),
            data_kind: crate::data_kind::to_proto(self.ctl.data_kind()),
            network_sha: String::new(),
            opponent_sha: String::new(),
            mix_ratio: 0.0,
            search: None,
            total_games: 0,
        };
        let job_id = self.insert_new_job(
            JobKind::JobSelfplay,
            self.default_priority(JobKind::JobSelfplay),
            0,
            &JobSpecJson::SelfPlay(spec),
        )?;
        tracing::info!("[job] bootstrapped persistent selfplay job id={job_id}");
        Ok(())
    }
}
