use bqs_proto::JobKind;

use super::JobManager;
use super::spec::{JobSpecJson, RatingSpecJson};
use crate::{CoreResult, ports::JobRow};

impl JobManager {
    pub fn list(&self, state: Option<&str>, kind: Option<i64>) -> CoreResult<Vec<JobRow>> {
        self.store.list_jobs(state, kind)
    }

    pub fn get(&self, job_id: &str) -> CoreResult<Option<JobRow>> {
        self.store.get_job(job_id)
    }

    pub fn dispatchable(&self, kind: JobKind) -> CoreResult<Vec<JobRow>> {
        let all = self.store.list_jobs(None, Some(kind as i64))?;
        Ok(all
            .into_iter()
            .filter(|j| {
                j.state == "running" && (j.total_games == 0 || j.completed_games < j.total_games)
            })
            .collect())
    }

    pub fn rating_spec(&self, job_id: &str) -> CoreResult<Option<RatingSpecJson>> {
        if job_id.is_empty() {
            return Ok(None);
        }
        Ok(self
            .store
            .get_job(job_id)?
            .and_then(|j| match JobSpecJson::parse(&j.spec_json) {
                Some(JobSpecJson::Rating(r)) => Some(r),
                _ => None,
            }))
    }

    pub fn is_rating_match(&self, match_id: i64) -> bool {
        self.store
            .list_jobs(None, Some(JobKind::JobRating as i64))
            .map(|rows| {
                rows.iter().any(|j| {
                    matches!(
                        JobSpecJson::parse(&j.spec_json),
                        Some(JobSpecJson::Rating(r)) if r.match_id == match_id
                    )
                })
            })
            .unwrap_or(false)
    }
}
