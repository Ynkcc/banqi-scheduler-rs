use bqs_proto::JobKind;

use super::JobManager;
use super::spec::{JobSpecJson, RatingSpecJson};
use crate::{CoreResult, ports::JobPage, ports::JobQuery, ports::JobRow};

impl JobManager {
    pub fn list(&self, query: &JobQuery) -> CoreResult<JobPage> {
        self.store.list_jobs(query)
    }

    pub fn get(&self, job_id: &str) -> CoreResult<Option<JobRow>> {
        self.store.get_job(job_id)
    }

    pub fn dispatchable(&self, kind: JobKind) -> CoreResult<Vec<JobRow>> {
        let all = self
            .store
            .list_jobs(&JobQuery {
                kind: Some(kind as i64),
                ..JobQuery::all()
            })?
            .items;
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
                Ok(JobSpecJson::Rating(r)) => Some(r),
                _ => None,
            }))
    }
}
