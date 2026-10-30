use bqs_proto::*;

use crate::{CoreResult, jobs::JobSpecJson, ports::JobRow};

use super::Server;

impl Server {
    pub fn submit_job(&self, req: SubmitJobRequest) -> CoreResult<SubmitJobReply> {
        let (accepted, message, job_id) = self.jobs.submit(&req)?;
        Ok(SubmitJobReply {
            accepted,
            message,
            job_id,
        })
    }

    pub fn list_jobs(&self, req: ListJobsRequest) -> CoreResult<ListJobsReply> {
        let kind = req.kind_filter.map(|k| k as i64);
        let rows = self.jobs.list(
            if req.state_filter.is_empty() {
                None
            } else {
                Some(req.state_filter.as_str())
            },
            kind,
        )?;
        let jobs = rows.iter().map(|r| self.job_status(r)).collect();
        Ok(ListJobsReply { jobs })
    }

    pub fn control_job(&self, req: JobControlRequest) -> CoreResult<JobControlReply> {
        let (accepted, message, state) = self.jobs.control(&req.job_id, &req.control)?;
        Ok(JobControlReply {
            accepted,
            message,
            state,
        })
    }

    fn job_status(&self, r: &JobRow) -> JobStatus {
        let kind = JobKind::try_from(r.kind as i32).unwrap_or(JobKind::JobCollect);
        let (collect, selfplay, rating) = match JobSpecJson::parse(&r.spec_json) {
            Some(JobSpecJson::Collect(c)) => (Some(c.to_proto()), None, None),
            Some(JobSpecJson::SelfPlay(s)) => (None, Some(s.to_proto()), None),
            Some(JobSpecJson::Rating(rt)) => (None, None, Some(rt.to_proto())),
            None => (None, None, None),
        };
        JobStatus {
            job_id: r.job_id.clone(),
            kind: kind as i32,
            priority: r.priority as u32,
            state: r.state.clone(),
            total_games: r.total_games as u64,
            completed_games: r.completed_games as u64,
            dispatched_games: r.dispatched_games as u64,
            created_at: r.created_at,
            updated_at: r.updated_at,
            collect,
            selfplay,
            rating,
        }
    }
}
