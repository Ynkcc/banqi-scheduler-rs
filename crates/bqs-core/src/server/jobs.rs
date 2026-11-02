use bqs_proto::*;

use crate::{CoreResult, jobs::JobSpecJson, ports::JobQuery, ports::JobRow};

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
        let page = self.jobs.list(&JobQuery {
            job_id: (!req.job_id.is_empty()).then_some(req.job_id),
            state: (!req.state_filter.is_empty()).then_some(req.state_filter),
            kind,
            limit: (req.limit > 0).then_some(req.limit),
            offset: req.offset.max(0),
        })?;
        let jobs = page.items.iter().map(|r| self.job_status(r)).collect();
        Ok(ListJobsReply {
            jobs,
            total: page.total,
        })
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
        let collect = match JobSpecJson::parse(&r.spec_json) {
            Some(JobSpecJson::Collect(c)) => Some(c.to_proto()),
            _ => None,
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
            ..Default::default()
        }
    }
}
