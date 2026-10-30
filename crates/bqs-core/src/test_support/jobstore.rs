use std::collections::HashMap;
use std::sync::Mutex;

use crate::{
    ports::{JobRow, JobStore, JobTaskLink},
    CoreError, CoreResult,
};

#[derive(Debug, Default)]
pub struct FakeJobs {
    inner: Mutex<FakeJobsInner>,
}

#[derive(Debug, Default)]
struct FakeJobsInner {
    jobs: HashMap<String, JobRow>,
    tasks: HashMap<String, JobTaskLink>,
    next_id: i64,
}

impl JobStore for FakeJobs {
    fn insert_job(&self, row: JobRow) -> CoreResult<()> {
        self.inner
            .lock()
            .unwrap()
            .jobs
            .insert(row.job_id.clone(), row);
        Ok(())
    }

    fn get_job(&self, job_id: &str) -> CoreResult<Option<JobRow>> {
        Ok(self.inner.lock().unwrap().jobs.get(job_id).cloned())
    }

    fn list_jobs(&self, state: Option<&str>, kind: Option<i64>) -> CoreResult<Vec<JobRow>> {
        let inner = self.inner.lock().unwrap();
        let mut rows: Vec<JobRow> = inner
            .jobs
            .values()
            .filter(|j| state.is_none_or(|s| j.state == s) && kind.is_none_or(|k| j.kind == k))
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.created_at.cmp(&b.created_at))
        });
        Ok(rows)
    }

    fn update_job_state(&self, job_id: &str, state: &str, now: i64) -> CoreResult<bool> {
        let mut inner = self.inner.lock().unwrap();
        match inner.jobs.get_mut(job_id) {
            Some(j) => {
                j.state = state.to_string();
                j.updated_at = now;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn incr_job_dispatched(&self, job_id: &str, games: i64, now: i64) -> CoreResult<JobRow> {
        let mut inner = self.inner.lock().unwrap();
        let j = inner
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| CoreError::Store(format!("job {job_id} not found")))?;
        j.dispatched_games += games;
        j.next_seq += 1;
        j.updated_at = now;
        Ok(j.clone())
    }

    fn incr_job_completed(&self, job_id: &str, games: i64, now: i64) -> CoreResult<JobRow> {
        let mut inner = self.inner.lock().unwrap();
        let j = inner
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| CoreError::Store(format!("job {job_id} not found")))?;
        j.completed_games += games;
        j.updated_at = now;
        Ok(j.clone())
    }

    fn insert_job_task(&self, task_id: &str, job_id: &str, games: i64) -> CoreResult<()> {
        self.inner.lock().unwrap().tasks.insert(
            task_id.to_string(),
            JobTaskLink {
                task_id: task_id.to_string(),
                job_id: job_id.to_string(),
                games,
            },
        );
        Ok(())
    }

    fn get_job_task(&self, task_id: &str) -> CoreResult<Option<JobTaskLink>> {
        Ok(self.inner.lock().unwrap().tasks.get(task_id).cloned())
    }

    fn delete_job_task(&self, task_id: &str) -> CoreResult<()> {
        self.inner.lock().unwrap().tasks.remove(task_id);
        Ok(())
    }

    fn update_job_spec(&self, job_id: &str, spec_json: &str, now: i64) -> CoreResult<()> {
        let mut inner = self.inner.lock().unwrap();
        match inner.jobs.get_mut(job_id) {
            Some(j) => {
                j.spec_json = spec_json.to_string();
                j.updated_at = now;
                Ok(())
            }
            None => Err(CoreError::Store(format!("job {job_id} not found"))),
        }
    }
}

impl FakeJobs {
    pub fn seed_job(&self, row: JobRow) {
        self.inner
            .lock()
            .unwrap()
            .jobs
            .insert(row.job_id.clone(), row);
    }

    pub fn next_id(&self) -> i64 {
        let mut inner = self.inner.lock().unwrap();
        inner.next_id += 1;
        inner.next_id
    }
}
