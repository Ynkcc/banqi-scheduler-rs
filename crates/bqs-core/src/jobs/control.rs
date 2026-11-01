use bqs_proto::JobKind;

use super::spec::JobSpecJson;
use super::JobManager;
use crate::consts;
use crate::ports::JobQuery;
use crate::CoreResult;

impl JobManager {
    pub fn control(&self, job_id: &str, action: &str) -> CoreResult<(bool, String, String)> {
        let Some(job) = self.store.get_job(job_id)? else {
            return Ok((false, format!("job_not_found:{job_id}"), String::new()));
        };
        let now = self.now();
        match action {
            "pause" => {
                if job.state != "running" {
                    return Ok((
                        false,
                        format!("pause_requires_running state={}", job.state),
                        job.state,
                    ));
                }
                self.store.update_job_state(job_id, "paused", now)?;
                tracing::info!("[job] paused id={job_id}");
                Ok((true, String::new(), "paused".into()))
            }
            "resume" => {
                if job.state != "paused" {
                    return Ok((
                        false,
                        format!("resume_requires_paused state={}", job.state),
                        job.state,
                    ));
                }
                self.store.update_job_state(job_id, "running", now)?;
                tracing::info!("[job] resumed id={job_id}");

                self.preemption_sweep(true);
                Ok((true, String::new(), "running".into()))
            }
            "cancel" => {
                if job.state == "completed" || job.state == "cancelled" {
                    return Ok((
                        false,
                        format!("job_is_terminal state={}", job.state),
                        job.state,
                    ));
                }
                self.store.update_job_state(job_id, "cancelled", now)?;

                self.cancel_in_flight_for_job(job_id);

                if job.kind == JobKind::JobRating as i64 {
                    self.cancel_rating_match(&job.spec_json);
                }
                tracing::info!("[job] cancelled id={job_id}（在飞任务已加入取消集合）");
                Ok((true, String::new(), "cancelled".into()))
            }
            other => Ok((
                false,
                format!("invalid_control=\"{other}\"（可选 pause/resume/cancel）"),
                job.state,
            )),
        }
    }

    pub fn note_dispatch(
        &self,
        task_id: &str,
        job_id: &str,
        games: i64,
        worker_id: &str,
        kind: bqs_proto::TaskKind,
        network_sha: &str,
        opponent_sha: &str,
        opponent_spec: &str,
        match_id: i64,
        deadline_secs_override: Option<i64>,
    ) -> CoreResult<i64> {
        let row = self.store.incr_job_dispatched(job_id, games, self.now())?;
        self.store.insert_job_task(task_id, job_id, games)?;
        let mut t = crate::dispatch::RunningTask::new(kind);
        t.worker_id = worker_id.to_string();
        t.network_sha = network_sha.to_string();
        t.opponent_sha = opponent_sha.to_string();
        t.opponent_spec = opponent_spec.to_string();
        t.games = games;
        t.match_id = match_id;

        t.gating = kind == bqs_proto::TaskKind::TaskRating || match_id != 0;
        t.job_id = job_id.to_string();
        self.disp.register(task_id, t);
        if let Some(extra) = deadline_secs_override {
            if extra > 0 {
                self.disp.override_deadline(task_id, self.now() + extra);
            }
        }
        Ok(row.next_seq - 1)
    }

    pub fn record_report(&self, task_id: &str, games: i64) -> CoreResult<()> {
        let Some(link) = self.store.get_job_task(task_id)? else {
            return Ok(());
        };
        if games <= 0 {
            return Ok(());
        }
        let row = self
            .store
            .incr_job_completed(&link.job_id, games, self.now())?;
        self.store.delete_job_task(task_id)?;
        let total = row.total_games;
        if total > 0 && row.completed_games >= total && row.state == "running" {
            self.store
                .update_job_state(&link.job_id, "completed", self.now())?;
            self.cancel_in_flight_for_job(&link.job_id);
            tracing::info!(
                "[job] ✅ completed id={} completed={}/{}（在飞任务已加入取消集合）",
                link.job_id,
                row.completed_games,
                total
            );
        } else if total > 0 {
            tracing::info!(
                "[job] += id={} task={} games={} completed={}/{}",
                link.job_id,
                task_id,
                games,
                row.completed_games,
                total
            );
        } else {
            tracing::info!(
                "[job] += id={} task={} games={} completed={}（持续型）",
                link.job_id,
                task_id,
                games,
                row.completed_games
            );
        }
        Ok(())
    }

    pub fn cancel_in_flight_for_job(&self, job_id: &str) {
        let in_flight: Vec<(String, String)> = self
            .disp
            .running()
            .into_iter()
            .filter(|t| t.job_id == job_id)
            .map(|t| (t.task_id, t.worker_id))
            .collect();
        let mut inner = self.inner.lock().unwrap();
        for (task_id, worker_id) in in_flight {
            inner.cancelled.entry(task_id).or_insert(worker_id);
        }
    }

    pub fn preemption_sweep(&self, force: bool) {
        let now = self.now();
        {
            let mut inner = self.inner.lock().unwrap();
            if !force {
                if let Some(last) = inner.last_preempt {
                    if now - last < consts::JOB_PREEMPT_INTERVAL.as_secs() as i64 {
                        return;
                    }
                }
            }
            inner.last_preempt = Some(now);
        }
        let jobs = match self.store.list_jobs(&JobQuery::all()) {
            Ok(p) => p.items,
            Err(e) => {
                tracing::warn!("[job] ⚠️ 抢占评估读取 job 列表失败: {e}");
                return;
            }
        };
        let max_dispatchable = jobs
            .iter()
            .filter(|j| {
                j.state == "running" && (j.total_games == 0 || j.completed_games < j.total_games)
            })
            .map(|j| j.priority)
            .max();
        let Some(max_p) = max_dispatchable else {
            return;
        };
        for j in jobs.iter().filter(|j| {
            j.state == "running"
                && j.priority < max_p
                && (j.total_games == 0 || j.completed_games < j.total_games)
        }) {
            self.cancel_in_flight_for_job(&j.job_id);
        }
    }

    pub fn cancel_task_ids_for_worker(&self, worker_id: &str) -> Vec<String> {
        let mut inner = self.inner.lock().unwrap();
        let mut out = Vec::new();
        let ids: Vec<String> = inner.cancelled.keys().cloned().collect();
        for id in ids {
            match self.disp.lookup(&id) {
                Some(t) if t.worker_id == worker_id => {
                    out.push(id.clone());
                    inner.cancelled.remove(&id);
                }
                Some(_) => {}
                None => {
                    inner.cancelled.remove(&id);
                }
            }
        }
        out
    }

    fn cancel_rating_match(&self, spec_json: &str) {
        if let Some(JobSpecJson::Rating(spec)) = JobSpecJson::parse(spec_json) {
            if spec.match_id != 0 {
                match self.matches.cancel_match(spec.match_id) {
                    Ok(true) => {
                        tracing::info!("[job] rating match {} cancelled（不晋级）", spec.match_id)
                    }
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!(
                            "[job] ⚠️ rating match 取消失败 match={}: {e}",
                            spec.match_id
                        )
                    }
                }
            }
        }
    }

    pub fn complete_rating_job(&self, job_id: &str) -> CoreResult<()> {
        let Some(job) = self.store.get_job(job_id)? else {
            return Ok(());
        };
        if job.state != "running" && job.state != "paused" {
            return Ok(());
        }
        self.store
            .update_job_state(job_id, "completed", self.now())?;
        self.cancel_in_flight_for_job(job_id);
        tracing::info!(
            "[job] ✅ rating job completed id={job_id}（对打已判停/达上限，在飞已取消）"
        );
        Ok(())
    }
}
