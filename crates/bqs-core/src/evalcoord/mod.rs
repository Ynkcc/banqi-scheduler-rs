mod claim;
mod judge;
mod queue;
mod report;
mod settings;
mod types;
mod views;

#[cfg(test)]
mod tests;

pub use types::{
    BestMutator, ClaimedEvalTask, EvalOverride, EvalProgress, EvalReportAck, EvalRunningJob,
    PendingEval, StoreBestMutator,
};
pub use views::eval_search_mode_name;

pub const SETTING_EVAL_PROMOTIONS: &str = "eval_promotions";

use std::sync::{Arc, Mutex, Weak};

use bqs_config::Config;

use crate::{
    control::Control,
    dispatch::TaskDispatcher,
    ports::{Clock, EvalStore, NetworkStore},
};

#[derive(Default)]
struct Inner {
    evals: Vec<PendingEval>,
    promotions: i64,
}

pub struct EvalCoordinator {
    cfg: Arc<Config>,
    ctl: Arc<Control>,
    clock: Arc<dyn Clock>,
    eval: Arc<dyn EvalStore>,
    nets: Arc<dyn NetworkStore>,
    disp: Arc<TaskDispatcher>,
    mutator: Box<dyn BestMutator>,
    inner: Mutex<Inner>,

    job_hook: Mutex<Option<Weak<crate::jobs::JobManager>>>,
}

impl EvalCoordinator {
    pub fn new(
        ports: &crate::ports::Ports,
        cfg: Arc<Config>,
        clock: Arc<dyn Clock>,
        ctl: Arc<Control>,
        disp: Arc<TaskDispatcher>,
    ) -> Self {
        let this = Self {
            cfg,
            ctl,
            clock,
            eval: ports.eval.clone(),
            nets: ports.networks.clone(),
            disp,
            mutator: Box::new(StoreBestMutator::new(ports.networks.clone())),
            inner: Mutex::new(Inner::default()),
            job_hook: Mutex::new(None),
        };
        this.restore_queue();
        this
    }

    pub fn set_job_hook(&self, jobs: &Arc<crate::jobs::JobManager>) {
        *self.job_hook.lock().unwrap() = Some(Arc::downgrade(jobs));
    }

    pub(super) fn notify_job_created(&self, sha: &str, spec: &str, opponent_sha: &str) {
        let hook = self.job_hook.lock().unwrap().clone();
        if let Some(jobs) = hook.and_then(|w| w.upgrade()) {
            if let Err(e) = jobs.create_eval_job(sha, spec, opponent_sha) {
                tracing::warn!(
                    "[eval] ⚠️ 创建 JOB_EVAL job 失败 network={sha} opponent={spec}: {e}"
                );
            }
        }
    }
}
