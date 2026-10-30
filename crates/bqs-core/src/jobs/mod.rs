mod control;
mod insert;
mod query;
mod spec;
mod validate;

#[cfg(test)]
mod tests;

pub use spec::{
    CollectSpecJson, EvalSpecJson, JobSpecJson, PlayerSpecJson, RatingSpecJson, SearchSpecJson,
    SeedStrategyJson, SelfPlaySpecJson, resolve_seed, valid_scenario,
};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bqs_config::Config;
use bqs_proto::JobKind;

use crate::{
    control::Control,
    dispatch::TaskDispatcher,
    ports::{Clock, JobStore, MatchStore, NetworkStore},
};

#[derive(Default)]
struct Inner {
    cancelled: HashMap<String, String>,

    last_preempt: Option<i64>,
}

pub struct JobManager {
    store: Arc<dyn JobStore>,
    disp: Arc<TaskDispatcher>,
    nets: Arc<dyn NetworkStore>,
    matches: Arc<dyn MatchStore>,
    ctl: Arc<Control>,
    cfg: Arc<Config>,
    clock: Arc<dyn Clock>,
    inner: Mutex<Inner>,
}

impl JobManager {
    pub fn new(
        store: Arc<dyn JobStore>,
        disp: Arc<TaskDispatcher>,
        nets: Arc<dyn NetworkStore>,
        matches: Arc<dyn MatchStore>,
        ctl: Arc<Control>,
        cfg: Arc<Config>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            store,
            disp,
            nets,
            matches,
            ctl,
            cfg,
            clock,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub(super) fn now(&self) -> i64 {
        self.clock.now()
    }

    pub(super) fn default_priority(&self, kind: JobKind) -> i64 {
        match kind {
            JobKind::JobSelfplay => self.cfg.jobs.priority_selfplay,
            JobKind::JobCollect => self.cfg.jobs.priority_collect,
            JobKind::JobEval => self.cfg.jobs.priority_eval,
            JobKind::JobRating => self.cfg.jobs.priority_rating,
        }
    }
}
