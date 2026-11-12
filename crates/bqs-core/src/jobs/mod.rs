mod control;
mod insert;
mod query;
mod spec;
mod validate;

#[cfg(test)]
mod tests;

pub use spec::{
    CollectSpecJson, JobSpecJson, PlayerSpecJson, RatingSpecJson, SearchSpecJson, SeedStrategyJson,
    canonical_scenario, resolve_seed, scenario_echo,
};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bqs_config::Config;
use bqs_proto::JobKind;

use crate::{
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
        cfg: Arc<Config>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            store,
            disp,
            nets,
            matches,
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
            JobKind::JobSelfplay => crate::consts::JOB_PRIORITY_SELFPLAY,
            JobKind::JobCollect => self.cfg.jobs.priority_collect,
            JobKind::JobRating => self.cfg.jobs.priority_rating,
        }
    }
}
