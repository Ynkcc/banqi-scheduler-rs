mod derive;
mod job;

#[cfg(test)]
mod tests;

pub use derive::DeriveSource;
pub use job::JobSource;

use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{TaskRequest, TaskResponse};

use crate::{
    CoreResult,
    assembler::TaskAssembler,
    dispatch::TaskDispatcher,
    featurepack::DeriveQueue,
    jobs::JobManager,
    ports::{MatchStore, NetworkStore},
};

pub trait TaskSource: Send + Sync {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>>;
}

pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

pub fn random_seed() -> u64 {
    let id = new_id();
    u64::from_str_radix(&id[..16.min(id.len())], 16)
        .unwrap_or_else(|_| u64::from_str_radix(&id[16..32], 16).unwrap_or(0))
}

pub struct Sources;

impl Sources {
    #[allow(clippy::too_many_arguments)]
    pub fn assemble(
        jobs: Arc<JobManager>,
        derive: Arc<DeriveQueue>,
        matches: Arc<dyn MatchStore>,
        nets: Arc<dyn NetworkStore>,
        disp: Arc<TaskDispatcher>,
        asm: Arc<TaskAssembler>,
        cfg: Arc<Config>,
    ) -> Vec<Box<dyn TaskSource>> {
        vec![
            Box::new(DeriveSource {
                derive,
                disp: disp.clone(),
                asm: asm.clone(),
            }),
            Box::new(JobSource {
                jobs,
                matches,
                nets,
                disp,
                asm,
                cfg,
            }),
        ]
    }
}
