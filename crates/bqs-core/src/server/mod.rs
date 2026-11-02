mod episodes;
mod jobs;
mod networks;
mod queues;
mod worker;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use bqs_config::Config;

use crate::{
    assembler::TaskAssembler,
    control::Control,
    dispatch::TaskDispatcher,
    featurepack::DeriveQueue,
    jobs::JobManager,
    ports::{Clock, ObjectSigner, Ports},
    reports::ReportRouter,
    sources::Sources,
};

pub struct Server {
    pub cfg: Arc<Config>,

    pub ports: Ports,
    pub signer: Arc<dyn ObjectSigner>,
    pub clock: Arc<dyn Clock>,

    pub disp: Arc<TaskDispatcher>,
    pub derive: Arc<DeriveQueue>,
    pub jobs: Arc<JobManager>,
    pub ctl: Arc<Control>,

    pub asm: Arc<TaskAssembler>,

    pub router: Arc<ReportRouter>,

    pub sources: Vec<Box<dyn crate::sources::TaskSource>>,
}

pub struct ServerDeps {
    pub cfg: Config,
    pub ports: Ports,
    pub signer: Arc<dyn ObjectSigner>,
    pub clock: Arc<dyn Clock>,
}

impl Server {
    pub fn assemble(deps: ServerDeps) -> Arc<Self> {
        let cfg = Arc::new(deps.cfg);
        let ports = deps.ports;
        let signer = deps.signer;
        let clock = deps.clock;

        let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), clock.clone()));
        let ctl = Arc::new(Control::new(&ports, cfg.clone()));
        let derive = Arc::new(DeriveQueue::new(&ports, cfg.clone(), clock.clone()));
        let jobs = Arc::new(JobManager::new(
            ports.jobs.clone(),
            disp.clone(),
            ports.networks.clone(),
            ports.matches.clone(),
            cfg.clone(),
            clock.clone(),
        ));

        let asm = Arc::new(TaskAssembler::new(signer.clone(), cfg.clone(), ctl.clone()));
        let router = Arc::new(ReportRouter::new(
            disp.clone(),
            jobs.clone(),
            ports.matches.clone(),
            cfg.clone(),
        ));
        let sources = Sources::assemble(
            jobs.clone(),
            derive.clone(),
            ports.matches.clone(),
            ports.networks.clone(),
            disp.clone(),
            asm.clone(),
            cfg.clone(),
        );

        Arc::new(Self {
            cfg,
            ports,
            signer,
            clock,
            disp,
            derive,
            jobs,
            ctl,
            asm,
            router,
            sources,
        })
    }

    fn ttl_secs(&self) -> i64 {
        crate::consts::PRESIGN_TTL.as_secs() as i64
    }
}
