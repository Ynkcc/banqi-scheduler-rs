mod episodes;
mod jobs;
mod networks;
mod trainer;
mod worker;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use bqs_config::Config;

use crate::{
    assembler::TaskAssembler,
    balance::BalanceMonitor,
    control::Control,
    dispatch::TaskDispatcher,
    gatekeeper::Gatekeeper,
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
    pub gate: Arc<Gatekeeper>,
    pub balance: Arc<BalanceMonitor>,
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
        let gate = Arc::new(Gatekeeper::new(&ports, cfg.clone()));
        let balance = Arc::new(BalanceMonitor::new(
            &ports,
            cfg.clone(),
            clock.clone(),
            ctl.clone(),
        ));
        let jobs = Arc::new(JobManager::new(
            ports.jobs.clone(),
            disp.clone(),
            ports.networks.clone(),
            ports.matches.clone(),
            ctl.clone(),
            cfg.clone(),
            clock.clone(),
        ));

        let asm = Arc::new(TaskAssembler::new(
            ports.networks.clone(),
            signer.clone(),
            cfg.clone(),
            ctl.clone(),
        ));
        let router = Arc::new(ReportRouter::new(
            gate.clone(),
            disp.clone(),
            jobs.clone(),
            ports.matches.clone(),
            ports.networks.clone(),
            cfg.clone(),
        ));
        let sources = Sources::assemble(
            jobs.clone(),
            ports.matches.clone(),
            ports.networks.clone(),
            disp.clone(),
            asm.clone(),
            ctl.clone(),
            cfg.clone(),
        );

        if let Err(e) = jobs.ensure_selfplay_job() {
            panic!("[jobs] selfplay job 引导失败，拒绝启动: {e}");
        }

        Arc::new(Self {
            cfg,
            ports,
            signer,
            clock,
            disp,
            gate,
            balance,
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
