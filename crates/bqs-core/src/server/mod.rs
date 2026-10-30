mod episodes;
mod jobs;
mod networks;
mod queues;
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
    evalcoord::EvalCoordinator,
    featurepack::DeriveQueue,
    gatekeeper::Gatekeeper,
    jobs::JobManager,
    ports::{Clock, ObjectSigner, Ports},
    reanalysis::ReanaQueue,
    reports::ReportRouter,
    sources::Sources,
};

pub struct Server {
    pub cfg: Arc<Config>,

    pub ports: Ports,
    pub signer: Arc<dyn ObjectSigner>,
    pub clock: Arc<dyn Clock>,

    pub disp: Arc<TaskDispatcher>,
    pub evalc: Arc<EvalCoordinator>,
    pub gate: Arc<Gatekeeper>,
    pub balance: Arc<BalanceMonitor>,
    pub reana: Arc<ReanaQueue>,
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
        let evalc = Arc::new(EvalCoordinator::new(
            &ports,
            cfg.clone(),
            clock.clone(),
            ctl.clone(),
            disp.clone(),
        ));
        let gate = Arc::new(Gatekeeper::new(&ports, cfg.clone(), evalc.clone()));
        let balance = Arc::new(BalanceMonitor::new(
            &ports,
            cfg.clone(),
            clock.clone(),
            ctl.clone(),
        ));
        let reana = Arc::new(ReanaQueue::new(&ports, cfg.clone(), clock.clone()));
        let derive = Arc::new(DeriveQueue::new(&ports, cfg.clone(), clock.clone()));
        let jobs = Arc::new(JobManager::new(
            ports.jobs.clone(),
            disp.clone(),
            ports.networks.clone(),
            ports.matches.clone(),
            ctl.clone(),
            cfg.clone(),
            clock.clone(),
        ));

        evalc.set_job_hook(&jobs);

        let asm = Arc::new(TaskAssembler::new(
            ports.networks.clone(),
            signer.clone(),
            cfg.clone(),
            ctl.clone(),
        ));
        let router = Arc::new(ReportRouter::new(
            evalc.clone(),
            gate.clone(),
            disp.clone(),
            jobs.clone(),
            ports.matches.clone(),
            ports.networks.clone(),
            cfg.clone(),
        ));
        let sources = Sources::assemble(
            jobs.clone(),
            evalc.clone(),
            reana.clone(),
            derive.clone(),
            ports.matches.clone(),
            ports.networks.clone(),
            disp.clone(),
            asm.clone(),
            ctl.clone(),
            cfg.clone(),
        );

        evalc.ensure_best_evaluated();

        if let Err(e) = jobs.ensure_selfplay_job() {
            panic!("[jobs] selfplay job 引导失败，拒绝启动: {e}");
        }

        Arc::new(Self {
            cfg,
            ports,
            signer,
            clock,
            disp,
            evalc,
            gate,
            balance,
            reana,
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
