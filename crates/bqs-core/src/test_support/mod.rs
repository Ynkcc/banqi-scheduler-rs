mod clock;
mod episodes;
mod eval;
mod jobstore;
mod matches;
mod networks;
mod packs;
mod registry;
mod settings;
mod signer;
mod trainers;
mod workers;

pub use clock::FakeClock;
pub use episodes::FakeEpisodes;
pub use eval::FakeEval;
pub use jobstore::FakeJobs;
pub use matches::FakeMatches;
pub use networks::FakeNetworks;
pub use packs::FakePacks;
pub use registry::FakeRegistry;
pub use settings::FakeSettings;
pub use signer::FakeSigner;
pub use trainers::FakeTrainers;
pub use workers::FakeWorkers;

use std::sync::Arc;

use crate::ports::Ports;

#[derive(Debug, Default)]
pub struct Fakes {
    pub clock: Arc<FakeClock>,
    pub settings: Arc<FakeSettings>,
    pub networks: Arc<FakeNetworks>,
    pub matches: Arc<FakeMatches>,
    pub eval: Arc<FakeEval>,
    pub trainers: Arc<FakeTrainers>,
    pub registry: Arc<FakeRegistry>,
    pub episodes: Arc<FakeEpisodes>,
    pub workers: Arc<FakeWorkers>,
    pub packs: Arc<FakePacks>,
    pub jobs: Arc<FakeJobs>,
    pub signer: Arc<FakeSigner>,
}

impl Fakes {
    pub fn new() -> Self {
        let trainers = Arc::new(FakeTrainers::default());
        let registry = Arc::new(FakeRegistry::default());

        registry.set_mirror(trainers.clone());
        Self {
            clock: FakeClock::new(1_000),
            settings: Arc::new(FakeSettings::default()),
            networks: Arc::new(FakeNetworks::default()),
            matches: Arc::new(FakeMatches::default()),
            eval: Arc::new(FakeEval::default()),
            trainers,
            registry,
            episodes: Arc::new(FakeEpisodes::default()),
            workers: Arc::new(FakeWorkers::default()),
            packs: Arc::new(FakePacks::default()),
            jobs: Arc::new(FakeJobs::default()),
            signer: Arc::new(FakeSigner),
        }
    }

    pub fn ports(&self) -> Ports {
        Ports {
            settings: self.settings.clone(),
            networks: self.networks.clone(),
            matches: self.matches.clone(),
            eval: self.eval.clone(),
            trainers: self.trainers.clone(),
            episodes: self.episodes.clone(),
            workers: self.workers.clone(),
            trainer_registry: self.registry.clone(),
            feature_packs: self.packs.clone(),
            jobs: self.jobs.clone(),
        }
    }
}
