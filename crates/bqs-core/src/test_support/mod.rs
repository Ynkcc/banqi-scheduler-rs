mod clock;
mod episodes;
mod jobstore;
mod matches;
mod networks;
mod packs;
mod settings;
mod signer;
mod workers;

pub use clock::FakeClock;
pub use episodes::FakeEpisodes;
pub use jobstore::FakeJobs;
pub use matches::FakeMatches;
pub use networks::FakeNetworks;
pub use packs::FakePacks;
pub use settings::FakeSettings;
pub use signer::FakeSigner;
pub use workers::FakeWorkers;

use std::sync::Arc;

use crate::ports::{Ports, StoreCounts, StoreStats};

#[derive(Debug, Default)]
pub struct FakeStats;

impl StoreStats for FakeStats {
    fn counts(&self) -> crate::CoreResult<StoreCounts> {
        Ok(StoreCounts::default())
    }
}

#[derive(Debug, Default)]
pub struct Fakes {
    pub clock: Arc<FakeClock>,
    pub settings: Arc<FakeSettings>,
    pub networks: Arc<FakeNetworks>,
    pub matches: Arc<FakeMatches>,
    pub episodes: Arc<FakeEpisodes>,
    pub workers: Arc<FakeWorkers>,
    pub packs: Arc<FakePacks>,
    pub jobs: Arc<FakeJobs>,
    pub signer: Arc<FakeSigner>,
}

impl Fakes {
    pub fn new() -> Self {
        Self {
            clock: FakeClock::new(1_000),
            settings: Arc::new(FakeSettings::default()),
            networks: Arc::new(FakeNetworks::default()),
            matches: Arc::new(FakeMatches::default()),
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
            stats: Arc::new(FakeStats),
            episodes: self.episodes.clone(),
            workers: self.workers.clone(),
            feature_packs: self.packs.clone(),
            jobs: self.jobs.clone(),
        }
    }
}
