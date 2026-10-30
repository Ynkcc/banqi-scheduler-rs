use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use crate::ports::Clock;

#[derive(Debug, Default)]
pub struct FakeClock(AtomicI64);

impl FakeClock {
    pub fn new(start: i64) -> Arc<Self> {
        let c = Self::default();
        c.0.store(start, Ordering::SeqCst);
        Arc::new(c)
    }

    pub fn set(&self, t: i64) {
        self.0.store(t, Ordering::SeqCst);
    }

    pub fn advance(&self, d: i64) {
        self.0.fetch_add(d, Ordering::SeqCst);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}
