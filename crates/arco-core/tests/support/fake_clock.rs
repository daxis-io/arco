#![allow(dead_code)]

use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone)]
pub struct FakeClock {
    now: DateTime<Utc>,
}

impl FakeClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self { now }
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.now
    }

    pub fn advance(&mut self, duration: Duration) -> DateTime<Utc> {
        self.now += duration;
        self.now
    }

    pub fn rewind(&mut self, duration: Duration) -> DateTime<Utc> {
        self.now -= duration;
        self.now
    }
}
