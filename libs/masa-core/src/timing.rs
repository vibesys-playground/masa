use std::time::Duration;
#[cfg(not(feature = "test_clock"))]
use std::time::{SystemTime, UNIX_EPOCH};

/// Monotonic instant used by every Masa policy module.
///
/// This is `std::time::Instant` unless the `test_clock` feature is enabled, in
/// which case it reads the virtual clock in [`test_clock`].
#[cfg(not(feature = "test_clock"))]
pub use std::time::Instant;
#[cfg(feature = "test_clock")]
pub use test_clock::Instant;

/// Wall-clock time in microseconds since the Unix epoch.
#[cfg(not(feature = "test_clock"))]
#[inline]
pub fn time_now() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    now as u64
}

/// Virtual time in microseconds; see [`test_clock`].
#[cfg(feature = "test_clock")]
#[inline]
pub fn time_now() -> u64 {
    test_clock::now_us()
}

/// A virtual clock for deterministic tests, compiled in by the `test_clock`
/// feature only.
///
/// `time_now()` and [`Instant`] both read one per-thread microsecond counter
/// that only moves when a test calls [`set_now_us`] or [`advance_us`]. Per-thread
/// state lets parallel tests each own a private timeline.
#[cfg(feature = "test_clock")]
pub mod test_clock {
    use std::cell::Cell;
    use std::ops::{Add, Sub};
    use std::time::Duration;

    /// Where every thread's clock starts: far enough from zero that scenarios
    /// can subtract hours without underflow.
    pub const START_US: u64 = 1_000_000_000_000;

    thread_local! {
        static NOW_US: Cell<u64> = const { Cell::new(START_US) };
    }

    /// Current virtual time of this thread, in microseconds.
    pub fn now_us() -> u64 {
        NOW_US.with(Cell::get)
    }

    /// Jump this thread's clock to `us`.
    pub fn set_now_us(us: u64) {
        NOW_US.with(|now| now.set(us));
    }

    /// Move this thread's clock forward by `us` microseconds.
    pub fn advance_us(us: u64) {
        NOW_US.with(|now| now.set(now.get() + us));
    }

    /// Virtual-clock counterpart of `std::time::Instant`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Instant(u64);

    impl Instant {
        pub fn now() -> Self {
            Self(now_us())
        }

        pub fn elapsed(&self) -> Duration {
            Self::now().duration_since(*self)
        }

        pub fn duration_since(&self, earlier: Self) -> Duration {
            self.saturating_duration_since(earlier)
        }

        pub fn saturating_duration_since(&self, earlier: Self) -> Duration {
            Duration::from_micros(self.0.saturating_sub(earlier.0))
        }
    }

    impl Add<Duration> for Instant {
        type Output = Instant;

        fn add(self, rhs: Duration) -> Instant {
            Instant(self.0 + rhs.as_micros() as u64)
        }
    }

    impl Sub<Duration> for Instant {
        type Output = Instant;

        fn sub(self, rhs: Duration) -> Instant {
            Instant(self.0 - rhs.as_micros() as u64)
        }
    }

    impl Sub<Instant> for Instant {
        type Output = Duration;

        fn sub(self, rhs: Instant) -> Duration {
            self.duration_since(rhs)
        }
    }
}

#[derive(Debug, Clone)]
pub enum LatencyTracker {
    NotStarted,
    Started(Instant),
    Finished(Duration),
}

impl LatencyTracker {
    pub fn start(&mut self) {
        *self = match self {
            Self::NotStarted => Self::Started(Instant::now()),
            _ => panic!("Cannot start tracking latency twice"),
        }
    }

    pub fn record_latency(&mut self) {
        *self = match self {
            Self::Started(inst) => Self::Finished(inst.elapsed()),
            _ => panic!("Cannot record latency if the tracker hasn't started"),
        }
    }

    pub fn get_latency(&self) -> Option<Duration> {
        match self {
            Self::Finished(lat) => Some(*lat),
            _ => None,
        }
    }
}
