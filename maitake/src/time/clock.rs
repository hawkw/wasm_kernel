use super::{
    timer::{self, TimerError},
    Duration,
};
use core::{
    fmt,
    ops::{Add, AddAssign, Sub, SubAssign},
};

// /// A hardware clock.
// pub trait Clock: Send + Sync + 'static {
//     /// Returns the current hardware timestamp, in ticks of this `Clock`.
//     ///
//     /// # Monotonicity
//     ///
//     /// Implementations of this method MUST ensure that timestampss returned by
//     /// `now()` MUST be [monontonically non-decreasing]. This means that a call
//     /// to `now()` MUST NOT ever  return a value less than the value returned by
//     /// a previous call to`now()`.
//     ///
//     /// Note that this means that timestamps returned by `now()` are expected
//     /// not to overflow. Of course, all integers *will* overflow eventually, so
//     /// this requirement can reasonably be weakened to expecting that timestamps
//     /// returned by `now()` will not overflow unless the system has been running
//     /// for a duration substantially longer than the system is expected to run
//     /// for. For example, if a system is expected to run for as long as a year
//     /// without being restarted, it's not unreasonable for timestamps returned
//     /// by `now()` to overflow after, say, 100 years. Ideally, a general-purpose
//     /// `Clock` implementation would not overflow for, say, 1,000 years.
//     ///
//     /// The implication of this is that if the timestamp counters provided by
//     /// the hardware platform are less than 64 bits wide (e.g., 16- or 32-bit
//     /// timestamps), the `Clock` implementation is responsible for ensuring that
//     /// they are extended to 64 bits, such as by counting overflows in the
//     /// `Clock` implementation.
//     ///
//     /// [monotonic]: https://en.wikipedia.org/wiki/Monotonic_function
//     #[must_use]
//     fn now_ticks(&self) -> Ticks;

//     #[must_use]
//     fn tick_duration(&self) -> Duration;

//     #[must_use]
//     fn now(&self) -> Instant {
//         let now = self.now_ticks();
//         let tick_duration = self.tick_duration();
//         Instant { now, tick_duration }
//     }

//     fn max_duration(&self) -> Duration {
//         max_duration(self.tick_duration())
//     }
// }

/// A hardware clock definition.
///
/// A `Clock` consists of a function that returns the hardware clock's current
/// timestamp in [`Ticks`], and a [`Duration`] that defines the amount of time
/// represented by a single tick of the clock.
#[derive(Clone, Debug)]
pub struct Clock {
    now: fn() -> Ticks,
    tick_duration: Duration,
    name: Option<&'static str>,
}

/// A measurement of a monotonically nondecreasing clock.
/// Opaque and useful only with [`Duration`].
///
/// Provided that the [`Clock` implementation is correct, `Instant`s are always
/// guaranteed to be no less than any previously measured instant when created,
/// and are often useful for tasks such as measuring benchmarks or timing how
/// long an operation takes.
///
/// Note, however, that instants are **not** guaranteed to be **steady**. In other
/// words, each tick of the underlying clock might not be the same length (e.g.
/// some seconds may be longer than others). An instant may jump forwards or
/// experience time dilation (slow down or speed up), but it will never go
/// backwards.
/// As part of this non-guarantee it is also not specified whether system suspends count as
/// elapsed time or not. The behavior varies across platforms and rust versions.
///
/// Instants are opaque types that can only be compared to one another. There is
/// no method to get "the number of seconds" from an instant. Instead, it only
/// allows measuring the duration between two instants (or comparing two
/// instants).
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Instant(Duration);

/// Timer ticks are always counted by a 64-bit unsigned integer.
pub type Ticks = u64;

impl Clock {
    pub const fn new(tick_duration: Duration, now: fn() -> Ticks) -> Self {
        Self {
            now,
            tick_duration,
            name: None,
        }
    }

    pub const fn named(self, name: &'static str) -> Self {
        Self {
            name: Some(name),
            ..self
        }
    }

    #[must_use]
    pub(crate) fn now_ticks(&self) -> Ticks {
        (self.now)()
    }

    #[must_use]
    pub fn tick_duration(&self) -> Duration {
        self.tick_duration
    }

    #[must_use]
    pub fn now(&self) -> Instant {
        let now = self.now_ticks();
        let tick_duration = self.tick_duration();
        Instant(ticks_to_dur(tick_duration, now))
    }

    pub fn max_duration(&self) -> Duration {
        max_duration(self.tick_duration())
    }
}

#[track_caller]
#[inline]
#[must_use]
pub(in crate::time) fn ticks_to_dur(tick_duration: Duration, ticks: Ticks) -> Duration {
    tick_duration.saturating_mul(ticks as u32)
}

#[track_caller]
#[inline]
#[must_use]
pub(in crate::time) fn dur_to_ticks(
    tick_duration: Duration,
    dur: Duration,
) -> Result<Ticks, TimerError> {
    (dur.as_nanos() / tick_duration.as_nanos())
        .try_into()
        .map_err(|_| TimerError::DurationTooLong {
            requested: dur,
            max: max_duration(tick_duration),
        })
}

#[track_caller]
#[inline]
#[must_use]
pub(in crate::time) fn max_duration(tick_duration: Duration) -> Duration {
    ticks_to_dur(tick_duration, Ticks::MAX)
}

impl Instant {
    /// Returns an instant corresponding to "now".
    ///
    /// This function uses the [global default timer][global]. See [the
    /// module-level documentation][global] for details on using the global
    /// default timer.
    ///
    /// # Panics
    ///
    /// This function panics if the [global default timer][global] has not been
    /// set.
    ///
    /// For a version of this function that returns a [`Result`] rather than
    /// panicking, use [`Instant::try_now`] instead.
    ///
    /// [global]: crate::time#global-timers
    #[must_use]
    pub fn now() -> Instant {
        Self::try_now().expect("no global timer set")
    }

    /// Returns an instant corresponding to "now", without panicking.
    ///
    /// This function uses the [global default timer][global]. See [the
    /// module-level documentation][global] for details on using the global
    /// default timer.
    ///
    /// # Returns
    ///
    /// - [`Ok`]`(`[`Instant`]`)` if the [global default timer] is available.
    /// - [`Err`]`(`[`TimerError`]`)` if no [global default timer][global] has
    ///   been set.
    ///
    /// [global]: crate::time#global-timers
    pub fn try_now() -> Result<Self, TimerError> {
        Ok(timer::global::default()?.now())
    }

    /// Returns the amount of time elapsed from another instant to this one,
    /// or zero duration if that instant is later than this one.
    #[must_use]
    pub fn duration_since(&self, earlier: Instant) -> Duration {
        self.checked_duration_since(earlier).unwrap_or_default()
    }

    /// Returns the amount of time elapsed from another instant to this one,
    /// or [`None`]` if that instant is later than this one.
    #[must_use]
    pub fn checked_duration_since(&self, earlier: Instant) -> Option<Duration> {
        self.0.checked_sub(earlier.0)
    }

    /// Returns the amount of time elapsed since this instant.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.0
    }

    /// Returns `Some(t)` where `t` is the time `self + duration` if `t` can be represented as
    /// `Instant` (which means it's inside the bounds of the underlying data structure), [`None`]
    /// otherwise.
    #[must_use]
    pub fn checked_add(&self, duration: Duration) -> Option<Instant> {
        self.0.checked_add(duration).map(Instant)
    }

    /// Returns `Some(t)` where `t` is the time `self - duration` if `t` can be represented as
    /// `Instant` (which means it's inside the bounds of the underlying data structure), [`None`]
    /// otherwise.
    #[must_use]
    pub fn checked_sub(&self, duration: Duration) -> Option<Instant> {
        self.0.checked_sub(duration).map(Instant)
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    /// # Panics
    ///
    /// This function may panic if the resulting point in time cannot be represented by the
    /// underlying data structure. See [`Instant::checked_add`] for a version without panic.
    fn add(self, other: Duration) -> Instant {
        self.checked_add(other)
            .expect("overflow when adding duration to instant")
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, other: Duration) {
        *self = *self + other;
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;

    fn sub(self, other: Duration) -> Instant {
        self.checked_sub(other)
            .expect("overflow when subtracting duration from instant")
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, other: Duration) {
        *self = *self - other;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    /// Returns the amount of time elapsed from another instant to this one,
    /// or zero duration if that instant is later than this one.
    fn sub(self, other: Instant) -> Duration {
        self.duration_since(other)
    }
}

impl fmt::Display for Instant {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}
