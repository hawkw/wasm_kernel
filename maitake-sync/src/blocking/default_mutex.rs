//! Default "chef's choice" [`ScopedRawMutex`] implementation.
//!
//! This type is what users will get when they don't override the `Lock` type
//! parameter for `maitake-sync`'s synchronziation primitives.
//!
//! # Notes
//!
//! - The `DefaultMutex` cannot ever implement `RawMutex`, only
//!   `ScopedRawMutex`. This is because it's impossible for a
//!   `critical-section`-based implementation to implement `RawMutex`, due to
//!   [`critical-section`'s safety requirements][cs-reqs], which we can't uphold
//!   in a RAII situation with multiple locks. If we implemented `RawMutex` for
//!   the non-`critical-section` implementations, then the `critical-section`
//!   feature flag would *take away* methods that would otherwise be available,
//!   making it non-additive, which is a BIG NO-NO for feature flags.
//!
//! - On the other hand, it *is* okay to have `cfg(loom)` not implement
//!   `ConstInit` where every other implementation does. This is because `cfg(loom)`
//!   is a `RUSTFLAGS` cfg rather than a feature flag, and therefore can only be
//!   enabled by the top-level build. It can't be enabled by a dependency and
//!   suddenly make your code not compile. Loom users are already used to stuff
//!   being const-initializable in real life, but not in loom tests, so this is
//!   more okay.
//!
//! [cs-reqs]:
//!     https://docs.rs/critical-section/latest/critical_section/fn.acquire.html#safety
#[cfg(not(loom))]
use super::ConstInit;
use super::ScopedRawMutex;

/// Default, best-effort [`ScopedRawMutex`] implementation.
///
/// This is the default `Lock` type parameter for the [`Mutex`](crate::Mutex)
/// type, and for the async synchronization primitives that use the blocking
/// `Mutex`. This type makes a best-effort attempt to Do The Right Thing based
/// on the currently enabled [feature flags]. In particular, here's what we
/// currently give you:
///
/// - **If `cfg(loom)` is enabled, then the `DefaultMutex` is a [`loom` mutex]**
///   so that `maitake-sync` primitives work nicely in `loom` tests
///
/// - **If the `std` feature is enabled, then the `DefaultMutex` is a
///   [`std::sync::Mutex`]**, so that `std` users get an OS mutex rather than a
///   spinlock.
///
/// - **If the `critical-section` feature is enabled, then the `DefaultMutex` is
///   a spinlock that [acquires a critical section][cs] once locked**. This
///   ensures that bare-metal users who have enabled `critical-section` get a
///   mutex that disables IRQs when locked.
///
/// - **Otherwise, the `DefaultMutex` is a [`Spinlock`]**. This is the default
///   behavior and will at least work on all platforms, but may not be the most
///   efficient, and may not be IRQ-safe.
///
/// # Notes
///
/// - Regardless of feature flags, this type implements the [`ScopedRawMutex`]
///   trait, *not* the [`RawMutex`] trait. In order to use methods or types that
///   require a [`RawMutex`], you must [provide your own `RawMutex`
///   type][overriding].
/// - :warning: If the `critical-section` feature is enabled, you **MUST**
///   provide a `critical-section` implementation.  See the [`critical-section`
///   documentation][cs-providing] for details on how to select an
///   implementation. If you don't provide an implementation, you'll get a
///   [linker error][cs-link-err] when compiling your code.
/// - This type has a `const fn new()` constructor and implements the
///   [`ConstInit` trait](super::ConstInit) *except* when `cfg(loom)` is
///   enabled.
///
///   Loom users are probably already aware that `loom`'s simulated
///   types cannot be const initialized, as they must bind to the *current* test
///   iteration when constructed. This is not a non-additive feature flag,
///   because `loom` support can only be enabled by a `RUSTFLAGS` cfg set by the
///   top-level build, and not by a dependency.`s`
///
/// [feature flags]: crate#features
/// [`loom` mutex]: https://docs.rs/loom/latest/loom/sync/struct.Mutex.html
/// [cs]: https://docs.rs/critical-section/latest/critical_section/fn.with.html
/// [`Spinlock`]: crate::spin::Spinlock
/// [overriding]: crate::blocking#overriding-mutex-implementations
/// [`RawMutex`]: mutex_traits::RawMutex
/// [cs-providing]:
///     https://docs.rs/critical-section/latest/critical_section/index.html#usage-in-no-std-binaries
/// [cs-link-err]:
///     https://docs.rs/critical-section/latest/critical_section/index.html#undefined-reference-errors
///
// N.B. that this is a wrapper type around the various impls, rather than just a
// re-export, because I didn't want to duplicate the docs for all the impls...
#[must_use = "why create a `DefaultMutex` if you're not going to lock it?"]
pub struct DefaultMutex(Inner);

impl DefaultMutex {
    loom_const_fn! {
        /// Returns a new `DefaultMutex`.
        ///
        /// See the [type-level documentation](Self) for details on how to use a `DefaultMutex`.
        // loom::sync::Mutex`'s constructor captures the location it's
        // constructed, so that we can track where it came from in test output.
        // That's nice, let's not break it!
        #[track_caller]
        #[inline]
        pub fn new() -> Self {
            Self(Inner::new())
        }
    }
}

impl Default for DefaultMutex {
    #[track_caller] // again, for Loom Reasons
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for DefaultMutex {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(not(loom))]
impl ConstInit for DefaultMutex {
    // As is traditional, clippy is wrong about this.
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: Self = Self::new();
}

unsafe impl ScopedRawMutex for DefaultMutex {
    #[track_caller]
    fn with_lock<R>(&self, f: impl FnOnce() -> R) -> R {
        self.0.with_lock(f)
    }

    #[track_caller]
    fn try_with_lock<R>(&self, f: impl FnOnce() -> R) -> Option<R> {
        self.0.try_with_lock(f)
    }

    #[inline]
    fn is_locked(&self) -> bool {
        self.0.is_locked()
    }
}

#[cfg(loom)]
use loom_impl::LoomDefaultMutex as Inner;

#[cfg(all(not(loom), feature = "std"))]
use std_impl::StdDefaultMutex as Inner;

#[cfg(all(not(loom), not(feature = "std"), feature = "critical-section"))]
use cs_impl::CriticalSectionDefaultMutex as Inner;

#[cfg(all(not(loom), not(feature = "std"), not(feature = "critical-section")))]
use spin_impl::SpinDefaultMutex as Inner;

#[cfg(loom)]
mod loom_impl {
    use super::ScopedRawMutex;
    use core::panic::Location;
    use tracing::{debug, debug_span};

    #[derive(Debug)]
    pub(super) struct LoomDefaultMutex(loom::sync::Mutex<()>);

    impl LoomDefaultMutex {
        // loom::sync::Mutex`'s constructor captures the location it's
        // constructed, so that we can track where it came from in test output.
        // That's nice, let's not break it!
        #[track_caller]
        pub(super) fn new() -> Self {
            Self(loom::sync::Mutex::new(()))
        }
    }

    unsafe impl ScopedRawMutex for LoomDefaultMutex {
        #[track_caller]
        #[inline]
        fn with_lock<R>(&self, f: impl FnOnce() -> R) -> R {
            let location = Location::caller();
            trace!(
                target: "maitake_sync::blocking",
                %location,
                "DefaultMutex::with_lock()",
            );

            let guard = self.0.lock();
            let _span = debug_span!(
                target: "maitake_sync::blocking",
                "locked",
                %location,
            )
            .entered();
            debug!(
                target: "maitake_sync::blocking",
                "DefaultMutex::with_lock() -> locked",
            );

            let result = f();
            drop(guard);

            debug!(
                target: "maitake_sync::blocking",
                "DefaultMutex::with_lock() -> unlocked",
            );

            result
        }

        #[track_caller]
        #[inline]
        fn try_with_lock<R>(&self, f: impl FnOnce() -> R) -> Option<R> {
            let location = Location::caller();
            trace!(
                target: "maitake_sync::blocking",
                %location,
                "DefaultMutex::try_with_lock()",
            );

            match self.0.try_lock() {
                Ok(guard) => {
                    let _span = debug_span!(target: "maitake_sync::blocking", "locked", %location)
                        .entered();
                    debug!(
                        target: "maitake_sync::blocking",
                        "DefaultMutex::try_with_lock() -> locked",
                    );

                    let result = f();
                    drop(guard);

                    debug!(
                        target: "maitake_sync::blocking",
                        "DefaultMutex::try_with_lock() -> unlocked",
                    );

                    Some(result)
                }
                Err(_) => {
                    debug!(
                        target: "maitake_sync::blocking",
                        %location,
                        "DefaultMutex::try_with_lock() -> already locked",
                    );

                    None
                }
            }
        }

        fn is_locked(&self) -> bool {
            self.0.try_lock().is_err()
        }
    }
}

#[cfg(all(not(loom), feature = "std"))]
mod std_impl {
    use super::ScopedRawMutex;
    #[derive(Debug)]
    #[must_use]
    pub(super) struct StdDefaultMutex(std::sync::Mutex<()>);

    impl StdDefaultMutex {
        #[inline]
        pub(super) const fn new() -> Self {
            Self(std::sync::Mutex::new(()))
        }
    }

    unsafe impl ScopedRawMutex for StdDefaultMutex {
        #[track_caller]
        #[inline(always)]
        fn with_lock<R>(&self, f: impl FnOnce() -> R) -> R {
            let _guard = self.0.lock().unwrap();
            f()
        }

        #[track_caller]
        #[inline(always)]
        fn try_with_lock<R>(&self, f: impl FnOnce() -> R) -> Option<R> {
            let _guard = self.0.try_lock().ok()?;
            Some(f())
        }

        fn is_locked(&self) -> bool {
            self.0.try_lock().is_ok()
        }
    }
}

#[cfg(all(not(loom), not(feature = "std"), feature = "critical-section"))]
mod cs_impl {
    use super::ScopedRawMutex;
    use crate::spin::Spinlock;

    #[derive(Debug)]
    pub(super) struct CriticalSectionDefaultMutex(Spinlock);

    impl CriticalSectionDefaultMutex {
        #[inline]
        pub(super) const fn new() -> Self {
            Self(Spinlock::new())
        }
    }

    unsafe impl ScopedRawMutex for CriticalSectionDefaultMutex {
        #[track_caller]
        #[inline(always)]
        fn with_lock<R>(&self, f: impl FnOnce() -> R) -> R {
            critical_section::with(|_cs| self.0.with_lock(f))
        }

        #[track_caller]
        #[inline(always)]
        fn try_with_lock<R>(&self, f: impl FnOnce() -> R) -> Option<R> {
            critical_section::with(|_cs| self.0.try_with_lock(f))
        }

        #[inline]
        fn is_locked(&self) -> bool {
            self.0.is_locked()
        }
    }
}

#[cfg(all(not(loom), not(feature = "std"), not(feature = "critical-section")))]
mod spin_impl {
    use super::ScopedRawMutex;
    use crate::spin::Spinlock;

    #[derive(Debug)]
    pub(super) struct SpinDefaultMutex(Spinlock);

    impl SpinDefaultMutex {
        #[inline]
        pub(super) const fn new() -> Self {
            Self(Spinlock::new())
        }
    }

    unsafe impl ScopedRawMutex for SpinDefaultMutex {
        #[track_caller]
        #[inline(always)]
        fn with_lock<R>(&self, f: impl FnOnce() -> R) -> R {
            self.0.with_lock(f)
        }

        #[track_caller]
        #[inline(always)]
        fn try_with_lock<R>(&self, f: impl FnOnce() -> R) -> Option<R> {
            self.0.try_with_lock(f)
        }

        #[inline(always)]
        fn is_locked(&self) -> bool {
            self.0.is_locked()
        }
    }
}

#[cfg(test)]
mod test {
    use super::DefaultMutex;

    // Check that a `DefaultMutex` will always implement the traits we expect it
    // to.
    #[test]
    fn default_mutex_trait_impls() {
        fn assert_scoped_raw_mutex<T: mutex_traits::ScopedRawMutex>() {}
        fn assert_send_and_sync<T: Send + Sync>() {}
        fn assert_default<T: Default>() {}
        fn assert_debug<T: core::fmt::Debug>() {}

        assert_scoped_raw_mutex::<DefaultMutex>();
        assert_send_and_sync::<DefaultMutex>();
        assert_default::<DefaultMutex>();
        assert_debug::<DefaultMutex>();
    }

    // Check that a non-`loom` `DefaultMutex` has a const-fn constructor, and
    // implements `ConstInit`.
    #[cfg(not(loom))]
    #[test]
    fn const_constructor() {
        fn assert_const_init<T: mutex_traits::ConstInit>() {}

        assert_const_init::<DefaultMutex>();

        static _MY_COOL_MUTEX: DefaultMutex = DefaultMutex::new();
    }
}
