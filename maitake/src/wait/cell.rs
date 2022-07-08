use crate::{
    loom::{
        cell::UnsafeCell,
        sync::atomic::{
            AtomicUsize,
            Ordering::{self, *},
        },
    },
    util::tracing,
};
use core::{
    future::Future,
    ops,
    pin::Pin,
    task::{Context, Poll, Waker},
};
use mycelium_util::{fmt, sync::CachePadded};

/// An error indicating that a [`WaitCell`] was closed or busy while
/// attempting register a waiter.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Error {
    Closed,
    Busy,
}

const fn closed() -> Result<(), Error> {
    Err(Error::Closed)
}

const fn registered() -> Result<(), Error> {
    Ok(())
}

const fn busy() -> Result<(), Error> {
    Err(Error::Busy)
}

/// An atomically registered [`Waker`].
///
/// This is inspired by the [`AtomicWaker` type] used in Tokio's
/// synchronization primitives, with the following modifications:
///
/// - An additional bit of state is added to allow setting a "close" bit.
/// - A `WaitCell` is always woken by value (for now).
/// - `WaitCell` does not handle unwinding, because mycelium kernel-mode code
///   does not support unwinding.
///
/// [`AtomicWaker`]: https://github.com/tokio-rs/tokio/blob/09b770c5db31a1f35631600e1d239679354da2dd/tokio/src/sync/task/atomic_waker.rs
/// [`Waker`]: core::task::Waker
pub struct WaitCell {
    lock: CachePadded<AtomicUsize>,
    waker: UnsafeCell<Option<Waker>>,
}

#[derive(Eq, PartialEq, Copy, Clone)]
struct State(usize);

/// Future returned from [`WaitCell::wait()`].
///
/// This future is fused, so once it has completed, any future calls to poll
/// will immediately return [`Poll::Ready`].
#[derive(Debug)]
#[must_use = "futures do nothing unless `.await`ed or `poll`ed"]
pub struct Wait<'a> {
    /// The [`WaitQueue`] being waited on from.
    cell: &'a WaitCell,

    /// Whether we have already polled once
    registered: bool,
}

// === impl WaitCell ===

impl WaitCell {
    #[cfg(not(all(loom, test)))]
    pub const fn new() -> Self {
        Self {
            lock: CachePadded::new(AtomicUsize::new(State::WAITING.0)),
            waker: UnsafeCell::new(None),
        }
    }

    #[cfg(all(loom, test))]
    pub fn new() -> Self {
        Self {
            lock: CachePadded::new(AtomicUsize::new(State::WAITING.0)),
            waker: UnsafeCell::new(None),
        }
    }
}

impl WaitCell {
    pub fn register_wait(&self, waker: &Waker) -> Result<(), Error> {
        tracing::trace!(wait_cell = ?fmt::ptr(self), ?waker, "registering waker");

        // this is based on tokio's AtomicWaker synchronization strategy
        match test_dbg!(self.compare_exchange(State::WAITING, State::PARKING, Acquire)) {
            // someone else is notifying, so don't wait!
            Err(actual) if test_dbg!(actual.is(State::CLOSED)) => {
                return closed();
            }
            Err(actual) if test_dbg!(actual.is(State::NOTIFYING)) => {
                return busy();
            }

            Err(actual) => {
                debug_assert!(
                    actual == State::PARKING || actual == State::PARKING | State::NOTIFYING
                );
                return busy();
            }
            Ok(_) => {}
        }

        test_trace!("-> wait cell locked!");
        let prev_waker = self.waker.with_mut(|old_waker| unsafe {
            match &mut *old_waker {
                Some(old_waker) if waker.will_wake(old_waker) => None,
                old => old.replace(waker.clone()),
            }
        });

        if let Some(prev_waker) = prev_waker {
            test_trace!("Replaced an old waker in cell, waking");
            prev_waker.wake();
        }

        match test_dbg!(self.compare_exchange(State::PARKING, State::WAITING, AcqRel)) {
            Ok(_) => registered(),
            Err(actual) => {
                test_trace!(state = ?actual, "was notified");
                let waker = self.waker.with_mut(|waker| unsafe { (*waker).take() });
                // Reset to the WAITING state by clearing everything *except*
                // the closed bits (which must remain set).
                let state = test_dbg!(self.fetch_and(State::CLOSED, AcqRel));
                // The only valid state transition while we were parking is to
                // add the CLOSED bit.
                debug_assert!(
                    state == actual || state == actual | State::CLOSED,
                    "state changed unexpectedly while parking!"
                );

                if let Some(waker) = waker {
                    waker.wake();
                }

                // We just went to the closed state, so sorry new waker, shops
                // closed
                closed()
            }
        }
    }

    /// Wait to be woken up by this cell.
    ///
    /// Note: The waiter is not registered until AFTER the first time the waiter is polled
    pub fn wait(&self) -> Wait<'_> {
        Wait {
            cell: self,
            registered: false,
        }
    }

    pub fn wake(&self) -> bool {
        self.notify2(State::WAITING)
    }

    pub fn close(&self) {
        self.notify2(State::CLOSED);
    }

    fn notify2(&self, close: State) -> bool {
        tracing::trace!(wait_cell = ?fmt::ptr(self), ?close, "notifying");
        let bits = State::NOTIFYING | close;
        if test_dbg!(self.fetch_or(bits, AcqRel)) == State::WAITING {
            // we have the lock!
            let waker = self.waker.with_mut(|thread| unsafe { (*thread).take() });

            test_dbg!(self.fetch_and(!State::NOTIFYING, AcqRel));

            if let Some(waker) = test_dbg!(waker) {
                tracing::trace!(wait_cell = ?fmt::ptr(self), ?close, ?waker, "notified");
                waker.wake();
                return true;
            }
        }
        false
    }
}

impl WaitCell {
    #[inline(always)]
    fn compare_exchange(
        &self,
        State(curr): State,
        State(new): State,
        success: Ordering,
    ) -> Result<State, State> {
        self.lock
            .compare_exchange(curr, new, success, Acquire)
            .map(State)
            .map_err(State)
    }

    #[inline(always)]
    fn fetch_and(&self, State(state): State, order: Ordering) -> State {
        State(self.lock.fetch_and(state, order))
    }

    #[inline(always)]
    fn fetch_or(&self, State(state): State, order: Ordering) -> State {
        State(self.lock.fetch_or(state, order))
    }

    #[inline(always)]
    fn current_state(&self) -> State {
        State(self.lock.load(Acquire))
    }
}

unsafe impl Send for WaitCell {}
unsafe impl Sync for WaitCell {}

impl fmt::Debug for WaitCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitCell")
            .field("state", &self.current_state())
            .field("waker", &fmt::display(".."))
            .finish()
    }
}

// === impl Wait ===

impl Future for Wait<'_> {
    type Output = Result<(), super::Closed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.registered {
            // We made it to "once", and got polled again, We must be ready!
            return Poll::Ready(Ok(()));
        }

        match test_dbg!(self.cell.register_wait(cx.waker())) {
            Ok(_) => {
                self.registered = true;
                Poll::Pending
            }
            Err(Error::Busy) => {
                // Cell was busy, all we can do is try again later
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(Error::Closed) => super::closed(),
        }
    }
}

// === impl State ===

impl State {
    const WAITING: Self = Self(0b00);
    const PARKING: Self = Self(0b01);
    const NOTIFYING: Self = Self(0b10);
    const CLOSED: Self = Self(0b100);

    fn is(self, Self(state): Self) -> bool {
        self.0 & state == state
    }
}

impl ops::BitOr for State {
    type Output = Self;

    fn bitor(self, Self(rhs): Self) -> Self::Output {
        Self(self.0 | rhs)
    }
}

impl ops::Not for State {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self(!self.0)
    }
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut has_states = false;

        fmt_bits!(self, f, has_states, PARKING, NOTIFYING, CLOSED);

        if !has_states {
            if *self == Self::WAITING {
                return f.write_str("WAITING");
            }

            f.debug_tuple("UnknownState")
                .field(&format_args!("{:#b}", self.0))
                .finish()?;
        }

        Ok(())
    }
}

#[cfg(all(feature = "alloc", not(loom), test))]
mod test {
    use super::*;
    use crate::scheduler::Scheduler;
    use alloc::sync::Arc;

    #[test]
    fn wait_smoke() {
        static COMPLETED: AtomicUsize = AtomicUsize::new(0);

        let sched = Scheduler::new();
        let wait = Arc::new(WaitCell::new());

        let wait2 = wait.clone();
        sched.spawn(async move {
            wait2.wait().await.unwrap();
            COMPLETED.fetch_add(1, Ordering::Relaxed);
        });

        let tick = sched.tick();
        assert_eq!(tick.completed, 0);
        assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);

        assert_eq!(wait.wake(), true);
        let tick = sched.tick();
        assert_eq!(tick.completed, 1);
        assert_eq!(COMPLETED.load(Ordering::Relaxed), 1);
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;

    use crate::loom::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    use std::sync::Arc;

    #[derive(Debug)]
    pub(crate) struct Chan {
        num: AtomicUsize,
        task: WaitCell,
        num_notify: usize,
    }

    impl Chan {
        pub(crate) fn new(num_notify: usize) -> Arc<Self> {
            Arc::new(Self {
                num: AtomicUsize::new(0),
                task: WaitCell::new(),
                num_notify,
            })
        }

        pub(crate) async fn wait(self: Arc<Chan>) {
            let this = Arc::downgrade(&self);
            drop(self);
            futures_util::future::poll_fn(move |cx| {
                let this = match this.upgrade() {
                    Some(this) => this,
                    None => return Poll::Ready(()),
                };

                let res = this.task.wait();
                futures_util::pin_mut!(res);

                if this.num_notify == this.num.load(Relaxed) {
                    return Poll::Ready(());
                }

                res.poll(cx).map(drop)
            })
            .await
        }

        pub(crate) fn wake(&self) {
            self.num.fetch_add(1, Relaxed);
            self.task.wake();
        }

        #[allow(dead_code)]
        pub(crate) fn close(&self) {
            self.num.fetch_add(1, Relaxed);
            self.task.close();
        }
    }

    impl Drop for Chan {
        fn drop(&mut self) {
            tracing::debug!(chan = ?fmt::alt(self), "drop")
        }
    }
}

#[cfg(all(loom, test))]
mod loom {
    use super::*;
    use crate::loom::{future, sync::Arc, thread};
    use futures::{select_biased, FutureExt};
    use tracing::info;

    #[test]
    fn basic() {
        crate::loom::model(|| {
            let wake = Arc::new(WaitCell::new());

            let waiter = wake.clone();
            let closer = wake.clone();

            thread::spawn(move || {
                info!("waking");
                waiter.wake();
                info!("woke'd");
            });
            thread::spawn(move || {
                info!("closing");
                closer.close();
                info!("closed");
            });

            info!("waiting");
            let _ = future::block_on(async move {
                // Unfortunately, SOMETIMES loom just wants to preempt the 'waker' task
                // JUST as it gains access to the lock, but BEFORE actually waking the pending task,
                // never yielding back to the waker task, which then just essentially leaves it stuck
                // in the NOTIFYING state.
                //
                // This happens in the `notify2` function, AFTER the `fetch_or` call, and BEFORE
                // the `fetch_and` call.
                //
                // This never allows the waker to complete and free the wait lock. This means that the waiter
                // can never check to see if the waiting is complete, because it hasn't been notified!
                //
                // For this reason, we limit the number of poll attempts to 10, because otherwise
                // the loom test will deadlock.
                //
                // In the future, if we have a variant of the `wait()` function that DOESN'T retry
                // forever, e.g. with some max, or no retries-on-busy ever, we can likely replace this
                // hack with that approach instead.
                //
                // Sorry, Eliza.
                // -James
                let mut wait = wake.wait().fuse();

                // Use select_biased with a ready future to wake the waiter up to 10 times,
                // before we bail and move on with our lives
                for _ in 0..10 {
                    let mut ready = futures::future::ready(());
                    select_biased! {
                        _ = wait => break,
                        _ = ready => {},
                    };
                }
            });
            info!("wait'd");
        });
    }
}
