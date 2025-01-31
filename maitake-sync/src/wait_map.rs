//! A map of [`Waker`]s associated with keys, so that a task can be woken by
//! key.
//!
//! See the documentation for the [`WaitMap`] type for details.
use crate::{
    blocking::{DefaultMutex, Mutex, ScopedRawMutex},
    loom::{
        cell::UnsafeCell,
        sync::atomic::{AtomicUsize, Ordering::*},
    },
    util::{fmt, CachePadded, WakeBatch},
};
use cordyceps::{
    list::{self, List},
    Linked,
};
use core::{
    fmt::Debug,
    future::Future,
    marker::PhantomPinned,
    mem,
    pin::Pin,
    ptr::{self, NonNull},
    task::{Context, Poll, Waker},
};
use mycelium_bitfield::{enum_from_bits, FromBits};
use pin_project::{pin_project, pinned_drop};

#[cfg(test)]
mod tests;

/// Errors returned by [`WaitMap::wait`], indicating a failed wake.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WaitError {
    /// The [`WaitMap`] has already been [closed].
    ///
    /// [closed]: WaitMap::close
    Closed,

    /// The received data has already been extracted
    AlreadyConsumed,

    /// The [`Wait`] was never added to the [`WaitMap`]
    NeverAdded,

    /// The [`WaitMap`] already had an item matching the given
    /// key
    Duplicate,
}

/// The result of a call to [`WaitMap::wait()`].
pub type WaitResult<T> = Result<T, WaitError>;

const fn closed<T>() -> Poll<WaitResult<T>> {
    Poll::Ready(Err(WaitError::Closed))
}

const fn consumed<T>() -> Poll<WaitResult<T>> {
    Poll::Ready(Err(WaitError::AlreadyConsumed))
}

const fn never_added<T>() -> Poll<WaitResult<T>> {
    Poll::Ready(Err(WaitError::NeverAdded))
}

const fn duplicate<T>() -> Poll<WaitResult<T>> {
    Poll::Ready(Err(WaitError::Duplicate))
}

const fn notified<T>(data: T) -> Poll<WaitResult<T>> {
    Poll::Ready(Ok(data))
}

/// A map of [`Waker`]s associated with keys, allowing tasks to be woken by
/// their key.
///
/// A `WaitMap` allows any number of tasks to [wait] asynchronously and be
/// woken when a value with a certain key arrives. This can be used to
/// implement structures like "async mailboxes", where an async function
/// requests some data (such as a response) associated with a certain
/// key (such as a message ID). When the data is received, the key can
/// be used to provide the task with the desired data, as well as wake
/// the task for further processing.
///
/// # Overriding the blocking mutex
///
/// This type uses a [blocking `Mutex`](crate::blocking::Mutex) internally to
/// synchronize access to its wait list. By default, this is a [`DefaultMutex`]. To
/// use an alternative [`ScopedRawMutex`] implementation, use the
/// [`new_with_raw_mutex`](Self::new_with_raw_mutex) constructor. See [the documentation
/// on overriding mutex
/// implementations](crate::blocking#overriding-mutex-implementations) for more
/// details.
///
/// # Examples
///
/// Waking a single task at a time by calling [`wake`][wake]:
///
/// ```ignore
/// use std::sync::Arc;
/// use maitake::scheduler;
/// use maitake_sync::wait_map::{WaitMap, WakeOutcome};
///
/// const TASKS: usize = 10;
///
/// // In order to spawn tasks, we need a `Scheduler` instance.
/// let scheduler = Scheduler::new();
///
/// // Construct a new `WaitMap`.
/// let q = Arc::new(WaitMap::new());
///
/// // Spawn some tasks that will wait on the queue.
/// // We'll use the task index (0..10) as the key.
/// for i in 0..TASKS {
///     let q = q.clone();
///     scheduler.spawn(async move {
///         let val = q.wait(i).await.unwrap();
///         assert_eq!(val, i + 100);
///     });
/// }
///
/// // Tick the scheduler once.
/// let tick = scheduler.tick();
///
/// // No tasks should complete on this tick, as they are all waiting
/// // to be woken by the queue.
/// assert_eq!(tick.completed, 0, "no tasks have been woken");
///
/// // We now wake each of the tasks, using the same key (0..10),
/// // and provide them with a value that is their `key + 100`,
/// // e.g. 100..110. Only the task that has been woken will be
/// // notified.
/// for i in 0..TASKS {
///     let result = q.wake(&i, i + 100);
///     assert!(matches!(result, WakeOutcome::Woke));
///
///     // Tick the scheduler.
///     let tick = scheduler.tick();
///
///     // Exactly one task should have completed
///     assert_eq!(tick.completed, 1);
/// }
///
/// // Tick the scheduler.
/// let tick = scheduler.tick();
///
/// // No additional tasks should be completed
/// assert_eq!(tick.completed, 0);
/// assert!(!tick.has_remaining);
/// ```
///
/// # Implementation Notes
///
/// This type is currently implemented using [intrusive doubly-linked
/// list][ilist].
///
/// The *[intrusive]* aspect of this map is important, as it means that it does
/// not allocate memory. Instead, nodes in the linked list are stored in the
/// futures of tasks trying to wait for capacity. This means that it is not
/// necessary to allocate any heap memory for each task waiting to be woken.
///
/// However, the intrusive linked list introduces one new danger: because
/// futures can be *cancelled*, and the linked list nodes live within the
/// futures trying to wait on the queue, we *must* ensure that the node
/// is unlinked from the list before dropping a cancelled future. Failure to do
/// so would result in the list containing dangling pointers. Therefore, we must
/// use a *doubly-linked* list, so that nodes can edit both the previous and
/// next node when they have to remove themselves. This is kind of a bummer, as
/// it means we can't use something nice like this [intrusive queue by Dmitry
/// Vyukov][2], and there are not really practical designs for lock-free
/// doubly-linked lists that don't rely on some kind of deferred reclamation
/// scheme such as hazard pointers or QSBR.
///
/// Instead, we just stick a [`Mutex`] around the linked list, which must be
/// acquired to pop nodes from it, or for nodes to remove themselves when
/// futures are cancelled. This is a bit sad, but the critical sections for this
/// mutex are short enough that we still get pretty good performance despite it.
///
/// [`Waker`]: core::task::Waker
/// [wait]: WaitMap::wait
/// [wake]: WaitMap::wake
/// [`UnsafeCell`]: core::cell::UnsafeCell
/// [ilist]: cordyceps::List
/// [intrusive]: https://fuchsia.dev/fuchsia-src/development/languages/c-cpp/fbl_containers_guide/introduction
/// [2]: https://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue
pub struct WaitMap<K: PartialEq, V, Lock: ScopedRawMutex = DefaultMutex> {
    /// The wait queue's state variable.
    state: CachePadded<AtomicUsize>,

    /// The linked list of waiters.
    ///
    /// # Safety
    ///
    /// This is protected by a mutex; the mutex *must* be acquired when
    /// manipulating the linked list, OR when manipulating waiter nodes that may
    /// be linked into the list. If a node is known to not be linked, it is safe
    /// to modify that node (such as by waking the stored [`Waker`]) without
    /// holding the lock; otherwise, it may be modified through the list, so the
    /// lock must be held when modifying the
    /// node.
    ///
    /// A spinlock (from `mycelium_util`) is used here, in order to support
    /// `no_std` platforms; when running `loom` tests, a `loom` mutex is used
    /// instead to simulate the spinlock, because loom doesn't play nice with
    /// real spinlocks.
    queue: Mutex<List<Waiter<K, V>>, Lock>,
}

impl<K, V, Lock> Debug for WaitMap<K, V, Lock>
where
    K: PartialEq,
    Lock: ScopedRawMutex,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WaitMap")
            .field("state", &self.state)
            .field("queue", &self.queue)
            .finish()
    }
}

/// Future returned from [`WaitMap::wait()`].
///
/// This future is fused, so once it has completed, any future calls to poll
/// will immediately return [`Poll::Ready`].
///
/// # Notes
///
/// This future is `!Unpin`, as it is unsafe to [`core::mem::forget`] a
/// `Wait` future once it has been polled. For instance, the following code
/// must not compile:
///
///```compile_fail
/// use maitake_sync::wait_map::Wait;
///
/// // Calls to this function should only compile if `T` is `Unpin`.
/// fn assert_unpin<T: Unpin>() {}
///
/// assert_unpin::<Wait<'_, usize, ()>>();
/// ```
#[derive(Debug)]
#[pin_project(PinnedDrop)]
#[must_use = "futures do nothing unless `.await`ed or `poll`ed"]
pub struct Wait<'a, K: PartialEq, V, Lock: ScopedRawMutex = DefaultMutex> {
    /// The [`WaitMap`] being waited on from.
    queue: &'a WaitMap<K, V, Lock>,

    /// Entry in the wait queue linked list.
    #[pin]
    waiter: Waiter<K, V>,
}

impl<'map, 'wait, K: PartialEq, V, Lock: ScopedRawMutex> Wait<'map, K, V, Lock> {
    /// Returns a future that completes when the `Wait` item has been
    /// added to the [`WaitMap`], and is ready to receive data
    ///
    /// This is useful for ensuring that a receiver is ready before
    /// sending a message that will elicit the expected response.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use std::pin::pin;
    /// use maitake::scheduler;
    /// use maitake_sync::wait_map::{WaitMap, WakeOutcome};
    ///
    /// let scheduler = Scheduler::new();
    /// let q = Arc::new(WaitMap::new());
    ///
    /// let q2 = q.clone();
    /// scheduler.spawn(async move {
    ///     let mut wait = pin!(q2.wait(0));
    ///
    ///     // At this point, we have created the future, but it has not yet
    ///     // been added to the queue. We could immediately await 'wait',
    ///     // but then we would be unable to progress further. We must
    ///     // first pin the `wait` future, to ensure that it does not move
    ///     // until it has been completed.
    ///     wait.as_mut().subscribe().await.unwrap();
    ///
    ///     // We now know the waiter has been enqueued, at this point we could
    ///     // send a message that will cause key == 0 to be returned, without
    ///     // worrying about racing with the expected response, e.g:
    ///     //
    ///     // sender.send_with_id(0, SomeMessage).await?;
    ///     //
    ///     let val = wait.await.unwrap();
    ///     assert_eq!(val, 10);
    /// });
    ///
    /// assert!(matches!(q.wake(&0, 100), WakeOutcome::NoMatch(_)));
    ///
    /// let tick = scheduler.tick();
    ///
    /// assert!(matches!(q.wake(&0, 100), WakeOutcome::Woke));
    /// ```
    pub fn subscribe(self: Pin<&'wait mut Self>) -> Subscribe<'wait, 'map, K, V, Lock> {
        Subscribe { wait: self }
    }

    /// Deprecated alias for [`Wait::subscribe`]. See that method for details.
    #[deprecated(
        since = "0.1.3",
        note = "renamed to `subscribe` for consistency, use that instead"
    )]
    #[allow(deprecated)] // let us use the deprecated type alias
    pub fn enqueue(self: Pin<&'wait mut Self>) -> EnqueueWait<'wait, 'map, K, V, Lock> {
        self.subscribe()
    }
}

/// A waiter node which may be linked into a wait queue.
#[pin_project]
struct Waiter<K: PartialEq, V> {
    /// The intrusive linked list node.
    #[pin]
    node: UnsafeCell<Node<K, V>>,

    /// The future's state.
    state: WaitState,

    key: K,
}

impl<K: PartialEq, V> Debug for Waiter<K, V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Waiter")
            .field("node", &self.node)
            .field("state", &self.state)
            .field("key", &fmt::display(core::any::type_name::<K>()))
            .field("val", &fmt::display(core::any::type_name::<V>()))
            .finish()
    }
}

#[repr(C)]
struct Node<K: PartialEq, V> {
    /// Intrusive linked list pointers.
    ///
    /// # Safety
    ///
    /// This *must* be the first field in the struct in order for the `Linked`
    /// impl to be sound.
    links: list::Links<Waiter<K, V>>,

    /// The node's waker, if it has yet to be woken, or the data assigned to the
    /// node, if it has been woken.
    waker: Wakeup<V>,

    // This type is !Unpin due to the heuristic from:
    // <https://github.com/rust-lang/rust/pull/82834>
    _pin: PhantomPinned,
}

impl<K: PartialEq, V> Debug for Node<K, V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Node")
            .field("links", &self.links)
            .field("waker", &self.waker)
            .finish()
    }
}

enum_from_bits! {
    /// The state of a [`Waiter`] node in a [`WaitMap`].
    #[derive(Debug, Eq, PartialEq)]
    enum WaitState<u8> {
        /// The waiter has not yet been enqueued.
        ///
        /// When in this state, the node is **not** part of the linked list, and
        /// can be dropped without removing it from the list.
        Start = 0b01,

        /// The waiter is waiting.
        ///
        /// When in this state, the node **is** part of the linked list. If the
        /// node is dropped in this state, it **must** be removed from the list
        /// before dropping it. Failure to ensure this will result in dangling
        /// pointers in the linked list!
        Waiting = 0b10,

        /// The waiter has been woken.
        ///
        /// When in this state, the node is **not** part of the linked list, and
        /// can be dropped without removing it from the list.
        Completed = 0b11,
    }
}

/// The queue's current state.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
enum State {
    /// No waiters are queued, and there is no pending notification.
    /// Waiting while the queue is in this state will enqueue the waiter
    Empty = 0b00,

    /// There are one or more waiters in the queue. Waiting while
    /// the queue is in this state will not transition the state. Waking while
    /// in this state will wake the appropriate waiter in the queue; if this empties
    /// the queue, then the queue will transition to [`State::Empty`].
    Waiting = 0b01,

    // TODO(AJM): We have a state gap here. Is this okay?
    /// The queue is closed. Waiting while in this state will return
    /// [`Closed`] without transitioning the queue's state.
    ///
    /// *Note*: This *must* correspond to all state bits being set, as it's set
    /// via a [`fetch_or`].
    ///
    /// [`Closed`]: crate::Closed
    /// [`fetch_or`]: core::sync::atomic::AtomicUsize::fetch_or
    Closed = 0b11,
}

#[derive(Clone)]
enum Wakeup<V> {
    /// The Waiter has been created, but no wake has occurred. This should
    /// be the ONLY state while in `WaitState::Start`
    Empty,

    /// The Waiter has moved to the `WaitState::Waiting` state. We now
    /// have the relevant waker, and are still waiting for data. This
    /// corresponds to `WaitState::Waiting`.
    Waiting(Waker),

    /// The Waiter has received data, and is waiting for the woken task
    /// to notice, and take the data by polling+completing the future.
    /// This corresponds to `WaitState::Completed`.
    ///
    /// This state stores the received value; taking the value out of the waiter
    /// advances the state to `Retrieved`.
    DataReceived(V),

    /// The waiter has received data, and already given it away, and has
    /// no more data to give. This corresponds to `WaitState::Completed`.
    Retreived,

    /// The Queue the waiter is part of has been closed. No data will
    /// be received from this future. This corresponds to
    /// `WaitState::Completed`.
    Closed,
}

// === impl WaitMap ===

impl<K: PartialEq, V> WaitMap<K, V> {
    loom_const_fn! {
        /// Returns a new `WaitMap`.
        ///
        /// This constructor returns a `WaitMap` that uses a [`DefaultMutex`] as
        /// the [`ScopedRawMutex`] implementation for wait list synchronization.
        /// To use a different [`ScopedRawMutex`] implementation, use the
        /// [`new_with_raw_mutex`](Self::new_with_raw_mutex) constructor, instead. See
        /// [the documentation on overriding mutex
        /// implementations](crate::blocking#overriding-mutex-implementations)
        /// for more details.
        #[must_use]
        pub fn new() -> Self {
            Self::new_with_raw_mutex(DefaultMutex::new())
        }
    }
}

impl<K, V, Lock> Default for WaitMap<K, V, Lock>
where
    K: PartialEq,
    Lock: ScopedRawMutex + Default,
{
    fn default() -> Self {
        Self::new_with_raw_mutex(Lock::default())
    }
}

impl<K, V, Lock> WaitMap<K, V, Lock>
where
    K: PartialEq,
    Lock: ScopedRawMutex,
{
    loom_const_fn! {
        /// Returns a new `WaitMap`, using the provided [`ScopedRawMutex`]
        /// implementation for wait-list synchronization.
        ///
        /// This constructor allows a `WaitMap` to be constructed with any type that
        /// implements [`ScopedRawMutex`] as the underlying raw blocking mutex
        /// implementation. See [the documentation on overriding mutex
        /// implementations](crate::blocking#overriding-mutex-implementations)
        /// for more details.
        #[must_use]
        pub fn new_with_raw_mutex(lock: Lock) -> Self {
            Self {
                state: CachePadded::new(AtomicUsize::new(State::Empty.into_usize())),
                queue: Mutex::new_with_raw_mutex(List::new(), lock),
            }
        }
    }
}

impl<K: PartialEq, V, Lock: ScopedRawMutex> WaitMap<K, V, Lock> {
    /// Wake a certain task in the queue.
    ///
    /// If the queue is empty, a wakeup is stored in the `WaitMap`, and the
    /// next call to [`wait`] will complete immediately.
    ///
    /// [`wait`]: WaitMap::wait
    #[inline]
    pub fn wake(&self, key: &K, val: V) -> WakeOutcome<V> {
        // snapshot the queue's current state.
        let mut state = self.load();

        // check if any tasks are currently waiting on this queue. if there are
        // no waiting tasks, store the wakeup to be consumed by the next call to
        // `wait`.
        match state {
            // Something is waiting!
            State::Waiting => {}

            // if the queue is closed, bail.
            State::Closed => return WakeOutcome::Closed(val),

            // if the queue is empty, bail.
            State::Empty => return WakeOutcome::NoMatch(val),
        }

        // okay, there are tasks waiting on the queue; we must acquire the lock
        // on the linked list and wake the next task from the queue.
        let mut val = Some(val);
        let maybe_waker = self.queue.with_lock(|queue| {
            test_debug!("wake: -> locked");

            // the queue's state may have changed while we were waiting to acquire
            // the lock, so we need to acquire a new snapshot.
            state = self.load();

            let node = self.node_match_locked(key, &mut *queue, state)?;
            // if there's a node, give it the value and take the waker and
            // return it. we return the waker from this closure rather than
            // waking it, because we need to release the lock before waking the
            // task.
            let val = val
                .take()
                .expect("value is only taken elsewhere if there is no waker, but there is one");
            let waker = Waiter::<K, V>::wake(node, &mut *queue, Wakeup::DataReceived(val));
            Some(waker)
        });

        if let Some(waker) = maybe_waker {
            waker.wake();
            WakeOutcome::Woke
        } else {
            let val =
                val.expect("value is only taken elsewhere if there is a waker, and there isn't");
            WakeOutcome::NoMatch(val)
        }
    }

    /// Returns `true` if this `WaitMap` is [closed](Self::close).
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.load() == State::Closed
    }

    /// Close the queue, indicating that it may no longer be used.
    ///
    /// Once a queue is closed, all [`wait`] calls (current or future) will
    /// return an error.
    ///
    /// This method is generally used when implementing higher-level
    /// synchronization primitives or resources: when an event makes a resource
    /// permanently unavailable, the queue can be closed.
    ///
    /// [`wait`]: Self::wait
    pub fn close(&self) {
        let state = self.state.fetch_or(State::Closed.into_usize(), SeqCst);
        let state = test_dbg!(State::from_bits(state));
        if state != State::Waiting {
            return;
        }

        let mut batch = WakeBatch::new();
        let mut waiters_remaining = true;
        while waiters_remaining {
            waiters_remaining = self.queue.with_lock(|waiters| {
                while let Some(node) = waiters.pop_back() {
                    let waker = Waiter::wake(node, waiters, Wakeup::Closed);
                    if !batch.add_waker(waker) {
                        // there's still room in the wake set, just keep adding to it.
                        return true;
                    }
                }
                false
            });
            batch.wake_all();
        }
    }

    /// Wait to be woken up by this queue.
    ///
    /// This returns a [`Wait`] future that will complete when the task is
    /// woken by a call to [`wake`] with a matching `key`, or when the `WaitMap`
    /// is dropped.
    ///
    /// **Note**: `key`s must be unique. If the given key already exists in the
    /// `WaitMap`, the future will resolve to an Error the first time it is polled
    ///
    /// [`wake`]: Self::wake
    pub fn wait(&self, key: K) -> Wait<'_, K, V, Lock> {
        Wait {
            queue: self,
            waiter: self.waiter(key),
        }
    }

    /// Returns a [`Waiter`] entry in this queue.
    ///
    /// This is factored out into a separate function because it's used by both
    /// [`WaitMap::wait`] and [`WaitMap::wait_owned`].
    fn waiter(&self, key: K) -> Waiter<K, V> {
        let state = WaitState::Start;
        Waiter {
            state,
            node: UnsafeCell::new(Node {
                links: list::Links::new(),
                waker: Wakeup::Empty,
                _pin: PhantomPinned,
            }),
            key,
        }
    }

    #[cfg_attr(test, track_caller)]
    fn load(&self) -> State {
        #[allow(clippy::let_and_return)]
        let state = State::from_bits(self.state.load(SeqCst));
        test_debug!("state.load() = {state:?}");
        state
    }

    #[cfg_attr(test, track_caller)]
    fn store(&self, state: State) {
        test_debug!("state.store({state:?}");
        self.state.store(state as usize, SeqCst);
    }

    #[cfg_attr(test, track_caller)]
    fn compare_exchange(&self, current: State, new: State) -> Result<State, State> {
        #[allow(clippy::let_and_return)]
        let res = self
            .state
            .compare_exchange(current as usize, new as usize, SeqCst, SeqCst)
            .map(State::from_bits)
            .map_err(State::from_bits);
        test_debug!("state.compare_exchange({current:?}, {new:?}) = {res:?}");
        res
    }

    #[cold]
    #[inline(never)]
    fn node_match_locked(
        &self,
        key: &K,
        queue: &mut List<Waiter<K, V>>,
        curr: State,
    ) -> Option<NonNull<Waiter<K, V>>> {
        let state = curr;

        // is the queue still in the `Waiting` state? it is possible that we
        // transitioned to a different state while locking the queue.
        if test_dbg!(state) != State::Waiting {
            // If we are not waiting, we are either empty or closed.
            // Not much to do.
            return None;
        }

        let mut cursor = queue.cursor_front_mut();
        let opt_node = cursor.remove_first(|t| &t.key == key);

        // if we took the final waiter currently in the queue, transition to the
        // `Empty` state.
        if test_dbg!(queue.is_empty()) {
            self.store(State::Empty);
        }

        opt_node
    }
}

/// The result of an attempted [`WaitMap::wake()`] operation.
#[derive(Debug)]
pub enum WakeOutcome<V> {
    /// The task was successfully woken, and the data was provided.
    Woke,

    /// No task matching the given key was found in the queue.
    NoMatch(V),

    /// The queue was already closed when the wake was attempted,
    /// and the data was not provided to any task.
    Closed(V),
}

// === impl WaitError ===

impl fmt::Display for WaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.pad("WaitMap closed"),
            Self::Duplicate => f.pad("duplicate key"),
            &Self::AlreadyConsumed => f.pad("received data has already been consumed"),
            Self::NeverAdded => f.pad("Wait was never added to WaitMap"),
        }
    }
}

feature! {
    #![feature = "core-error"]
    impl core::error::Error for WaitError {}
}

// === impl Waiter ===

/// A future that ensures a [`Wait`] has been added to a [`WaitMap`].
///
/// See [`Wait::subscribe`] for more information and usage example.
#[must_use = "futures do nothing unless `.await`ed or `poll`ed"]
#[derive(Debug)]
pub struct Subscribe<'a, 'b, K, V, Lock = DefaultMutex>
where
    K: PartialEq,
    Lock: ScopedRawMutex,
{
    wait: Pin<&'a mut Wait<'b, K, V, Lock>>,
}

impl<K, V, Lock> Future for Subscribe<'_, '_, K, V, Lock>
where
    K: PartialEq,
    Lock: ScopedRawMutex,
{
    type Output = WaitResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.wait.as_mut().project();
        if let WaitState::Start = test_dbg!(&this.waiter.state) {
            this.waiter.start_to_wait(this.queue, cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

/// Deprecated alias for [`Subscribe`]. See the [`Wait::subscribe`]
/// documentation for more details.
#[deprecated(
    since = "0.1.3",
    note = "renamed to `Subscribe` for consistency, use that instead"
)]
pub type EnqueueWait<'a, 'b, K, V, Lock> = Subscribe<'a, 'b, K, V, Lock>;

impl<K: PartialEq, V> Waiter<K, V> {
    /// Wake the task that owns this `Waiter`.
    ///
    /// # Safety
    ///
    /// This is only safe to call while the list is locked. The `list`
    /// parameter ensures this method is only called while holding the lock, so
    /// this can be safe.
    ///
    /// Of course, that must be the *same* list that this waiter is a member of,
    /// and currently, there is no way to ensure that...
    #[inline(always)]
    #[cfg_attr(loom, track_caller)]
    fn wake(this: NonNull<Self>, list: &mut List<Self>, wakeup: Wakeup<V>) -> Waker {
        Waiter::with_node(this, list, |node| {
            let waker = test_dbg!(mem::replace(&mut node.waker, wakeup));
            match waker {
                Wakeup::Waiting(waker) => waker,
                _ => unreachable!("tried to wake a waiter in the {:?} state!", waker),
            }
        })
    }

    /// # Safety
    ///
    /// This is only safe to call while the list is locked. The dummy `_list`
    /// parameter ensures this method is only called while holding the lock, so
    /// this can be safe.
    ///
    /// Of course, that must be the *same* list that this waiter is a member of,
    /// and currently, there is no way to ensure that...
    #[inline(always)]
    #[cfg_attr(loom, track_caller)]
    fn with_node<T>(
        mut this: NonNull<Self>,
        _list: &mut List<Self>,
        f: impl FnOnce(&mut Node<K, V>) -> T,
    ) -> T {
        unsafe {
            // safety: this is only called while holding the lock on the queue,
            // so it's safe to mutate the waiter.
            this.as_mut().node.with_mut(|node| f(&mut *node))
        }
    }

    /// Moves a `Wait` from the `Start` condition.
    ///
    /// Caller MUST ensure the `Wait` is in the start condition before calling.
    fn start_to_wait<Lock>(
        mut self: Pin<&mut Self>,
        queue: &WaitMap<K, V, Lock>,
        cx: &mut Context<'_>,
    ) -> Poll<WaitResult<()>>
    where
        Lock: ScopedRawMutex,
    {
        // Try to wait...
        test_debug!("poll_wait: locking...");
        queue.queue.with_lock(move |waiters| {
            test_debug!("poll_wait: -> locked");
            let mut this = self.as_mut().project();

            debug_assert!(
                matches!(this.state, WaitState::Start),
                "start_to_wait should ONLY be called from the Start state!"
            );

            let mut queue_state = queue.load();

            // transition the queue to the waiting state
            'to_waiting: loop {
                match test_dbg!(queue_state) {
                    // the queue is `Empty`, transition to `Waiting`
                    State::Empty => match queue.compare_exchange(queue_state, State::Waiting) {
                        Ok(_) => break 'to_waiting,
                        Err(actual) => queue_state = actual,
                    },
                    // the queue is already `Waiting`
                    State::Waiting => break 'to_waiting,
                    State::Closed => return closed(),
                }
            }

            // Check if key already exists
            //
            // Note: It's okay not to re-update the state here, if we were empty
            // this check will never trigger, if we are already waiting, we should
            // still be waiting.
            let mut cursor = waiters.cursor_front_mut();
            if cursor.any(|n| &n.key == this.key) {
                return duplicate();
            }

            // enqueue the node
            *this.state = WaitState::Waiting;
            this.node.as_mut().with_mut(|node| {
                unsafe {
                    // safety: we may mutate the node because we are
                    // holding the lock.
                    (*node).waker = Wakeup::Waiting(cx.waker().clone());
                }
            });
            let ptr = unsafe { NonNull::from(Pin::into_inner_unchecked(self)) };
            waiters.push_front(ptr);

            Poll::Ready(Ok(()))
        })
    }

    fn poll_wait<Lock>(
        mut self: Pin<&mut Self>,
        queue: &WaitMap<K, V, Lock>,
        cx: &mut Context<'_>,
    ) -> Poll<WaitResult<V>>
    where
        Lock: ScopedRawMutex,
    {
        test_debug!(ptr = ?fmt::ptr(self.as_mut()), "Waiter::poll_wait");
        let this = self.as_mut().project();

        match test_dbg!(&this.state) {
            WaitState::Start => {
                let _ = self.start_to_wait(queue, cx)?;
                Poll::Pending
            }
            WaitState::Waiting => {
                // We must lock the linked list in order to safely mutate our node in
                // the list. We don't actually need the mutable reference to the
                // queue here, though.
                queue.queue.with_lock(|_waiters| {
                    this.node.with_mut(|node| unsafe {
                        // safety: we may mutate the node because we are
                        // holding the lock.
                        let node = &mut *node;
                        let result;
                        node.waker = match mem::replace(&mut node.waker, Wakeup::Empty) {
                            // We already had a waker, but are now getting another one.
                            // Store the new one, droping the old one
                            Wakeup::Waiting(waker) => {
                                result = Poll::Pending;
                                if !waker.will_wake(cx.waker()) {
                                    Wakeup::Waiting(cx.waker().clone())
                                } else {
                                    Wakeup::Waiting(waker)
                                }
                            }
                            // We have received the data, take the data out of the
                            // future, and provide it to the poller
                            Wakeup::DataReceived(val) => {
                                result = notified(val);
                                Wakeup::Retreived
                            }
                            Wakeup::Retreived => {
                                result = consumed();
                                Wakeup::Retreived
                            }

                            Wakeup::Closed => {
                                *this.state = WaitState::Completed;
                                result = closed();
                                Wakeup::Closed
                            }
                            Wakeup::Empty => {
                                result = never_added();
                                Wakeup::Closed
                            }
                        };
                        result
                    })
                })
            }
            WaitState::Completed => consumed(),
        }
    }

    /// Release this `Waiter` from the queue.
    ///
    /// This is called from the `drop` implementation for the [`Wait`] and
    /// [`WaitOwned`] futures.
    fn release<Lock>(mut self: Pin<&mut Self>, queue: &WaitMap<K, V, Lock>)
    where
        Lock: ScopedRawMutex,
    {
        let state = *(self.as_mut().project().state);
        let ptr = NonNull::from(unsafe { Pin::into_inner_unchecked(self) });
        test_debug!(self = ?fmt::ptr(ptr), ?state, ?queue, "Waiter::release");

        // if we're not enqueued, we don't have to do anything else.
        if state != WaitState::Waiting {
            return;
        }

        queue.queue.with_lock(|waiters| {
            let state = queue.load();

            // remove the node
            unsafe {
                // safety: we have the lock on the queue, so this is safe.
                waiters.remove(ptr);
            };

            // if we removed the last waiter from the queue, transition the state to
            // `Empty`.
            if test_dbg!(waiters.is_empty()) && state == State::Waiting {
                queue.store(State::Empty);
            }
        })
    }
}

unsafe impl<K: PartialEq, V> Linked<list::Links<Waiter<K, V>>> for Waiter<K, V> {
    type Handle = NonNull<Waiter<K, V>>;

    fn into_ptr(r: Self::Handle) -> NonNull<Self> {
        r
    }

    unsafe fn from_ptr(ptr: NonNull<Self>) -> Self::Handle {
        ptr
    }

    unsafe fn links(target: NonNull<Self>) -> NonNull<list::Links<Waiter<K, V>>> {
        // Safety: using `ptr::addr_of!` avoids creating a temporary
        // reference, which stacked borrows dislikes.
        let node = ptr::addr_of!((*target.as_ptr()).node);
        (*node).with_mut(|node| {
            let links = ptr::addr_of_mut!((*node).links);
            // Safety: since the `target` pointer is `NonNull`, we can assume
            // that pointers to its members are also not null, making this use
            // of `new_unchecked` fine.
            NonNull::new_unchecked(links)
        })
    }
}

// === impl Wait ===

impl<K: PartialEq, V> Future for Wait<'_, K, V> {
    type Output = WaitResult<V>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.waiter.poll_wait(this.queue, cx)
    }
}

#[pinned_drop]
impl<K, V, Lock> PinnedDrop for Wait<'_, K, V, Lock>
where
    K: PartialEq,
    Lock: ScopedRawMutex,
{
    fn drop(mut self: Pin<&mut Self>) {
        let this = self.project();
        this.waiter.release(this.queue);
    }
}

// === impl MapState ===

impl State {
    #[inline]
    fn from_bits(bits: usize) -> Self {
        Self::try_from_bits(bits).expect("This shouldn't be possible")
    }
}

impl FromBits<usize> for State {
    const BITS: u32 = 2;
    type Error = core::convert::Infallible;

    fn try_from_bits(bits: usize) -> Result<Self, Self::Error> {
        Ok(match bits as u8 {
            bits if bits == Self::Empty as u8 => Self::Empty,
            bits if bits == Self::Waiting as u8 => Self::Waiting,
            bits if bits == Self::Closed as u8 => Self::Closed,
            _ => unsafe {
                // TODO(AJM): this isn't *totally* true anymore...
                unreachable_unchecked!("all potential 2-bit patterns should be covered!")
            },
        })
    }

    fn into_bits(self) -> usize {
        self.into_usize()
    }
}

impl State {
    const fn into_usize(self) -> usize {
        self as u8 as usize
    }
}

// === impl WaitOwned ===

feature! {
    #![feature = "alloc"]

    use alloc::sync::Arc;

    /// Future returned from [`WaitMap::wait_owned()`].
    ///
    /// This is identical to the [`Wait`] future, except that it takes an
    /// [`Arc`] reference to the [`WaitMap`], allowing the returned future to
    /// live for the `'static` lifetime.
    ///
    /// This future is fused, so once it has completed, any future calls to poll
    /// will immediately return [`Poll::Ready`].
    ///
    /// # Notes
    ///
    /// This future is `!Unpin`, as it is unsafe to [`core::mem::forget`] a
    /// `Wait` future once it has been polled. For instance, the following code
    /// must not compile:
    ///
    ///```compile_fail
    /// use maitake_sync::wait_map::WaitOwned;
    ///
    /// // Calls to this function should only compile if `T` is `Unpin`.
    /// fn assert_unpin<T: Unpin>() {}
    ///
    /// assert_unpin::<WaitOwned<'_, usize, ()>>();
    #[derive(Debug)]
    #[pin_project(PinnedDrop)]
    pub struct WaitOwned<K: PartialEq, V, Lock: ScopedRawMutex = DefaultMutex> {
        /// The `WaitMap` being waited on.
        queue: Arc<WaitMap<K, V, Lock>>,

        /// Entry in the wait queue.
        #[pin]
        waiter: Waiter<K, V>,
    }

    impl<K: PartialEq, V, Lock: ScopedRawMutex> WaitMap<K, V, Lock> {
        /// Wait to be woken up by this queue, returning a future that's valid
        /// for the `'static` lifetime.
        ///
        /// This is identical to the [`wait`] method, except that it takes a
        /// [`Arc`] reference to the [`WaitMap`], allowing the returned future to
        /// live for the `'static` lifetime.
        ///
        /// This returns a [`WaitOwned`] future that will complete when the task is
        /// woken by a call to [`wake`] with a matching `key`, or when the `WaitMap`
        /// is dropped.
        ///
        /// **Note**: `key`s must be unique. If the given key already exists in the
        /// `WaitMap`, the future will resolve to an Error the first time it is polled
        ///
        /// [`wake`]: Self::wake
        /// [`wait`]: Self::wait
        pub fn wait_owned(self: &Arc<Self>, key: K) -> WaitOwned<K, V, Lock> {
            let waiter = self.waiter(key);
            let queue = self.clone();
            WaitOwned { queue, waiter }
        }
    }

    impl<K: PartialEq, V, Lock: ScopedRawMutex> Future for WaitOwned<K, V, Lock> {
        type Output = WaitResult<V>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.project();
            this.waiter.poll_wait(&*this.queue, cx)
        }
    }

    #[pinned_drop]
    impl<K, V, Lock> PinnedDrop for WaitOwned<K, V, Lock>
    where
        K: PartialEq,
        Lock: ScopedRawMutex,
    {
        fn drop(mut self: Pin<&mut Self>) {
            let this = self.project();
            this.waiter.release(&*this.queue);
        }
    }
}

impl<V> fmt::Debug for Wakeup<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("Wakeup::Empty"),
            Self::Waiting(waker) => f.debug_tuple("Wakeup::Waiting").field(waker).finish(),
            Self::DataReceived(_) => f.write_str("Wakeup::DataReceived(..)"),
            Self::Retreived => f.write_str("Wakeup::Retrieved"),
            Self::Closed => f.write_str("Wakeup::Closed"),
        }
    }
}
