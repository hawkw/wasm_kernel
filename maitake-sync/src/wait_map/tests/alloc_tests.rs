use super::super::*;
use crate::loom::sync::Arc;
use core::{
    future,
    pin::pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use futures_util::{select_biased, FutureExt};
use tokio_test::{assert_pending, assert_ready, assert_ready_err, task};

#[test]
fn enqueue() {
    let _trace = crate::util::trace_init();
    static COMPLETED: AtomicUsize = AtomicUsize::new(0);
    static ENQUEUED: AtomicUsize = AtomicUsize::new(0);

    let q = Arc::new(WaitMap::new());

    // Create a waiter, but do not tick the scheduler yet
    let mut waiter1 = task::spawn({
        let q = q.clone();
        async move {
            let val = q.wait(0).await.unwrap();
            COMPLETED.fetch_add(1, Ordering::Relaxed);
            assert_eq!(val, 100);
        }
    });

    // Attempt to wake - but waiter is not enqueued yet
    assert!(matches!(q.wake(&0, 100), WakeOutcome::NoMatch(_)));
    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);
    assert_eq!(ENQUEUED.load(Ordering::Relaxed), 0);

    // Create a second waiter - this one that first checks for enqueued
    let mut waiter2 = task::spawn({
        let q = q.clone();
        async move {
            let mut wait = pin!(q.wait(1));
            wait.as_mut().subscribe().await.unwrap();
            ENQUEUED.fetch_add(1, Ordering::Relaxed);

            let val = wait.await.unwrap();
            COMPLETED.fetch_add(1, Ordering::Relaxed);
            assert_eq!(val, 101);
        }
    });

    // Attempt to wake - but waiter is not enqueued yet
    assert!(matches!(q.wake(&0, 100), WakeOutcome::NoMatch(_)));
    assert!(matches!(q.wake(&1, 101), WakeOutcome::NoMatch(_)));
    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);
    assert_eq!(ENQUEUED.load(Ordering::Relaxed), 0);

    // Tick once, we can see the second task moved into the enqueued state
    assert_pending!(waiter1.poll());
    assert_pending!(waiter2.poll());
    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);
    assert_eq!(ENQUEUED.load(Ordering::Relaxed), 1);

    assert!(matches!(q.wake(&0, 100), WakeOutcome::Woke));
    assert!(matches!(q.wake(&1, 101), WakeOutcome::Woke));
    assert!(waiter1.is_woken());
    assert!(waiter2.is_woken());

    assert_ready!(waiter1.poll());
    assert_ready!(waiter2.poll());
    assert_eq!(COMPLETED.load(Ordering::Relaxed), 2);
    assert_eq!(ENQUEUED.load(Ordering::Relaxed), 1);
}

#[test]
fn duplicate() {
    let _trace = crate::util::trace_init();
    static COMPLETED: AtomicUsize = AtomicUsize::new(0);
    static ENQUEUED: AtomicUsize = AtomicUsize::new(0);

    let q = Arc::new(WaitMap::new());

    // Create a waiter, but do not tick the scheduler yet
    let mut waiter1 = task::spawn({
        let q = q.clone();
        async move {
            let mut wait = pin!(q.wait(0));
            wait.as_mut().subscribe().await.unwrap();
            ENQUEUED.fetch_add(1, Ordering::Relaxed);

            let val = wait.await.unwrap();
            COMPLETED.fetch_add(1, Ordering::Relaxed);
            assert_eq!(val, 100);
        }
    });

    // Create a waiter, but do not tick the scheduler yet
    let mut waiter2 = task::spawn({
        let q = q.clone();
        async move {
            // Duplicate key!
            let mut wait = pin!(q.wait(0));
            wait.as_mut().subscribe().await
        }
    });

    // Tick once, we can see the second task moved into the enqueued state
    assert_pending!(waiter1.poll());
    let err = assert_ready_err!(waiter2.poll());
    assert!(matches!(err, WaitError::Duplicate));
    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);
    assert_eq!(ENQUEUED.load(Ordering::Relaxed), 1);

    assert!(matches!(q.wake(&0, 100), WakeOutcome::Woke));

    assert!(waiter1.is_woken());
    assert_ready!(waiter1.poll());

    assert_eq!(COMPLETED.load(Ordering::Relaxed), 1);
    assert_eq!(ENQUEUED.load(Ordering::Relaxed), 1);
}

#[test]
fn close() {
    let _trace = crate::util::trace_init();
    static COMPLETED: AtomicUsize = AtomicUsize::new(0);

    let q: Arc<WaitMap<usize, ()>> = Arc::new(WaitMap::new());

    const TASKS: usize = 10;

    let mut tasks = (0..TASKS)
        .map(|i| {
            let wait = q.wait_owned(i);
            task::spawn(async move {
                wait.await.expect_err("dropping the queue must close it");
                COMPLETED.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect::<Vec<_>>();

    for task in &mut tasks {
        assert_pending!(task.poll());
    }

    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);

    q.close();

    for task in &mut tasks {
        assert_ready!(task.poll());
    }

    assert_eq!(COMPLETED.load(Ordering::Relaxed), TASKS);
}

#[test]
fn wake_one() {
    let _trace = crate::util::trace_init();
    static COMPLETED: AtomicUsize = AtomicUsize::new(0);

    let q = Arc::new(WaitMap::new());

    const TASKS: usize = 10;

    let mut tasks = (0..TASKS)
        .map(|i| {
            let q = q.clone();
            task::spawn(async move {
                let val = q.wait(i).await.unwrap();
                COMPLETED.fetch_add(1, Ordering::Relaxed);

                assert_eq!(val, 100 + i);

                if i < (TASKS - 1) {
                    assert!(matches!(q.wake(&(i + 1), 100 + i + 1), WakeOutcome::Woke));
                } else {
                    assert!(matches!(
                        q.wake(&(i + 1), 100 + i + 1),
                        WakeOutcome::NoMatch(_)
                    ));
                }
            })
        })
        .collect::<Vec<_>>();

    for task in &mut tasks {
        assert_pending!(task.poll());
    }
    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);

    q.wake(&0, 100);

    for task in &mut tasks {
        assert!(task.is_woken());
        assert_ready!(task.poll());
    }

    assert_eq!(COMPLETED.load(Ordering::Relaxed), TASKS);
}

#[derive(Debug)]
struct CountDropKey {
    idx: usize,
    cnt: &'static AtomicUsize,
}

impl PartialEq for CountDropKey {
    fn eq(&self, other: &Self) -> bool {
        self.idx == other.idx
    }
}

impl Drop for CountDropKey {
    fn drop(&mut self) {
        self.cnt.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct CountDropVal {
    cnt: &'static AtomicUsize,
}

impl Drop for CountDropVal {
    fn drop(&mut self) {
        self.cnt.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn drop_no_wake() {
    let _trace = crate::util::trace_init();
    static COMPLETED: AtomicUsize = AtomicUsize::new(0);
    static KEY_DROPS: AtomicUsize = AtomicUsize::new(0);
    static VAL_DROPS: AtomicUsize = AtomicUsize::new(0);

    let q = Arc::new(WaitMap::<CountDropKey, CountDropVal>::new());

    const TASKS: usize = 10;

    let mut tasks = (0..TASKS)
        .map(|i| {
            let q = q.clone();
            task::spawn(async move {
                q.wait(CountDropKey {
                    idx: i,
                    cnt: &KEY_DROPS,
                })
                .await
                .unwrap_err();
                COMPLETED.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect::<Vec<_>>();

    for task in &mut tasks {
        assert_pending!(task.poll());
    }
    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);
    assert_eq!(KEY_DROPS.load(Ordering::Relaxed), 0);
    assert_eq!(VAL_DROPS.load(Ordering::Relaxed), 0);

    q.close();

    for task in &mut tasks {
        assert!(task.is_woken());
        assert_ready!(task.poll());
    }
    assert_eq!(COMPLETED.load(Ordering::Relaxed), TASKS);
    assert_eq!(KEY_DROPS.load(Ordering::Relaxed), TASKS);
    assert_eq!(VAL_DROPS.load(Ordering::Relaxed), 0);
}

#[test]
fn drop_wake_completed() {
    let _trace = crate::util::trace_init();
    static COMPLETED: AtomicUsize = AtomicUsize::new(0);
    static KEY_DROPS: AtomicUsize = AtomicUsize::new(0);
    static VAL_DROPS: AtomicUsize = AtomicUsize::new(0);
    static DONT_CARE: AtomicUsize = AtomicUsize::new(0);
    let q = Arc::new(WaitMap::<CountDropKey, CountDropVal>::new());

    const TASKS: usize = 10;

    let mut tasks = (0..TASKS)
        .map(|i| {
            let q = q.clone();
            task::spawn(async move {
                q.wait(CountDropKey {
                    idx: i,
                    cnt: &KEY_DROPS,
                })
                .await
                .unwrap();
                COMPLETED.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect::<Vec<_>>();

    for task in &mut tasks {
        assert_pending!(task.poll());
    }

    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);
    assert_eq!(KEY_DROPS.load(Ordering::Relaxed), 0);
    assert_eq!(VAL_DROPS.load(Ordering::Relaxed), 0);

    for i in 0..TASKS {
        q.wake(
            &CountDropKey {
                idx: i,
                cnt: &DONT_CARE,
            },
            CountDropVal { cnt: &VAL_DROPS },
        );
    }

    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);
    assert_eq!(KEY_DROPS.load(Ordering::Relaxed), 0);
    assert_eq!(VAL_DROPS.load(Ordering::Relaxed), 0);

    for task in &mut tasks {
        assert!(task.is_woken());
        assert_ready!(task.poll());
    }

    assert_eq!(COMPLETED.load(Ordering::Relaxed), TASKS);
    assert_eq!(KEY_DROPS.load(Ordering::Relaxed), TASKS);
    assert_eq!(VAL_DROPS.load(Ordering::Relaxed), TASKS);
}

#[test]
fn drop_wake_bailed() {
    let _trace = crate::util::trace_init();
    static COMPLETED: AtomicUsize = AtomicUsize::new(0);
    static KEY_DROPS: AtomicUsize = AtomicUsize::new(0);
    static VAL_DROPS: AtomicUsize = AtomicUsize::new(0);
    static DONT_CARE: AtomicUsize = AtomicUsize::new(0);
    static BAIL: AtomicBool = AtomicBool::new(false);

    let q = Arc::new(WaitMap::<CountDropKey, CountDropVal>::new());

    const TASKS: usize = 10;

    let mut tasks = (0..TASKS)
        .map(|i| {
            let q = q.clone();
            task::spawn(async move {
                let mut bail_fut = future::poll_fn(|_| match BAIL.load(Ordering::Relaxed) {
                    false => Poll::Pending,
                    true => Poll::Ready(()),
                })
                .fuse();

                let mut wait_fut = pin!(q
                    .wait(CountDropKey {
                        idx: i,
                        cnt: &KEY_DROPS,
                    })
                    .fuse());

                // NOTE: `select_baised is used specifically to ensure the bail
                // future is processed first.
                select_biased! {
                    _a = bail_fut => {},
                    _b = wait_fut => {
                        COMPLETED.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for task in &mut tasks {
        assert_pending!(task.poll());
    }

    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);
    assert_eq!(KEY_DROPS.load(Ordering::Relaxed), 0);
    assert_eq!(VAL_DROPS.load(Ordering::Relaxed), 0);

    for i in 0..TASKS {
        q.wake(
            &CountDropKey {
                idx: i,
                cnt: &DONT_CARE,
            },
            CountDropVal { cnt: &VAL_DROPS },
        );
    }

    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);
    assert_eq!(KEY_DROPS.load(Ordering::Relaxed), 0);
    assert_eq!(VAL_DROPS.load(Ordering::Relaxed), 0);
    BAIL.store(true, Ordering::Relaxed);

    for task in &mut tasks {
        assert!(task.is_woken());
        assert_ready!(task.poll());
    }
    assert_eq!(COMPLETED.load(Ordering::Relaxed), 0);
    assert_eq!(KEY_DROPS.load(Ordering::Relaxed), TASKS);
    assert_eq!(VAL_DROPS.load(Ordering::Relaxed), TASKS);
}
