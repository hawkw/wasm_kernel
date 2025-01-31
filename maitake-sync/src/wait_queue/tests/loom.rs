use super::*;
use crate::loom::{self, future, sync::Arc, thread};

#[test]
fn wake_one() {
    loom::model(|| {
        let q = Arc::new(WaitQueue::new());
        let thread = thread::spawn({
            let q = q.clone();
            move || {
                future::block_on(async {
                    q.wait().await.expect("queue must not be closed");
                });
            }
        });

        q.wake();
        thread.join().unwrap();
    });
}

#[test]
fn wake_all_sequential() {
    loom::model(|| {
        let q = Arc::new(WaitQueue::new());
        let wait1 = q.wait();
        let wait2 = q.wait();

        let thread = thread::spawn({
            let q = q.clone();
            move || {
                q.wake_all();
            }
        });

        future::block_on(async {
            wait1.await.unwrap();
            wait2.await.unwrap();
        });

        thread.join().unwrap();
    });
}

#[test]
#[cfg(feature = "alloc")]
fn wake_all_concurrent() {
    use alloc::sync::Arc;
    // must be higher than the number of threads in a `WakeBatch`, but below
    // Loom's min thread count.
    const THREADS: usize = 3;

    loom::model(|| {
        let q = Arc::new(WaitQueue::new());
        let mut waits = (0..THREADS)
            .map(|_| q.clone().wait_owned())
            .collect::<Vec<_>>();

        let threads = waits
            .drain(..)
            .map(|wait| thread::spawn(move || future::block_on(wait).expect("wait must not fail")));

        q.wake_all();

        for thread in threads {
            thread.join().unwrap();
        }
    });
}

#[test]
#[cfg(loom)] // this uses the model builder, which i don't wanna rewrite a stub version of...
fn wake_all_reregistering() {
    use alloc::sync::Arc;
    const THREADS: usize = 2;

    fn run_thread(q: Arc<(WaitQueue, AtomicUsize)>) -> impl FnOnce() {
        move || {
            future::block_on(async move {
                let (ref q, ref done) = &*q;
                q.wait().await.expect("queue must not close");
                q.wait().await.expect("queue must not close");
                done.fetch_add(1, SeqCst);
            })
        }
    }

    let mut builder = loom::model::Builder::new();
    // run with a lower preemption bound to try and make this test not take
    // forever...
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let q = Arc::new((WaitQueue::new(), AtomicUsize::new(0)));

        let threads = (0..THREADS)
            .map(|_| thread::spawn(run_thread(q.clone())))
            .collect::<Vec<_>>();

        let (ref q, ref done) = &*q;
        while test_dbg!(done.load(SeqCst)) < THREADS {
            q.wake_all();
            thread::yield_now();
        }

        for thread in threads {
            thread.join().unwrap();
        }
    });
}

#[test]
#[cfg(feature = "alloc")]
fn wake_close() {
    use alloc::sync::Arc;

    loom::model(|| {
        let q = Arc::new(WaitQueue::new());
        let wait1 = q.wait_owned();
        let wait2 = q.wait_owned();

        let thread1 =
            thread::spawn(move || future::block_on(wait1).expect_err("wait1 must be canceled"));
        let thread2 =
            thread::spawn(move || future::block_on(wait2).expect_err("wait2 must be canceled"));

        q.close();

        thread1.join().unwrap();
        thread2.join().unwrap();
    });
}

#[test]
fn wake_one_many() {
    loom::model(|| {
        let q = Arc::new(WaitQueue::new());

        fn thread(q: &Arc<WaitQueue>) -> thread::JoinHandle<()> {
            let q = q.clone();
            thread::spawn(move || {
                future::block_on(async {
                    q.wait().await.expect("queue must not be closed");
                    q.wake();
                })
            })
        }

        q.wake();

        let thread1 = thread(&q);
        let thread2 = thread(&q);

        thread1.join().unwrap();
        thread2.join().unwrap();

        future::block_on(async {
            q.wait().await.expect("queue must not be closed");
        });
    });
}

#[test]
fn wake_mixed() {
    loom::model(|| {
        let q = Arc::new(WaitQueue::new());

        let thread1 = thread::spawn({
            let q = q.clone();
            move || {
                q.wake_all();
            }
        });

        let thread2 = thread::spawn({
            let q = q.clone();
            move || {
                q.wake();
            }
        });

        let thread3 = thread::spawn(move || {
            future::block_on(q.wait()).unwrap();
        });

        thread1.join().unwrap();
        thread2.join().unwrap();
        thread3.join().unwrap();
    });
}

#[test]
fn drop_wait_future() {
    use std::future::poll_fn;
    use std::pin::pin;
    use std::task::Poll;

    loom::model(|| {
        let q = Arc::new(WaitQueue::new());

        let thread1 = thread::spawn({
            let q = q.clone();
            move || {
                let mut wait = pin!(q.wait());

                future::block_on(poll_fn(|cx| {
                    if wait.as_mut().poll(cx).is_ready() {
                        q.wake();
                    }
                    Poll::Ready(())
                }));
            }
        });

        let thread2 = thread::spawn({
            let q = q.clone();
            move || {
                future::block_on(async {
                    q.wait().await.unwrap();
                    // Trigger second notification
                    q.wake();
                    q.wait().await.unwrap();
                });
            }
        });

        q.wake();

        thread1.join().unwrap();
        thread2.join().unwrap();
    });
}
