use super::*;
use crate::loom::{self, future, sync::Arc, thread};

#[test]
fn wake_one() {
    loom::model(|| {
        let q = Arc::new(WaitMap::<usize, usize>::new());
        let thread = thread::spawn({
            let q = q.clone();
            move || {
                future::block_on(async {
                    let _val = q.wait(123).await;
                });
            }
        });

        loom::thread::yield_now();
        let _result = q.wake(&123, 666);
        q.close();

        thread.join().unwrap();
    });
}

#[test]
fn wake_two_sequential() {
    loom::model(|| {
        let q = Arc::new(WaitMap::<usize, usize>::new());
        let q2 = q.clone();

        let thread_a = thread::spawn(move || {
            let wait1 = q2.wait(123);
            let wait2 = q2.wait(456);
            future::block_on(async move {
                let _ = wait1.await;
                let _ = wait2.await;
            });
        });

        let thread_b = thread::spawn(move || {
            loom::thread::yield_now();
            let _result = q.wake(&123, 321);
            let _result = q.wake(&456, 654);
            q.close();
        });

        thread_a.join().unwrap();
        thread_b.join().unwrap();
    });
}

#[test]
#[cfg(feature = "alloc")]
fn wake_close() {
    use ::alloc::sync::Arc;

    loom::model(|| {
        let q = Arc::new(WaitMap::<usize, usize>::new());
        let wait1 = q.wait_owned(123);
        let wait2 = q.wait_owned(456);

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
fn wake_and_drop() {
    use futures_util::FutureExt;
    loom::model(|| {
        // use `Arc`s as the value type to ensure their destructors are run.
        let q = Arc::new(WaitMap::<usize, Arc<()>>::new());

        let thread = thread::spawn({
            let q = q.clone();
            move || {
                dbg!(q.wait(1).now_or_never());
            }
        });

        thread::yield_now();
        dbg!(q.wake(&1, Arc::new(())));

        thread.join().unwrap();
    });
}
