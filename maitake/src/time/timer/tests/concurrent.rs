use super::*;
use crate::loom::{sync::Arc, thread};
use core::{
    future::{self, Future},
    pin::pin,
    task::{Context, Poll},
};

#[cfg(loom)]
use loom::future::block_on;
#[cfg(not(loom))]
use tokio_test::block_on;

#[cfg(loom)]
use loom::model;

#[cfg(not(loom))]
fn model(f: impl Fn() + Send + Sync + 'static) {
    let guard = crate::util::test::trace_init_with_default("info,maitake::time=trace");
    f();
    drop(guard);
}

#[test]
#[cfg_attr(not(loom), ignore)]
fn one_sleep() {
    model(|| {
        let clock = TestClock::start();
        let timer = Arc::new(Timer::new(TestClock::clock()));
        let thread = thread::spawn({
            let timer = timer.clone();
            let clock = clock.test_clock();
            move || {
                let _clock = clock.enter();
                let sleep = timer.sleep(Duration::from_secs(1));
                block_on(sleep)
            }
        });

        for _ in 0..10 {
            clock.advance(Duration::from_secs(1));
            timer.try_turn();
            thread::yield_now();
        }

        thread.join().unwrap();
    })
}

#[test]
#[cfg_attr(not(loom), ignore)]
fn two_sleeps_parallel() {
    model(|| {
        let clock = TestClock::start();
        let timer = Arc::new(Timer::new(TestClock::clock()));
        let thread1 = thread::spawn({
            let timer = timer.clone();
            let clock = clock.test_clock();
            move || {
                let _clock = clock.enter();
                let sleep = timer.sleep(Duration::from_secs(1));
                block_on(sleep)
            }
        });
        let thread2 = thread::spawn({
            let timer = timer.clone();
            let clock = clock.test_clock();
            move || {
                let _clock = clock.enter();
                let sleep = timer.sleep(Duration::from_secs(1));
                block_on(sleep)
            }
        });

        for _ in 0..10 {
            clock.advance(Duration::from_secs(1));
            timer.try_turn();
            thread::yield_now();
        }

        thread1.join().unwrap();
        thread2.join().unwrap();
    })
}

#[test]
#[cfg_attr(not(loom), ignore)]
fn two_sleeps_sequential() {
    model(|| {
        let clock = TestClock::start();
        let timer = Arc::new(Timer::new(TestClock::clock()));
        let thread = thread::spawn({
            let timer = timer.clone();
            let clock = clock.test_clock();
            move || {
                let _clock = clock.enter();
                block_on(async move {
                    timer.sleep(Duration::from_secs(1)).await;
                    timer.sleep(Duration::from_secs(1)).await;
                })
            }
        });

        for _ in 0..10 {
            clock.advance(Duration::from_secs(1));
            timer.try_turn();
            thread::yield_now();
        }

        thread.join().unwrap();
        drop(clock);
    })
}

#[test]
fn cancel_polled_sleeps() {
    fn poll_and_cancel(timer: Arc<Timer>) {
        block_on(async move {
            let mut sleep = pin!(timer.sleep_ticks(15));
            future::poll_fn(move |cx| {
                // poll once to register the sleep with the timer wheel, and
                // then return `Ready` so it gets canceled.
                let poll = sleep.as_mut().poll(cx);
                tokio_test::assert_pending!(
                    poll,
                    "sleep should not complete, as its timer has not been advanced",
                );
                let poll = sleep.as_mut().poll(cx);
                tokio_test::assert_pending!(
                    poll,
                    "sleep should not complete, as its timer has not been advanced",
                );
                Poll::Ready(())
            })
            .await
        })
    }

    model(|| {
        let clock = TestClock::start();
        let timer = Arc::new(Timer::new(TestClock::clock()));
        let thread = thread::spawn({
            let timer = timer.clone();
            let clock = clock.test_clock();
            move || {
                let _clock = clock.enter();
                poll_and_cancel(timer.clone())
            }
        });
        poll_and_cancel(timer);
        thread.join().unwrap()
    })
}

#[test]
#[cfg_attr(not(loom), ignore)]
fn reregister_waker() {
    model(|| {
        let clock = TestClock::start();
        let timer = Arc::new(Timer::new(TestClock::clock()));
        let thread = thread::spawn({
            let timer = timer.clone();
            let clock = clock.test_clock();
            move || {
                let _clock = clock.enter();
                let mut sleep = pin!(timer.sleep(Duration::from_secs(1)));
                // poll the sleep future initially with a no-op waker.
                let _ = sleep.as_mut().poll(&mut Context::from_waker(
                    futures_util::task::noop_waker_ref(),
                ));
                block_on(sleep)
            }
        });

        for _ in 0..10 {
            clock.advance(Duration::from_secs(1));
            timer.try_turn();
            thread::yield_now();
        }

        thread.join().unwrap();
    })
}
