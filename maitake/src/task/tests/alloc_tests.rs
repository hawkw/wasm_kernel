use super::*;
use crate::scheduler::Scheduler;
use alloc::boxed::Box;
use core::{
    ptr,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio_test::{assert_pending, assert_ready_err, assert_ready_ok};

/// This test ensures that layout-dependent casts in the `Task` struct's
/// vtable methods are valid.
#[test]
fn task_is_valid_for_casts() {
    let task = Box::new(Task::<NopSchedule, _, BoxStorage>::new(async {}));

    let task_ptr = Box::into_raw(task);

    // scope to ensure all task ptrs are dropped before we deallocate the
    // task allocation
    {
        let header_ptr = unsafe { ptr::addr_of!((*task_ptr).schedulable.header) };
        assert_eq!(
            task_ptr as *const (), header_ptr as *const (),
            "header pointer and task allocation pointer must have the same address!"
        );

        let sched_ptr = unsafe { ptr::addr_of!((*task_ptr).schedulable) };
        assert_eq!(
            task_ptr as *const (), sched_ptr as *const (),
            "schedulable pointer and task allocation pointer must have the same address!"
        );
    }

    // clean up after ourselves by ensuring the box is deallocated
    unsafe { drop(Box::from_raw(task_ptr)) }
}

/// This test just prints the size (in bytes) of an empty task struct.
#[test]
// No sense spending time running these trivial tests under Miri...
#[cfg_attr(miri, ignore)]
fn empty_task_size() {
    use core::{
        any::type_name,
        future,
        mem::{size_of, size_of_val},
    };

    type Future = future::Ready<()>;
    type EmptyTask = Task<NopSchedule, Future, BoxStorage>;

    println!(
        "---- task size (in {} mode) ----\n",
        if cfg!(debug_assertions) {
            "debug "
        } else {
            "release"
        }
    );
    println!("{}: {}B", type_name::<EmptyTask>(), size_of::<EmptyTask>(),);
    println!("{}: {}B", type_name::<Future>(), size_of::<Future>(),);
    println!(
        "task size: {}B",
        size_of::<EmptyTask>() - size_of::<Future>()
    );

    let task = Task::<Scheduler, Future, BoxStorage>::new(future::ready(()));
    println!("\nTask {{ // {}B", size_of_val(&task));
    println!(
        "    schedulable: Schedulable {{ // {}B",
        size_of_val(&task.schedulable)
    );
    println!(
        "        header: {{ // {}B",
        size_of_val(&task.schedulable.header)
    );
    println!(
        "            run_queue: {}B",
        size_of_val(&task.schedulable.header.run_queue)
    );
    println!(
        "            state: {}B",
        size_of_val(&task.schedulable.header.state)
    );

    // clippy warns us that the vtable field on `schedulable.header` is a
    // reference, and that we probably want to dereference it to get the actual
    // size of the vtable. but...we *don't*. we want the size of the reference,
    // since we care about the actual header size here.
    #[allow(clippy::size_of_ref)]
    let vtable_ptr = size_of_val(&task.schedulable.header.vtable);
    println!("            vtable (ptr): {vtable_ptr}B",);
    println!(
        "            id: {}B",
        size_of_val(&task.schedulable.header.id)
    );
    println!(
        "            span: {}B",
        size_of_val(&task.schedulable.header.span)
    );
    println!("         }}");
    println!(
        "        scheduler: {}B",
        size_of_val(&task.schedulable.scheduler)
    );
    println!("    }}");
    println!("    inner: {}B", size_of_val(&task.inner));
    println!("    join_waker: {}B", size_of_val(&task.join_waker));
    println!("}}");
}

#[test]
fn join_handle_wakes() {
    let _trace = crate::util::trace_init();

    let scheduler = Scheduler::new();
    let join = scheduler.spawn(async move {
        future::yield_now().await;
        "hello world!"
    });

    let mut join = tokio_test::task::spawn(join);

    // the join handle should be pending until the scheduler runs.
    assert_pending!(test_dbg!(join.poll()), "join handle should be pending");
    assert!(!join.is_woken());

    // tick the scheduler.
    scheduler.tick();

    // the spawned task should complete on this tick.
    assert!(join.is_woken());
    let output = assert_ready_ok!(test_dbg!(join.poll()), "join handle should be notified");
    assert_eq!(test_dbg!(output), "hello world!");
}

#[test]
fn join_handle_cancels_before_poll() {
    let _trace = crate::util::trace_init();

    let scheduler = Scheduler::new();
    let join = scheduler.spawn(async move {
        future::yield_now().await;
        "hello world!"
    });

    let mut join = tokio_test::task::spawn(join);

    // the join handle should be pending until the scheduler runs.
    assert_pending!(test_dbg!(join.poll()), "join handle should be pending");
    assert!(!join.is_woken());

    join.cancel();

    // tick the scheduler.
    scheduler.tick();

    // the spawned task should complete on this tick.
    assert!(join.is_woken());
    let err = assert_ready_err!(test_dbg!(join.poll()), "join handle should be notified");
    assert!(err.is_canceled(), "JoinError must be canceled");
    assert!(!err.is_completed(), "JoinError must be completed");
}

#[test]
fn join_handle_cancels_after_poll() {
    let _trace = crate::util::trace_init();

    let scheduler = Scheduler::new();
    let join = scheduler.spawn(async move {
        future::yield_now().await;
        "hello world!"
    });

    let mut join = tokio_test::task::spawn(join);

    // the join handle should be pending until the scheduler runs.
    assert_pending!(test_dbg!(join.poll()), "join handle should be pending");
    assert!(!join.is_woken());

    // tick the scheduler.
    scheduler.tick();

    join.cancel();

    // the spawned task should complete on this tick.
    assert!(join.is_woken());
    let err = assert_ready_err!(test_dbg!(join.poll()), "join handle should be notified");
    assert!(err.is_canceled(), "JoinError must be canceled");
    assert!(err.is_completed(), "JoinError must be completed");
}

#[test]
fn taskref_cancels_before_poll() {
    let _trace = crate::util::trace_init();

    let scheduler = Scheduler::new();
    let join = scheduler.spawn(async move {
        future::yield_now().await;
        "hello world!"
    });

    let taskref = join.task_ref();
    let mut join = tokio_test::task::spawn(join);

    // the join handle should be pending until the scheduler runs.
    assert_pending!(test_dbg!(join.poll()), "join handle should be pending");
    assert!(!join.is_woken());

    assert!(taskref.cancel());

    // tick the scheduler.
    scheduler.tick();

    // the spawned task should complete on this tick.
    assert!(join.is_woken());
    let err = assert_ready_err!(test_dbg!(join.poll()), "join handle should be notified");
    assert!(err.is_canceled(), "JoinError must be canceled");
    assert!(!err.is_completed(), "JoinError must be completed");
}

#[test]
fn taskref_cancels_after_poll() {
    let _trace = crate::util::trace_init();

    let scheduler = Scheduler::new();
    let join = scheduler.spawn(async move {
        future::yield_now().await;
        "hello world!"
    });

    let taskref = join.task_ref();
    let mut join = tokio_test::task::spawn(join);

    // the join handle should be pending until the scheduler runs.
    assert_pending!(test_dbg!(join.poll()), "join handle should be pending");
    assert!(!join.is_woken());

    // tick the scheduler.
    scheduler.tick();

    assert!(taskref.cancel());

    // the spawned task should complete on this tick.
    assert!(join.is_woken());
    let err = assert_ready_err!(test_dbg!(join.poll()), "join handle should be notified");
    assert!(err.is_canceled(), "JoinError must be canceled");
    assert!(err.is_completed(), "JoinError must be completed");
}

#[test]
fn drop_join_handle() {
    let _trace = crate::util::trace_init();
    static COMPLETED: AtomicBool = AtomicBool::new(false);
    let scheduler = Scheduler::new();
    let join = scheduler.spawn(async move {
        future::yield_now().await;
        COMPLETED.store(true, Ordering::Relaxed);
    });

    drop(join);

    // tick the scheduler.
    scheduler.tick();

    assert!(COMPLETED.load(Ordering::Relaxed))
}

// Test for potential UB in `Cell::poll` due to niche optimization.
// See https://github.com/rust-lang/miri/issues/3780 for details.
//
// This is based on the test for analogous types in Tokio added in
// https://github.com/tokio-rs/tokio/pull/6744
#[test]
fn cell_miri() {
    use super::Cell;
    use alloc::{string::String, sync::Arc, task::Wake};
    use core::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    struct DummyWaker;

    impl Wake for DummyWaker {
        fn wake(self: Arc<Self>) {}
    }

    struct ThingAdder<'a> {
        thing: &'a mut String,
    }

    impl Future for ThingAdder<'_> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            unsafe {
                *self.get_unchecked_mut().thing += ", world";
            }
            Poll::Pending
        }
    }

    let mut thing = "hello".to_owned();

    // The async block is necessary to trigger the miri failure.
    #[allow(clippy::redundant_async_block)]
    let fut = async move { ThingAdder { thing: &mut thing }.await };

    let mut fut = Cell::Pending(fut);

    let waker = Arc::new(DummyWaker).into();
    let mut ctx = Context::from_waker(&waker);
    assert_eq!(fut.poll(&mut ctx), Poll::Pending);
    assert_eq!(fut.poll(&mut ctx), Poll::Pending);
}
