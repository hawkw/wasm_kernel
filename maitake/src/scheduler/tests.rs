use super::*;
use crate::{
    future,
    loom::sync::atomic::{AtomicUsize, Ordering::Relaxed},
    sync::WaitCell,
};
use core::{pin::pin, task::Poll};
use std::sync::Arc;

#[cfg(all(feature = "alloc", not(loom)))]
mod alloc_tests;

#[cfg(not(loom))]
mod custom_storage;

#[cfg(any(loom, feature = "alloc"))]
mod loom;

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
        future::poll_fn(move |cx| {
            let Some(this) = this.upgrade() else {
                return Poll::Ready(());
            };

            let res = pin!(this.task.wait());

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
        debug!(chan = ?fmt::alt(&self), "drop");
    }
}
