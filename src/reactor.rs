use crate::ucx::*;
use futures::future::{poll_fn, select};
use lazy_static::lazy_static;
use mio::{event::Evented, unix::EventedFd, Poll, PollOpt, Ready, Token};
use std::io::Result;
use std::sync::Arc;
use tokio::io::Registration;

/// Create a worker from default context, and register it to the reactor.
pub fn create_worker() -> Arc<Worker> {
    let worker = UCP_CONTEXT.create_worker();
    let ret = worker.clone();
    // spawn a future to make progress on the worker
    tokio::spawn(async move {
        let mut registration = Registration::new(&worker).unwrap();
        while Arc::strong_count(&worker) > 1 {
            select(
                poll_fn(|cx| registration.poll_read_ready(cx)),
                poll_fn(|cx| registration.poll_write_ready(cx)),
            )
            .await;
            // progress until no more events
            while worker.progress() != 0 {
                tokio::task::yield_now().await;
            }
            worker.arm();
        }
        registration.deregister(&worker).unwrap();
    });
    ret
}

lazy_static! {
    /// Global default UCP context.
    static ref UCP_CONTEXT: Arc<Context> = Context::new(&Config::new());
}

impl Evented for Worker {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> Result<()> {
        EventedFd(&self.event_fd()).register(poll, token, interest, opts)
    }
    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> Result<()> {
        EventedFd(&self.event_fd()).reregister(poll, token, interest, opts)
    }
    fn deregister(&self, poll: &Poll) -> Result<()> {
        EventedFd(&self.event_fd()).deregister(poll)
    }
}
