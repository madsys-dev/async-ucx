use crate::ucp::*;
use lazy_static::lazy_static;
use std::sync::Arc;

/// Create a worker from default context, and register it to the reactor.
pub fn create_worker() -> Arc<Worker> {
    let worker = UCP_CONTEXT.create_worker();
    let ret = worker.clone();
    // spawn a future to make progress on the worker
    tokio::spawn(async move {
        while Arc::strong_count(&worker) > 1 {
            while worker.progress() != 0 {}
            tokio::task::yield_now().await;
        }
    });
    ret
}

lazy_static! {
    /// Global default UCP context.
    pub static ref UCP_CONTEXT: Arc<Context> = Context::new(&Config::default());
}
