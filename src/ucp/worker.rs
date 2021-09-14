use super::*;
use derivative::*;
#[cfg(feature = "am")]
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
#[cfg(feature = "am")]
use std::sync::RwLock;
#[cfg(feature = "event")]
use tokio::io::unix::AsyncFd;

/// An object representing the communication context.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Worker {
    pub(super) handle: ucp_worker_h,
    context: Arc<Context>,
    #[cfg(feature = "am")]
    #[derivative(Debug = "ignore")]
    pub(crate) am_handlers: RwLock<HashMap<u32, Rc<AmHandler>>>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        unsafe { ucp_worker_destroy(self.handle) }
    }
}

impl Worker {
    pub(super) fn new(context: &Arc<Context>) -> Rc<Self> {
        let mut params = MaybeUninit::<ucp_worker_params_t>::uninit();
        unsafe { (*params.as_mut_ptr()).field_mask = 0 };
        let mut handle = MaybeUninit::uninit();
        let status =
            unsafe { ucp_worker_create(context.handle, params.as_ptr(), handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        Rc::new(Worker {
            handle: unsafe { handle.assume_init() },
            context: context.clone(),
            #[cfg(feature = "am")]
            am_handlers: RwLock::new(HashMap::new()),
        })
    }

    /// Make progress on the worker.
    pub async fn polling(self: Rc<Self>) {
        while Rc::strong_count(&self) > 1 {
            while self.progress() != 0 {}
            futures_lite::future::yield_now().await;
        }
    }

    /// Wait event then make progress.
    ///
    /// This function register `event_fd` on tokio's event loop and wait `event_fd` become readable,
    ////  then call progress function.
    #[cfg(feature = "event")]
    pub async fn event_poll(self: Rc<Self>) {
        let wait_fd = AsyncFd::new(self.event_fd()).unwrap();
        while Rc::strong_count(&self) > 1 {
            while self.progress() != 0 {}
            if self.arm() {
                let mut ready = wait_fd.readable().await.unwrap();
                ready.clear_ready();
            }
        }
    }

    /// Prints information about the worker.
    ///
    /// Including protocols being used, thresholds, UCT transport methods,
    /// and other useful information associated with the worker.
    pub fn print_to_stderr(&self) {
        unsafe { ucp_worker_print_info(self.handle, stderr) };
    }

    pub fn thread_mode(&self) -> ucs_thread_mode_t {
        let mut attr = MaybeUninit::<ucp_worker_attr>::uninit();
        unsafe { &mut *attr.as_mut_ptr() }.field_mask =
            ucp_worker_attr_field::UCP_WORKER_ATTR_FIELD_THREAD_MODE.0 as u64;
        let status = unsafe { ucp_worker_query(self.handle, attr.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let attr = unsafe { attr.assume_init() };
        attr.thread_mode
    }

    /// Get the address of the worker object.
    ///
    /// This address can be passed to remote instances of the UCP library
    /// in order to connect to this worker.
    pub fn address(&self) -> WorkerAddress<'_> {
        let mut handle = MaybeUninit::uninit();
        let mut length = MaybeUninit::uninit();
        let status = unsafe {
            ucp_worker_get_address(self.handle, handle.as_mut_ptr(), length.as_mut_ptr())
        };
        assert_eq!(status, ucs_status_t::UCS_OK);
        WorkerAddress {
            handle: unsafe { handle.assume_init() },
            length: unsafe { length.assume_init() } as usize,
            worker: self,
        }
    }

    pub fn create_listener(self: &Rc<Self>, addr: SocketAddr) -> Listener {
        Listener::new(self, addr)
    }

    pub fn connect(self: &Rc<Self>, addr: SocketAddr) -> Endpoint {
        Endpoint::connect(self, addr)
    }

    pub fn accept(self: &Rc<Self>, connection: ConnectionRequest) -> Endpoint {
        Endpoint::accept(self, connection)
    }

    /// Waits (blocking) until an event has happened.
    pub fn wait(&self) {
        let status = unsafe { ucp_worker_wait(self.handle) };
        assert_eq!(status, ucs_status_t::UCS_OK);
    }

    /// This needs to be called before waiting on each notification on this worker.
    ///
    /// Returns 'true' if one can wait for events (sleep mode).
    pub fn arm(&self) -> bool {
        let status = unsafe { ucp_worker_arm(self.handle) };
        match status {
            ucs_status_t::UCS_OK => true,
            ucs_status_t::UCS_ERR_BUSY => false,
            _ => panic!("{:?}", status),
        }
    }

    /// Explicitly progresses all communication operations on a worker.
    pub fn progress(&self) -> u32 {
        unsafe { ucp_worker_progress(self.handle) }
    }

    /// Returns a valid file descriptor for polling functions.
    pub fn event_fd(&self) -> i32 {
        let mut fd = MaybeUninit::uninit();
        let status = unsafe { ucp_worker_get_efd(self.handle, fd.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        unsafe { fd.assume_init() }
    }

    /// This routine flushes all outstanding AMO and RMA communications on the worker.
    pub fn flush(&self) {
        let status = unsafe { ucp_worker_flush(self.handle) };
        assert_eq!(status, ucs_status_t::UCS_OK);
    }
}

impl AsRawFd for Worker {
    fn as_raw_fd(&self) -> i32 {
        self.event_fd()
    }
}

/// The address of the worker object.
#[derive(Debug)]
pub struct WorkerAddress<'a> {
    handle: *mut ucp_address_t,
    length: usize,
    worker: &'a Worker,
}

impl<'a> AsRef<[u8]> for WorkerAddress<'a> {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.handle as *const u8, self.length) }
    }
}

impl<'a> Drop for WorkerAddress<'a> {
    fn drop(&mut self) {
        unsafe { ucp_worker_release_address(self.worker.handle, self.handle) }
    }
}
