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
    pub(crate) am_streams: RwLock<HashMap<u16, Rc<AmStreamInner>>>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        unsafe { ucp_worker_destroy(self.handle) }
    }
}

impl Worker {
    pub(super) fn new(context: &Arc<Context>) -> Result<Rc<Self>, Error> {
        let mut params = MaybeUninit::<ucp_worker_params_t>::uninit();
        unsafe {
            (*params.as_mut_ptr()).field_mask =
                ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE.0 as _;
            (*params.as_mut_ptr()).thread_mode = ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE;
        };
        let mut handle = MaybeUninit::uninit();
        let status =
            unsafe { ucp_worker_create(context.handle, params.as_ptr(), handle.as_mut_ptr()) };
        Error::from_status(status)?;

        Ok(Rc::new(Worker {
            handle: unsafe { handle.assume_init() },
            context: context.clone(),
            #[cfg(feature = "am")]
            am_streams: RwLock::new(HashMap::new()),
        }))
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
    pub async fn event_poll(self: Rc<Self>) -> Result<(), Error> {
        let fd = self.event_fd()?;
        let wait_fd = AsyncFd::new(fd).unwrap();
        while Rc::strong_count(&self) > 1 {
            while self.progress() != 0 {}
            if self.arm().unwrap() {
                let mut ready = wait_fd.readable().await.unwrap();
                ready.clear_ready();
            }
        }

        Ok(())
    }

    /// Prints information about the worker.
    ///
    /// Including protocols being used, thresholds, UCT transport methods,
    /// and other useful information associated with the worker.
    pub fn print_to_stderr(&self) {
        unsafe { ucp_worker_print_info(self.handle, stderr) };
    }

    /// Thread safe level of the context.
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
    pub fn address(&self) -> Result<WorkerAddress<'_>, Error> {
        let mut handle = MaybeUninit::uninit();
        let mut length = MaybeUninit::uninit();
        let status = unsafe {
            ucp_worker_get_address(self.handle, handle.as_mut_ptr(), length.as_mut_ptr())
        };
        Error::from_status(status)?;

        Ok(WorkerAddress {
            handle: unsafe { handle.assume_init() },
            length: unsafe { length.assume_init() } as usize,
            worker: self,
        })
    }

    /// Create a new [`Listener`].
    pub fn create_listener(self: &Rc<Self>, addr: SocketAddr) -> Result<Listener, Error> {
        Listener::new(self, addr)
    }

    /// Connect to a remote worker by address.
    pub fn connect_addr(self: &Rc<Self>, addr: &WorkerAddress) -> Result<Endpoint, Error> {
        Endpoint::connect_addr(self, addr.handle)
    }

    /// Connect to a remote listener.
    pub async fn connect_socket(self: &Rc<Self>, addr: SocketAddr) -> Result<Endpoint, Error> {
        Endpoint::connect_socket(self, addr).await
    }

    /// Accept a connection request.
    pub async fn accept(self: &Rc<Self>, connection: ConnectionRequest) -> Result<Endpoint, Error> {
        Endpoint::accept(self, connection).await
    }

    /// Waits (blocking) until an event has happened.
    pub fn wait(&self) -> Result<(), Error> {
        let status = unsafe { ucp_worker_wait(self.handle) };
        Error::from_status(status)
    }

    /// This needs to be called before waiting on each notification on this worker.
    ///
    /// Returns 'true' if one can wait for events (sleep mode).
    pub fn arm(&self) -> Result<bool, Error> {
        let status = unsafe { ucp_worker_arm(self.handle) };
        match status {
            ucs_status_t::UCS_OK => Ok(true),
            ucs_status_t::UCS_ERR_BUSY => Ok(false),
            status => Err(Error::from_error(status)),
        }
    }

    /// Explicitly progresses all communication operations on a worker.
    pub fn progress(&self) -> u32 {
        unsafe { ucp_worker_progress(self.handle) }
    }

    /// Returns a valid file descriptor for polling functions.
    pub fn event_fd(&self) -> Result<i32, Error> {
        let mut fd = MaybeUninit::uninit();
        let status = unsafe { ucp_worker_get_efd(self.handle, fd.as_mut_ptr()) };
        Error::from_status(status)?;

        unsafe { Ok(fd.assume_init()) }
    }

    /// This routine flushes all outstanding AMO and RMA communications on the worker.
    pub fn flush(&self) {
        let status = unsafe { ucp_worker_flush(self.handle) };
        assert_eq!(status, ucs_status_t::UCS_OK);
    }
}

impl AsRawFd for Worker {
    fn as_raw_fd(&self) -> i32 {
        self.event_fd().unwrap()
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
