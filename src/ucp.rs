//! Mid-level bindings for UCP.

use futures::future::poll_fn;
use futures::task::AtomicWaker;
use std::collections::VecDeque;
use std::ffi::CString;
use std::future::Future;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::SocketAddr;
use std::os::raw::c_void;
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::ptr::NonNull;
use std::ptr::{null, null_mut};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use ucx_sys::*;

#[path = "rma.rs"]
pub mod rma;

#[derive(Debug)]
pub struct Config {
    handle: *mut ucp_config_t,
}

impl Default for Config {
    fn default() -> Self {
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_config_read(null(), null(), handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        Config {
            handle: unsafe { handle.assume_init() },
        }
    }
}

impl Config {
    pub fn print_to_stderr(&self) {
        let flags = ucs_config_print_flags_t::UCS_CONFIG_PRINT_CONFIG
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_DOC
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_HEADER
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_HIDDEN;
        let title = CString::new("UCP Configuration").expect("Not a valid CStr");
        unsafe { ucp_config_print(self.handle, stderr, title.as_ptr(), flags) };
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        unsafe { ucp_config_release(self.handle) };
    }
}

#[derive(Debug)]
pub struct Context {
    handle: ucp_context_h,
}

unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn new(config: &Config) -> Arc<Self> {
        let params = ucp_params_t {
            field_mask: (ucp_params_field::UCP_PARAM_FIELD_FEATURES
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_SIZE
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_INIT
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_CLEANUP
                | ucp_params_field::UCP_PARAM_FIELD_MT_WORKERS_SHARED)
                .0 as u64,
            features: (ucp_feature::UCP_FEATURE_RMA
                | ucp_feature::UCP_FEATURE_STREAM
                | ucp_feature::UCP_FEATURE_WAKEUP)
                .0 as u64,
            request_size: std::mem::size_of::<Request>() as u64,
            request_init: Some(Request::init),
            request_cleanup: Some(Request::cleanup),
            tag_sender_mask: 0,
            mt_workers_shared: 1,
            estimated_num_eps: 0,
            estimated_num_ppn: 0,
        };
        let mut handle = MaybeUninit::uninit();
        let status = unsafe {
            ucp_init_version(
                UCP_API_MAJOR,
                UCP_API_MINOR,
                &params,
                config.handle,
                handle.as_mut_ptr(),
            )
        };
        assert_eq!(status, ucs_status_t::UCS_OK);
        Arc::new(Context {
            handle: unsafe { handle.assume_init() },
        })
    }

    pub fn create_worker(self: &Arc<Self>) -> Arc<Worker> {
        Worker::new(self)
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe { ucp_cleanup(self.handle) };
    }
}

#[derive(Debug)]
pub struct Worker {
    handle: ucp_worker_h,
    context: Arc<Context>,
}

unsafe impl Send for Worker {}
unsafe impl Sync for Worker {}

impl Drop for Worker {
    fn drop(&mut self) {
        unsafe { ucp_worker_destroy(self.handle) }
    }
}

impl Worker {
    fn new(context: &Arc<Context>) -> Arc<Self> {
        let params = ucp_worker_params_t {
            field_mask: ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE.0 as u64,
            thread_mode: ucs_thread_mode_t::UCS_THREAD_MODE_MULTI,
            cpu_mask: ucs_cpu_set_t { ucs_bits: [0; 16] },
            events: 0,
            event_fd: 0,
            user_data: null_mut(),
        };
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_worker_create(context.handle, &params, handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let worker = Arc::new(Worker {
            handle: unsafe { handle.assume_init() },
            context: context.clone(),
        });
        assert_eq!(
            worker.thread_mode(),
            ucs_thread_mode_t::UCS_THREAD_MODE_MULTI
        );
        worker
    }

    pub fn print_to_stderr(&self) {
        unsafe { ucp_worker_print_info(self.handle, stderr) };
    }

    fn thread_mode(&self) -> ucs_thread_mode_t {
        let mut attr = ucp_worker_attr {
            field_mask: ucp_worker_attr_field::UCP_WORKER_ATTR_FIELD_THREAD_MODE.0 as u64,
            ..unsafe { MaybeUninit::uninit().assume_init() }
        };
        let status = unsafe { ucp_worker_query(self.handle, &mut attr) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        attr.thread_mode
    }

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

    pub fn create_listener(self: &Arc<Self>, addr: SocketAddr) -> Arc<Listener> {
        Listener::new(self, addr)
    }

    pub fn create_endpoint(self: &Arc<Self>, addr: SocketAddr) -> Endpoint {
        Endpoint::new(self, addr)
    }

    pub fn wait(&self) {
        let status = unsafe { ucp_worker_wait(self.handle) };
        assert_eq!(status, ucs_status_t::UCS_OK);
    }

    /// Returns 'true' if one can wait for events (sleep mode).
    pub fn arm(&self) -> bool {
        let status = unsafe { ucp_worker_arm(self.handle) };
        match status {
            ucs_status_t::UCS_OK => true,
            ucs_status_t::UCS_ERR_BUSY => false,
            _ => panic!("{:?}", status),
        }
    }

    pub fn progress(&self) -> u32 {
        unsafe { ucp_worker_progress(self.handle) }
    }

    pub fn event_fd(&self) -> i32 {
        let mut fd = MaybeUninit::uninit();
        let status = unsafe { ucp_worker_get_efd(self.handle, fd.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        unsafe { fd.assume_init() }
    }

    /// Installs a user defined callback to handle incoming Active Messages with a specific id.
    pub fn set_am_handler(&self, id: u16, arg: usize) {
        unsafe extern "C" fn callback(
            arg: *mut c_void,
            data: *mut c_void,
            length: u64,
            _reply_ep: ucp_ep_h,
            _flags: u32,
        ) -> ucs_status_t {
            trace!("active_message: arg={:?}, len={:?}", arg, length);
            let _data = std::slice::from_raw_parts(data as *const u8, length as _);
            // TODO: release data
            ucs_status_t::UCS_OK
        }
        let status =
            unsafe { ucp_worker_set_am_handler(self.handle, id, Some(callback), arg as _, 0) };
        assert_eq!(status, ucs_status_t::UCS_OK);
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

#[derive(Debug)]
pub struct Listener {
    handle: ucp_listener_h,
    incomings: Mutex<Queue>,
    worker: Arc<Worker>,
}

#[derive(Debug, Default)]
struct Queue {
    items: VecDeque<Endpoint>,
    wakers: Vec<Waker>,
}

impl Listener {
    fn new(worker: &Arc<Worker>, addr: SocketAddr) -> Arc<Self> {
        unsafe extern "C" fn accept_handler(ep: ucp_ep_h, arg: *mut c_void) {
            trace!("accept endpoint={:?}", ep);
            let listener = ManuallyDrop::new(Arc::from_raw(arg as *const Listener));
            let endpoint = Endpoint {
                handle: ep,
                worker: listener.worker.clone(),
            };
            let mut incomings = listener.incomings.lock().unwrap();
            incomings.items.push_back(endpoint);
            for waker in incomings.wakers.drain(..) {
                waker.wake();
            }
        }
        #[allow(clippy::uninit_assumed_init)]
        let mut listener = Arc::new(Listener {
            handle: unsafe { MaybeUninit::uninit().assume_init() },
            incomings: Mutex::default(),
            worker: worker.clone(),
        });
        let sockaddr = os_socketaddr::OsSocketAddr::from(addr);
        let params = ucp_listener_params_t {
            field_mask: (ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_SOCK_ADDR
                | ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_ACCEPT_HANDLER)
                .0 as u64,
            sockaddr: ucs_sock_addr {
                addr: sockaddr.as_ptr() as _,
                addrlen: sockaddr.len(),
            },
            accept_handler: ucp_listener_accept_handler_t {
                cb: Some(accept_handler),
                arg: listener.as_ref() as *const Self as _,
            },
            conn_handler: ucp_listener_conn_handler_t {
                cb: None,
                arg: null_mut(),
            },
        };
        let handle = &mut Arc::get_mut(&mut listener).unwrap().handle;
        let status = unsafe { ucp_listener_create(worker.handle, &params, handle) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        listener
    }

    pub fn socket_addr(&self) -> SocketAddr {
        #[allow(clippy::uninit_assumed_init)]
        let mut attr = ucp_listener_attr_t {
            field_mask: ucp_listener_attr_field::UCP_LISTENER_ATTR_FIELD_SOCKADDR.0 as u64,
            sockaddr: unsafe { MaybeUninit::uninit().assume_init() },
        };
        let status = unsafe { ucp_listener_query(self.handle, &mut attr) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let sockaddr = unsafe {
            os_socketaddr::OsSocketAddr::from_raw_parts(&attr.sockaddr as *const _ as _, 8)
        };
        sockaddr.into_addr().unwrap()
    }

    pub async fn accept(&self) -> Endpoint {
        poll_fn(|cx| {
            let mut incomings = self.incomings.lock().unwrap();
            if let Some(endpoint) = incomings.items.pop_front() {
                Poll::Ready(endpoint)
            } else {
                incomings.wakers.push(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        unsafe { ucp_listener_destroy(self.handle) }
    }
}

#[derive(Debug)]
pub struct Endpoint {
    handle: ucp_ep_h,
    worker: Arc<Worker>,
}

unsafe impl Send for Endpoint {}

impl Endpoint {
    fn new(worker: &Arc<Worker>, addr: SocketAddr) -> Self {
        let sockaddr = os_socketaddr::OsSocketAddr::from(addr);
        let params = ucp_ep_params {
            field_mask: (ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR)
                .0 as u64,
            flags: ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER.0,
            sockaddr: ucs_sock_addr {
                addr: sockaddr.as_ptr() as _,
                addrlen: sockaddr.len(),
            },
            // set NONE to enable TCP
            // ref: https://github.com/rapidsai/ucx-py/issues/194#issuecomment-535726896
            err_mode: ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_NONE,
            err_handler: ucp_err_handler {
                cb: None,
                arg: null_mut(),
            },
            user_data: null_mut(),
            address: null_mut(),
            conn_request: null_mut(),
        };
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_ep_create(worker.handle, &params, handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let handle = unsafe { handle.assume_init() };
        trace!("create endpoint={:?}", handle);
        Endpoint {
            handle,
            worker: worker.clone(),
        }
    }

    pub fn print_to_stderr(&self) {
        unsafe { ucp_ep_print_info(self.handle, stderr) };
    }

    pub fn stream_send(&self, buf: &[u8]) -> RequestHandle {
        trace!("stream_send: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
            trace!(
                "stream_send: complete. req={:?}, status={:?}",
                request,
                status
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let status = unsafe {
            ucp_stream_send_nb(
                self.handle,
                buf.as_ptr() as _,
                buf.len() as _,
                ucp_dt_make_contig(1),
                Some(callback),
                0,
            )
        };
        if status.is_null() {
            trace!("stream_send: complete");
            RequestHandle::Ready(buf.len())
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle::from(status, buf.len())
        } else {
            panic!("failed to send stream: {:?}", UCS_PTR_RAW_STATUS(status));
        }
    }

    pub fn stream_recv(&self, buf: &mut [u8]) -> RequestHandle {
        trace!("stream_recv: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t, length: u64) {
            trace!(
                "stream_recv: complete. req={:?}, status={:?}, len={}",
                request,
                status,
                length
            );
            let request = &mut *(request as *mut Request);
            request.length = length as usize;
            request.waker.wake();
        }
        let mut length = MaybeUninit::uninit();
        let status = unsafe {
            ucp_stream_recv_nb(
                self.handle,
                buf.as_mut_ptr() as _,
                buf.len() as _,
                ucp_dt_make_contig(1),
                Some(callback),
                length.as_mut_ptr(),
                0,
            )
        };
        if status.is_null() {
            let length = unsafe { length.assume_init() } as usize;
            trace!("stream_recv: complete. len={}", length);
            RequestHandle::Ready(length)
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle::from(status, 0)
        } else {
            panic!("failed to recv stream: {:?}", UCS_PTR_RAW_STATUS(status));
        }
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        trace!("destroy endpoint={:?}", self.handle);
        unsafe { ucp_ep_destroy(self.handle) }
    }
}

/// Our defined request structure stored at `ucs_status_ptr_t`.
///
/// To enable this, set the following fields in `ucp_params_t` when initializing
/// UCP context:
/// ```ignore
/// ucp_params_t {
///     request_size: std::mem::size_of::<Request>() as u64,
///     request_init: Some(Request::init),
///     request_cleanup: Some(Request::cleanup),
/// }
/// ```
#[derive(Default)]
pub struct Request {
    waker: AtomicWaker,
    length: usize,
}

impl Request {
    /// Initialize request.
    ///
    /// This function will be called only on the very first time a request memory
    /// is initialized, and may not be called again if a request is reused.
    unsafe extern "C" fn init(request: *mut c_void) {
        (request as *mut Self).write(Request::default());
    }

    /// Final cleanup of the memory associated with the request.
    ///
    /// This routine may not be called every time a request is released.
    unsafe extern "C" fn cleanup(request: *mut c_void) {
        std::ptr::drop_in_place(request as *mut Self)
    }
}

/// A handle to the request returned from async IO functions.
pub enum RequestHandle {
    Ready(usize),
    Pending(NonNull<Request>),
}

unsafe impl Send for RequestHandle {}

impl RequestHandle {
    fn from(status_ptr: ucs_status_ptr_t, len: usize) -> Self {
        assert!(UCS_PTR_IS_PTR(status_ptr));
        let mut ptr = NonNull::new(status_ptr as *mut Request).unwrap();
        unsafe { ptr.as_mut() }.length = len;
        RequestHandle::Pending(ptr)
    }

    fn check_status(&self) -> ucs_status_t {
        match self {
            RequestHandle::Ready(_) => ucs_status_t::UCS_OK,
            RequestHandle::Pending(ptr) => unsafe { ucp_request_check_status(ptr.as_ptr() as _) },
        }
    }

    fn is_completed(&self) -> bool {
        self.check_status() != ucs_status_t::UCS_INPROGRESS
    }

    fn len(&self) -> usize {
        match self {
            RequestHandle::Ready(len) => *len,
            RequestHandle::Pending(ptr) => unsafe { ptr.as_ref() }.length,
        }
    }

    fn register_waker(&mut self, waker: &Waker) {
        if let RequestHandle::Pending(ptr) = self {
            unsafe { ptr.as_mut() }.waker.register(waker);
        }
    }
}

impl Future for RequestHandle {
    type Output = usize;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        if self.is_completed() {
            return Poll::Ready(self.len());
        }
        self.register_waker(cx.waker());
        if self.is_completed() {
            return Poll::Ready(self.len());
        }
        Poll::Pending
    }
}

impl Drop for RequestHandle {
    fn drop(&mut self) {
        if let RequestHandle::Pending(ptr) = self {
            trace!("request free: {:?}", ptr.as_ptr());
            unsafe { ucp_request_free(ptr.as_ptr() as _) };
        }
    }
}

extern "C" {
    static stderr: *mut FILE;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new() {
        let config = Config::default();
        let context = Context::new(&config);
        let worker1 = context.create_worker();
        let listener = worker1.create_listener("0.0.0.0:0".parse().unwrap());
        let listen_port = listener.socket_addr().port();

        std::thread::spawn(move || loop {
            worker1.wait();
            worker1.progress();
        });
        std::thread::spawn(move || {
            let worker2 = context.create_worker();
            let mut addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            addr.set_port(listen_port);
            let _endpoint = worker2.create_endpoint(addr);
        });

        let _endpoint = listener.accept().await;
    }
}
