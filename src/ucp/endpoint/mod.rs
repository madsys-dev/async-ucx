use super::*;
use std::cell::Cell;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Weak;
use std::sync::atomic::AtomicBool;
use std::task::Poll;

#[cfg(feature = "am")]
mod am;
mod rma;
mod stream;
mod tag;

#[cfg(feature = "am")]
pub use self::am::*;
pub use self::rma::*;
pub use self::stream::*;
pub use self::tag::*;

// State associate with ucp_ep_h
// todo: Add a `get_user_data` to UCX
#[derive(Debug)]
struct EndpointInner {
    closed: AtomicBool,
    status: Cell<ucs_status_t>,
    worker: Rc<Worker>,
}

impl EndpointInner {
    fn new(worker: Rc<Worker>) -> Self {
        EndpointInner {
            closed: AtomicBool::new(false),
            status: Cell::new(ucs_status_t::UCS_OK),
            worker,
        }
    }

    fn closed(self: &Rc<Self>) {
        if self
            .closed
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
        {
            // release a weak reference
            let _weak = unsafe { Weak::from_raw(Rc::as_ptr(self)) };
            self.set_status(ucs_status_t::UCS_ERR_CONNECTION_RESET);
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    // call from `err_handler` or `close`
    #[inline]
    fn set_status(&self, status: ucs_status_t) {
        if status != ucs_status_t::UCS_OK {
            self.status.set(status)
        }
    }

    #[inline]
    fn check(&self) -> Result<(), Error> {
        let status = self.status.get();
        Error::from_status(status)
    }
}

/// Communication endpoint.
#[derive(Debug, Clone)]
pub struct Endpoint {
    handle: ucp_ep_h,
    inner: Rc<EndpointInner>,
}

impl Endpoint {
    fn create(worker: &Rc<Worker>, mut params: ucp_ep_params) -> Result<Self, Error> {
        let inner = Rc::new(EndpointInner::new(worker.clone()));
        let weak = Rc::downgrade(&inner);

        // ucp endpoint keep a weak reference to inner
        // this reference will drop when endpoint is closed
        let ptr = Weak::into_raw(weak);
        unsafe extern "C" fn callback(arg: *mut c_void, ep: ucp_ep_h, status: ucs_status_t) {
            let weak: Weak<EndpointInner> = Weak::from_raw(arg as _);
            if let Some(inner) = weak.upgrade() {
                inner.set_status(status);
                // don't drop weak reference
                std::mem::forget(weak);
            } else {
                // no strong rc, force close endpoint here
                let status = ucp_ep_close_nb(ep, ucp_ep_close_mode::UCP_EP_CLOSE_MODE_FORCE as _);
                let _ = Error::from_ptr(status)
                    .map_err(|err| error!("Force close endpoint failed, {}", err));
            }
        }

        params.field_mask |= (ucp_ep_params_field::UCP_EP_PARAM_FIELD_USER_DATA
            | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLER)
            .0 as u64;
        params.user_data = ptr as _;
        params.err_handler = ucp_err_handler {
            cb: Some(callback),
            arg: std::ptr::null_mut(), // override by user_data
        };

        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_ep_create(worker.handle, &params, handle.as_mut_ptr()) };
        if let Err(err) = Error::from_status(status) {
            // error happened, drop reference
            let _weak = unsafe { Weak::from_raw(ptr as _) };
            return Err(err);
        }

        let handle = unsafe { handle.assume_init() };
        trace!("create endpoint={:?}", handle);
        Ok(Self { handle, inner })
    }

    pub(super) async fn connect_socket(
        worker: &Rc<Worker>,
        addr: SocketAddr,
    ) -> Result<Self, Error> {
        let sockaddr = socket2::SockAddr::from(addr);
        #[allow(invalid_value)]
        #[allow(clippy::uninit_assumed_init)]
        let params = ucp_ep_params {
            field_mask: (ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE)
                .0 as u64,
            flags: ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER.0,
            sockaddr: ucs_sock_addr {
                addr: sockaddr.as_ptr() as _,
                addrlen: sockaddr.len(),
            },
            err_mode: ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER,
            ..unsafe { MaybeUninit::uninit().assume_init() }
        };
        let endpoint = Endpoint::create(worker, params)?;

        // Workaround for UCX bug: https://github.com/openucx/ucx/issues/6872
        let buf = [0, 1, 2, 3];
        endpoint.stream_send(&buf).await?;

        Ok(endpoint)
    }

    pub(super) fn connect_addr(
        worker: &Rc<Worker>,
        addr: *const ucp_address_t,
    ) -> Result<Self, Error> {
        #[allow(invalid_value)]
        #[allow(clippy::uninit_assumed_init)]
        let params = ucp_ep_params {
            field_mask: (ucp_ep_params_field::UCP_EP_PARAM_FIELD_REMOTE_ADDRESS
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE)
                .0 as u64,
            address: addr,
            err_mode: ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER,
            ..unsafe { MaybeUninit::uninit().assume_init() }
        };
        Endpoint::create(worker, params)
    }

    pub(super) async fn accept(
        worker: &Rc<Worker>,
        connection: ConnectionRequest,
    ) -> Result<Self, Error> {
        #[allow(invalid_value)]
        #[allow(clippy::uninit_assumed_init)]
        let params = ucp_ep_params {
            field_mask: ucp_ep_params_field::UCP_EP_PARAM_FIELD_CONN_REQUEST.0 as u64,
            conn_request: connection.handle,
            ..unsafe { MaybeUninit::uninit().assume_init() }
        };
        let endpoint = Endpoint::create(worker, params)?;

        // Workaround for UCX bug: https://github.com/openucx/ucx/issues/6872
        let mut buf = [MaybeUninit::<u8>::uninit(); 4];
        endpoint.stream_recv(buf.as_mut()).await?;

        Ok(endpoint)
    }

    /// Whether the endpoint is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Get the endpoint status.
    pub fn get_status(&self) -> Result<(), Error> {
        self.inner.check()
    }

    #[inline]
    fn get_handle(&self) -> Result<ucp_ep_h, Error> {
        self.inner.check()?;
        Ok(self.handle)
    }

    /// Print endpoint information to stderr.
    pub fn print_to_stderr(&self) {
        if !self.inner.is_closed() {
            unsafe { ucp_ep_print_info(self.handle, stderr) };
        }
    }

    /// This routine flushes all outstanding AMO and RMA communications on the endpoint.
    pub async fn flush(&self) -> Result<(), Error> {
        let handle = self.get_handle()?;
        trace!("flush: endpoint={:?}", handle);
        unsafe extern "C" fn callback(request: *mut c_void, _status: ucs_status_t) {
            trace!("flush: complete");
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let status = unsafe { ucp_ep_flush_nb(handle, 0, Some(callback)) };
        if status.is_null() {
            trace!("flush: complete");
            Ok(())
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle {
                ptr: status,
                poll_fn: poll_normal,
            }
            .await
        } else {
            Error::from_ptr(status)
        }
    }

    /// This routine close connection.
    pub async fn close(&self, force: bool) -> Result<(), Error> {
        if force && self.is_closed() {
            return Ok(());
        } else if !force {
            self.get_status()?;
        }

        trace!("close: endpoint={:?}", self.handle);
        let mode = if force {
            ucp_ep_close_mode::UCP_EP_CLOSE_MODE_FORCE as u32
        } else {
            ucp_ep_close_mode::UCP_EP_CLOSE_MODE_FLUSH as u32
        };
        let status = unsafe { ucp_ep_close_nb(self.handle, mode) };
        if status.is_null() {
            trace!("close: complete");
            self.inner.closed();
            Ok(())
        } else if UCS_PTR_IS_PTR(status) {
            let result = loop {
                if let Poll::Ready(result) = unsafe { poll_normal(status) } {
                    unsafe { ucp_request_free(status as _) };
                    break result;
                } else {
                    futures_lite::future::yield_now().await;
                }
            };
            if result.is_ok() {
                self.inner.closed();
            }

            result
        } else {
            // todo: maybe this shouldn't treat as error ...
            let status = UCS_PTR_RAW_STATUS(status);
            warn!("close endpoint get error: {:?}", status);
            Error::from_status(status)
        }
    }

    /// Get the worker of the endpoint.
    pub fn worker(&self) -> &Rc<Worker> {
        &self.inner.worker
    }

    #[allow(unused)]
    #[cfg(test)]
    fn get_rc(&self) -> (usize, usize) {
        (Rc::strong_count(&self.inner), Rc::weak_count(&self.inner))
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        if !self.inner.is_closed() {
            trace!("destroy endpoint={:?}", self.handle);
            let status = unsafe {
                ucp_ep_close_nb(
                    self.handle,
                    ucp_ep_close_mode::UCP_EP_CLOSE_MODE_FORCE as u32,
                )
            };
            let _ = Error::from_ptr(status).map_err(|err| error!("Failed to force close, {}", err));
            self.inner.closed();
        }
    }
}

/// A handle to the request returned from async IO functions.
struct RequestHandle<T> {
    ptr: ucs_status_ptr_t,
    poll_fn: unsafe fn(ucs_status_ptr_t) -> Poll<T>,
}

impl<T> Future for RequestHandle<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        if let ret @ Poll::Ready(_) = unsafe { (self.poll_fn)(self.ptr) } {
            return ret;
        }
        let request = unsafe { &mut *(self.ptr as *mut Request) };
        request.waker.register(cx.waker());
        unsafe { (self.poll_fn)(self.ptr) }
    }
}

impl<T> Drop for RequestHandle<T> {
    fn drop(&mut self) {
        trace!("request free: {:?}", self.ptr);
        unsafe { ucp_request_free(self.ptr as _) };
    }
}

unsafe fn poll_normal(ptr: ucs_status_ptr_t) -> Poll<Result<(), Error>> {
    let status = ucp_request_check_status(ptr as _);
    if status == ucs_status_t::UCS_INPROGRESS {
        Poll::Pending
    } else {
        Poll::Ready(Error::from_status(status))
    }
}
