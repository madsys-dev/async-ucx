use super::*;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;

mod rma;
mod stream;
mod tag;

pub use self::rma::*;
pub use self::stream::*;
pub use self::tag::*;

#[derive(Debug)]
pub struct Endpoint {
    pub(super) handle: ucp_ep_h,
    pub(super) worker: Rc<Worker>,
}

impl Endpoint {
    pub(super) fn connect(worker: &Rc<Worker>, addr: SocketAddr) -> Self {
        let sockaddr = os_socketaddr::OsSocketAddr::from(addr);
        #[allow(invalid_value)]
        let params = ucp_ep_params {
            field_mask: (ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR)
                .0 as u64,
            flags: ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER.0,
            sockaddr: ucs_sock_addr {
                addr: sockaddr.as_ptr() as _,
                addrlen: sockaddr.len(),
            },
            ..unsafe { MaybeUninit::uninit().assume_init() }
        };
        Endpoint::create(worker, params)
    }

    pub(super) fn accept(worker: &Rc<Worker>, connection: ConnectionRequest) -> Self {
        #[allow(invalid_value)]
        let params = ucp_ep_params {
            field_mask: ucp_ep_params_field::UCP_EP_PARAM_FIELD_CONN_REQUEST.0 as u64,
            conn_request: connection.handle,
            ..unsafe { MaybeUninit::uninit().assume_init() }
        };
        Endpoint::create(worker, params)
    }

    fn create(worker: &Rc<Worker>, params: ucp_ep_params) -> Self {
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

    /// This routine flushes all outstanding AMO and RMA communications on the endpoint.
    pub async fn flush(&self) {
        trace!("flush: endpoint={:?}", self.handle);
        unsafe extern "C" fn callback(request: *mut c_void, _status: ucs_status_t) {
            trace!("flush: complete");
            ucp_request_free(request);
        }
        let status = unsafe { ucp_ep_flush_nb(self.handle, 0, Some(callback)) };
        if status.is_null() {
            trace!("flush: complete");
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle {
                ptr: status,
                poll_fn: poll_normal,
            }
            .await;
        } else {
            panic!("failed to flush endpoint: {:?}", UCS_PTR_RAW_STATUS(status));
        }
    }

    /// This routine releases the endpoint.
    pub async fn close(self) {
        trace!("close: endpoint={:?}", self.handle);
        let status = unsafe {
            ucp_ep_close_nb(
                self.handle,
                ucp_ep_close_mode::UCP_EP_CLOSE_MODE_FLUSH as u32,
            )
        };
        if status.is_null() {
            trace!("close: complete");
        } else if UCS_PTR_IS_PTR(status) {
            while unsafe { poll_normal(status) }.is_pending() {
                futures_lite::future::yield_now().await;
            }
        } else {
            panic!("failed to close endpoint: {:?}", UCS_PTR_RAW_STATUS(status));
        }
        std::mem::forget(self);
    }

    pub fn worker(&self) -> &Rc<Worker> {
        &self.worker
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        trace!("destroy endpoint={:?}", self.handle);
        unsafe { ucp_ep_destroy(self.handle) }
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

unsafe fn poll_normal(ptr: ucs_status_ptr_t) -> Poll<()> {
    let status = ucp_request_check_status(ptr as _);
    if status == ucs_status_t::UCS_INPROGRESS {
        Poll::Pending
    } else {
        Poll::Ready(())
    }
}
