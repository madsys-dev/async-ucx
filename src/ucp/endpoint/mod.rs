use super::*;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Poll, Waker};

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
    pub(super) fn new(worker: &Rc<Worker>, addr: SocketAddr) -> Rc<Self> {
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
        Rc::new(Endpoint {
            handle,
            worker: worker.clone(),
        })
    }

    pub fn print_to_stderr(&self) {
        unsafe { ucp_ep_print_info(self.handle, stderr) };
    }

    /// This routine flushes all outstanding AMO and RMA communications on the endpoint.
    pub fn flush(&self) {
        let status = unsafe { ucp_ep_flush(self.handle) };
        assert_eq!(status, ucs_status_t::UCS_OK);
    }

    /// This routine flushes all outstanding AMO and RMA communications on the endpoint.
    pub fn flush_begin(&self) {
        unsafe extern "C" fn callback(request: *mut c_void, _status: ucs_status_t) {
            ucp_request_free(request);
        }
        unsafe { ucp_ep_flush_nb(self.handle, 0, Some(callback)) };
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
pub enum RequestHandle {
    Ready(usize),
    Send(ucs_status_ptr_t, usize),
    Stream(ucs_status_ptr_t),
}

impl RequestHandle {
    fn test(&self) -> Poll<usize> {
        match *self {
            RequestHandle::Ready(len) => Poll::Ready(len),
            RequestHandle::Send(ptr, len) => unsafe {
                let status = ucp_request_check_status(ptr as _);
                if status == ucs_status_t::UCS_INPROGRESS {
                    Poll::Pending
                } else {
                    Poll::Ready(len)
                }
            },
            RequestHandle::Stream(ptr) => unsafe {
                let mut len = MaybeUninit::<usize>::uninit();
                let status = ucp_stream_recv_request_test(ptr as _, len.as_mut_ptr() as _);
                if status == ucs_status_t::UCS_INPROGRESS {
                    Poll::Pending
                } else {
                    Poll::Ready(len.assume_init())
                }
            },
        }
    }

    fn register_waker(&mut self, waker: &Waker) {
        let ptr = *match self {
            RequestHandle::Ready(_) => unreachable!(),
            RequestHandle::Send(ptr, _) => ptr,
            RequestHandle::Stream(ptr) => ptr,
        };
        unsafe { &mut *(ptr as *mut Request) }.waker.register(waker);
    }
}

impl Future for RequestHandle {
    type Output = usize;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        if let Poll::Ready(len) = self.test() {
            return Poll::Ready(len);
        }
        self.register_waker(cx.waker());
        self.test()
    }
}

impl Drop for RequestHandle {
    fn drop(&mut self) {
        let ptr = match *self {
            RequestHandle::Ready(_) => None,
            RequestHandle::Send(ptr, _) => Some(ptr),
            RequestHandle::Stream(ptr) => Some(ptr),
        };
        if let Some(ptr) = ptr {
            trace!("request free: {:?}", ptr);
            unsafe { ucp_request_free(ptr as _) };
        }
    }
}
