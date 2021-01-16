//! Mid-level bindings for UCP.

use futures::task::AtomicWaker;
use std::ffi::CString;
use std::mem::MaybeUninit;
use std::os::raw::c_void;
use std::ptr::{null, null_mut};
use std::rc::Rc;
use ucx_sys::*;

mod endpoint;
mod listener;
mod worker;

pub use self::endpoint::*;
pub use self::listener::*;
pub use self::worker::*;

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

impl Context {
    pub fn new(config: &Config) -> Rc<Self> {
        let params = ucp_params_t {
            field_mask: (ucp_params_field::UCP_PARAM_FIELD_FEATURES
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_SIZE
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_INIT
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_CLEANUP)
                .0 as u64,
            features: (ucp_feature::UCP_FEATURE_RMA
                | ucp_feature::UCP_FEATURE_TAG
                | ucp_feature::UCP_FEATURE_STREAM
                | ucp_feature::UCP_FEATURE_WAKEUP)
                .0 as u64,
            request_size: std::mem::size_of::<Request>() as u64,
            request_init: Some(Request::init),
            request_cleanup: Some(Request::cleanup),
            tag_sender_mask: 0,
            mt_workers_shared: 0,
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
        Rc::new(Context {
            handle: unsafe { handle.assume_init() },
        })
    }

    pub fn create_worker(self: &Rc<Self>) -> Rc<Worker> {
        Worker::new(self)
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe { ucp_cleanup(self.handle) };
    }
}

extern "C" {
    static stderr: *mut FILE;
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
struct Request {
    waker: AtomicWaker,
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
