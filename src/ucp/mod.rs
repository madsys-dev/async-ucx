//! Unified Communication Protocol (UCP).

use futures::task::AtomicWaker;
use std::ffi::CString;
use std::mem::MaybeUninit;
use std::os::raw::c_void;
use std::ptr::{null, null_mut};
use std::rc::Rc;
use std::sync::Arc;
use ucx1_sys::*;

mod endpoint;
mod listener;
mod worker;

use crate::Error;

pub use self::endpoint::*;
pub use self::listener::*;
pub use self::worker::*;

/// The configuration for UCP application context.
#[derive(Debug)]
pub struct Config {
    handle: *mut ucp_config_t,
}

impl Default for Config {
    fn default() -> Self {
        let mut handle = MaybeUninit::<*mut ucp_config>::uninit();
        let status = unsafe { ucp_config_read(null(), null(), handle.as_mut_ptr()) };
        Error::from_status(status).unwrap();

        Config {
            handle: unsafe { handle.assume_init() },
        }
    }
}

impl Config {
    /// Prints information about the context configuration.
    ///
    /// Including memory domains, transport resources, and other useful
    /// information associated with the context.
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

/// An object that holds a UCP communication instance's global information.
#[derive(Debug)]
pub struct Context {
    handle: ucp_context_h,
}

// Context is thread safe.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    /// Creates and initializes a UCP application context with default configuration.
    pub fn new() -> Result<Arc<Self>, Error> {
        Self::new_with_config(&Config::default())
    }

    /// Creates and initializes a UCP application context with specified configuration.
    pub fn new_with_config(config: &Config) -> Result<Arc<Self>, Error> {
        let features = ucp_feature::UCP_FEATURE_RMA
            | ucp_feature::UCP_FEATURE_TAG
            | ucp_feature::UCP_FEATURE_STREAM
            | ucp_feature::UCP_FEATURE_WAKEUP;
        #[cfg(feature = "am")]
        let features = features | ucp_feature::UCP_FEATURE_AM;

        let params = ucp_params_t {
            field_mask: (ucp_params_field::UCP_PARAM_FIELD_FEATURES
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_SIZE
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_INIT
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_CLEANUP
                | ucp_params_field::UCP_PARAM_FIELD_MT_WORKERS_SHARED)
                .0 as u64,
            features: features.0 as u64,
            request_size: std::mem::size_of::<Request>() as usize,
            request_init: Some(Request::init),
            request_cleanup: Some(Request::cleanup),
            mt_workers_shared: 1,
            ..unsafe { std::mem::zeroed() }
        };
        let mut handle = MaybeUninit::<*mut ucp_context>::uninit();
        let status = unsafe {
            ucp_init_version(
                UCP_API_MAJOR,
                UCP_API_MINOR,
                &params,
                config.handle,
                handle.as_mut_ptr(),
            )
        };
        Error::from_status(status)?;

        Ok(Arc::new(Context {
            handle: unsafe { handle.assume_init() },
        }))
    }

    /// Create a `Worker` object.
    pub fn create_worker(self: &Arc<Self>) -> Result<Rc<Worker>, Error> {
        Worker::new(self)
    }

    /// Prints information about the context configuration.
    ///
    /// Including memory domains, transport resources, and
    /// other useful information associated with the context.
    pub fn print_to_stderr(&self) {
        unsafe { ucp_context_print_info(self.handle, stderr) };
    }

    /// Fetches information about the context.
    pub fn query(&self) -> Result<ucp_context_attr, Error> {
        let mut attr = MaybeUninit::<ucp_context_attr>::uninit();
        let status = unsafe { ucp_context_query(self.handle, attr.as_mut_ptr()) };
        Error::from_status(status)?;
        Ok(unsafe { attr.assume_init() })
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
