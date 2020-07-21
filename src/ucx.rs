use std::ffi::CString;
use std::mem::MaybeUninit;
use std::ptr::{null, null_mut};
use ucx_sys::*;

#[derive(Debug)]
pub struct Config {
    handle: *mut ucp_config_t,
}

impl Config {
    pub fn new() -> Self {
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_config_read(null(), null(), handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let handle = unsafe { handle.assume_init() };
        Config { handle }
    }

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
pub struct UcpParams {
    inner: ucp_params_t,
}

impl Default for UcpParams {
    fn default() -> Self {
        let inner = ucp_params_t {
            field_mask: ucp_params_field::UCP_PARAM_FIELD_FEATURES.0 as u64,
            features: (ucp_feature::UCP_FEATURE_STREAM | ucp_feature::UCP_FEATURE_WAKEUP).0 as u64,
            request_size: 0,
            request_init: None,
            request_cleanup: None,
            tag_sender_mask: 0,
            mt_workers_shared: 0,
            estimated_num_eps: 0,
        };
        UcpParams { inner }
    }
}

#[derive(Debug)]
pub struct Context {
    handle: ucp_context_h,
}

impl Context {
    pub fn new(params: &UcpParams, config: &Config) -> Self {
        let mut handle = MaybeUninit::uninit();
        let status = unsafe {
            ucp_init_version(
                UCP_API_MAJOR,
                UCP_API_MINOR,
                &params.inner,
                config.handle,
                handle.as_mut_ptr(),
            )
        };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let handle = unsafe { handle.assume_init() };
        Context { handle }
    }

    pub fn create_worker(&self, params: &WorkerParams) -> Worker<'_> {
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_worker_create(self.handle, &params.inner, handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let handle = unsafe { handle.assume_init() };
        Worker {
            handle,
            context: self,
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe { ucp_cleanup(self.handle) };
    }
}

#[derive(Debug)]
pub struct WorkerParams {
    inner: ucp_worker_params_t,
}

impl Default for WorkerParams {
    fn default() -> Self {
        let inner = ucp_worker_params_t {
            field_mask: ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE.0 as u64,
            thread_mode: ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE,
            cpu_mask: ucs_cpu_set_t { ucs_bits: [0; 16] },
            events: 0,
            event_fd: 0,
            user_data: null_mut(),
        };
        WorkerParams { inner }
    }
}

#[derive(Debug)]
pub struct Worker<'a> {
    handle: ucp_worker_h,
    context: &'a Context,
}

impl<'a> Drop for Worker<'a> {
    fn drop(&mut self) {
        unsafe { ucp_worker_destroy(self.handle) }
    }
}

impl<'a> Worker<'a> {
    fn print_to_stderr(&self) {
        unsafe { ucp_worker_print_info(self.handle, stderr) };
    }

    fn get_address(&self) -> WorkerAddress<'_, '_> {
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
}

#[derive(Debug)]
pub struct WorkerAddress<'a, 'b: 'a> {
    handle: *mut ucp_address_t,
    length: usize,
    worker: &'b Worker<'a>,
}

impl<'a, 'b: 'a> Drop for WorkerAddress<'a, 'b> {
    fn drop(&mut self) {
        unsafe { ucp_worker_release_address(self.worker.handle, self.handle) }
    }
}

extern "C" {
    static stderr: *mut FILE;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        let config = Config::new();
        let context = Context::new(&UcpParams::default(), &config);
        let worker = context.create_worker(&WorkerParams::default());
    }
}
