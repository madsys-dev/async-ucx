use std::ffi::CString;
use std::mem::MaybeUninit;
use std::ptr::null;
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
        extern "C" {
            static stderr: *mut FILE;
        }
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

impl UcpParams {
    pub fn new() -> Self {
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
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe { ucp_cleanup(self.handle) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        let config = Config::new();
        config.print_to_stderr();
        let params = UcpParams::new();
        let context = Context::new(&params, &config);
    }
}
