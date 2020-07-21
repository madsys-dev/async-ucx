use std::ffi::CString;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
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
        Config {
            handle: unsafe { handle.assume_init() },
        }
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
pub struct Context {
    handle: ucp_context_h,
}

impl Context {
    pub fn new(config: &Config) -> Self {
        let params = ucp_params_t {
            field_mask: ucp_params_field::UCP_PARAM_FIELD_FEATURES.0 as u64,
            features: (ucp_feature::UCP_FEATURE_STREAM | ucp_feature::UCP_FEATURE_WAKEUP).0 as u64,
            request_size: 0,
            request_init: None,
            request_cleanup: None,
            tag_sender_mask: 0,
            mt_workers_shared: 0,
            estimated_num_eps: 0,
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
        Context {
            handle: unsafe { handle.assume_init() },
        }
    }

    pub fn create_worker(&self) -> Worker<'_> {
        let params = ucp_worker_params_t {
            field_mask: ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE.0 as u64,
            thread_mode: ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE,
            cpu_mask: ucs_cpu_set_t { ucs_bits: [0; 16] },
            events: 0,
            event_fd: 0,
            user_data: null_mut(),
        };
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_worker_create(self.handle, &params, handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        Worker {
            handle: unsafe { handle.assume_init() },
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

    fn address(&self) -> WorkerAddress<'_, '_> {
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

    fn create_listener(&self, addr: SocketAddr) -> Listener {
        unsafe extern "C" fn accept_handler(ep: ucp_ep_h, arg: *mut ::std::os::raw::c_void) {
            println!("accept");
        }
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
                arg: null_mut(),
            },
            conn_handler: ucp_listener_conn_handler_t {
                cb: None,
                arg: null_mut(),
            },
        };
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_listener_create(self.handle, &params, handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        Listener {
            handle: unsafe { handle.assume_init() },
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

#[derive(Debug)]
struct Listener<'a, 'b: 'a> {
    handle: ucp_listener_h,
    worker: &'b Worker<'a>,
}

impl<'a, 'b: 'a> Drop for Listener<'a, 'b> {
    fn drop(&mut self) {
        unsafe { ucp_listener_destroy(self.handle) }
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
        let context = Context::new(&config);
        let worker = context.create_worker();
        let listener = worker.create_listener("0.0.0.0:0".parse().unwrap());
        println!("{:?}", worker.address());
    }
}
