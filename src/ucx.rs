use std::collections::VecDeque;
use std::ffi::CString;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::SocketAddr;
use std::os::raw::c_void;
use std::ptr::{null, null_mut};
use std::sync::{Arc, Mutex};
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

unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn new(config: &Config) -> Arc<Self> {
        let params = ucp_params_t {
            field_mask: ucp_params_field::UCP_PARAM_FIELD_FEATURES.0 as u64,
            features: (ucp_feature::UCP_FEATURE_STREAM | ucp_feature::UCP_FEATURE_WAKEUP).0 as u64,
            request_size: 0,
            request_init: None,
            request_cleanup: None,
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
        Arc::new(Worker {
            handle: unsafe { handle.assume_init() },
            context: context.clone(),
        })
    }

    fn print_to_stderr(&self) {
        unsafe { ucp_worker_print_info(self.handle, stderr) };
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

    pub fn progress(&self) -> u32 {
        unsafe { ucp_worker_progress(self.handle) }
    }

    pub fn event_fd(&self) -> i32 {
        let mut fd = MaybeUninit::uninit();
        let status = unsafe { ucp_worker_get_efd(self.handle, fd.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        unsafe { fd.assume_init() }
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
    incomings: Mutex<VecDeque<Endpoint>>,
    worker: Arc<Worker>,
}

impl Listener {
    fn new(worker: &Arc<Worker>, addr: SocketAddr) -> Arc<Self> {
        unsafe extern "C" fn accept_handler(ep: ucp_ep_h, arg: *mut c_void) {
            let listener = ManuallyDrop::new(Arc::from_raw(arg as *const Listener));
            let endpoint = Endpoint {
                handle: ep,
                worker: listener.worker.clone(),
            };
            listener.incomings.lock().unwrap().push_back(endpoint);
        }
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
        let mut attr = ucp_listener_attr_t {
            field_mask: ucp_listener_attr_field::UCP_LISTENER_ATTR_FIELD_SOCKADDR.0 as u64,
            sockaddr: unsafe { MaybeUninit::uninit().assume_init() },
        };
        let status = unsafe { ucp_listener_query(self.handle, &mut attr) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let sockaddr = unsafe {
            os_socketaddr::OsSocketAddr::from_raw_parts(&attr.sockaddr as *const _ as _, 6)
        };
        sockaddr.into_addr().unwrap()
    }

    pub fn accept(&self) -> Option<Endpoint> {
        self.incomings.lock().unwrap().pop_front()
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

impl Endpoint {
    fn new(worker: &Arc<Worker>, addr: SocketAddr) -> Self {
        unsafe extern "C" fn err_handler(
            arg: *mut ::std::os::raw::c_void,
            ep: ucp_ep_h,
            status: ucs_status_t,
        ) {
            println!("err");
        }
        let sockaddr = os_socketaddr::OsSocketAddr::from(addr);
        let params = ucp_ep_params {
            field_mask: (ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLER)
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
                cb: Some(err_handler),
                arg: null_mut(),
            },
            user_data: null_mut(),
            address: null_mut(),
            conn_request: null_mut(),
        };
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_ep_create(worker.handle, &params, handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        Endpoint {
            handle: unsafe { handle.assume_init() },
            worker: worker.clone(),
        }
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        unsafe { ucp_ep_destroy(self.handle) }
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
        let worker1 = context.create_worker();
        let listener = worker1.create_listener("0.0.0.0:0".parse().unwrap());
        let listen_port = listener.socket_addr().port();

        std::thread::spawn(move || {
            let worker2 = context.create_worker();
            let mut addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            addr.set_port(listen_port);
            let endpoint = worker2.create_endpoint(addr);
        });

        while worker1.progress() == 0 {}
        let endpoint = listener.accept().unwrap();
    }
}
