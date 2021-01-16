use super::*;
use futures::future::poll_fn;
use std::collections::VecDeque;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::SocketAddr;
use std::sync::Mutex;
use std::task::{Poll, Waker};

#[derive(Debug)]
pub struct Listener {
    handle: ucp_listener_h,
    incomings: Mutex<Queue>,
    worker: Rc<Worker>,
}

#[derive(Debug, Default)]
struct Queue {
    items: VecDeque<Rc<Endpoint>>,
    wakers: Vec<Waker>,
}

impl Listener {
    pub(super) fn new(worker: &Rc<Worker>, addr: SocketAddr) -> Rc<Self> {
        unsafe extern "C" fn accept_handler(ep: ucp_ep_h, arg: *mut c_void) {
            trace!("accept endpoint={:?}", ep);
            let listener = ManuallyDrop::new(Rc::from_raw(arg as *const Listener));
            let endpoint = Rc::new(Endpoint {
                handle: ep,
                worker: listener.worker.clone(),
            });
            let mut incomings = listener.incomings.lock().unwrap();
            incomings.items.push_back(endpoint);
            for waker in incomings.wakers.drain(..) {
                waker.wake();
            }
        }
        #[allow(clippy::uninit_assumed_init)]
        let mut listener = Rc::new(Listener {
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
        let handle = &mut Rc::get_mut(&mut listener).unwrap().handle;
        let status = unsafe { ucp_listener_create(worker.handle, &params, handle) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        trace!("create listener={:?}", handle);
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

    pub async fn accept(&self) -> Rc<Endpoint> {
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
        trace!("destroy listener={:?}", self.handle);
        unsafe { ucp_listener_destroy(self.handle) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept() {
        env_logger::init();
        let (sender, recver) = tokio::sync::oneshot::channel();
        let f1 = spawn_thread!(async move {
            let context = Context::new(&Config::default());
            let worker1 = context.create_worker();
            tokio::task::spawn_local(worker1.clone().polling());
            let listener = worker1.create_listener("0.0.0.0:0".parse().unwrap());
            let listen_port = listener.socket_addr().port();
            sender.send(listen_port).unwrap();
            let _endpoint1 = listener.accept().await;
        });
        spawn_thread!(async move {
            let context = Context::new(&Config::default());
            let worker2 = context.create_worker();
            let mut addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let listen_port = recver.await.unwrap();
            addr.set_port(listen_port);
            let _endpoint2 = worker2.create_endpoint(addr);
        });
        f1.join().unwrap();
    }
}
