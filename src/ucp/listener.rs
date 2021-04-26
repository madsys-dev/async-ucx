use super::*;
use futures::future::poll_fn;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Poll, Waker};

#[derive(Debug)]
pub struct Listener {
    handle: ucp_listener_h,
    incomings: Pin<Rc<RefCell<Queue>>>,
}

#[derive(Debug, Default)]
struct Queue {
    items: VecDeque<ConnectionRequest>,
    wakers: Vec<Waker>,
}

#[derive(Debug)]
#[must_use = "connection must be accepted or rejected"]
pub struct ConnectionRequest {
    pub(super) handle: ucp_conn_request_h,
}

// connection can be send to other thread and accepted on its worker
unsafe impl Send for ConnectionRequest {}

impl ConnectionRequest {
    /// The address of the remote client that sent the connection request to the server.
    pub fn remote_addr(&self) -> SocketAddr {
        let mut attr = MaybeUninit::<ucp_conn_request_attr>::uninit();
        unsafe { &mut *attr.as_mut_ptr() }.field_mask =
            ucp_conn_request_attr_field::UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR.0 as u64;
        let status = unsafe { ucp_conn_request_query(self.handle, attr.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let attr = unsafe { attr.assume_init() };
        let sockaddr = unsafe {
            os_socketaddr::OsSocketAddr::from_raw_parts(&attr.client_address as *const _ as _, 8)
        };
        sockaddr.into_addr().unwrap()
    }
}

impl Listener {
    pub(super) fn new(worker: &Rc<Worker>, addr: SocketAddr) -> Self {
        unsafe extern "C" fn connect_handler(conn_request: ucp_conn_request_h, arg: *mut c_void) {
            trace!("connect request={:?}", conn_request);
            let mut incomings = (*(arg as *const RefCell<Queue>)).borrow_mut();
            let connection = ConnectionRequest {
                handle: conn_request,
            };
            incomings.items.push_back(connection);
            for waker in incomings.wakers.drain(..) {
                waker.wake();
            }
        }
        let incomings = Rc::pin(RefCell::new(Queue::default()));
        let sockaddr = os_socketaddr::OsSocketAddr::from(addr);
        let params = ucp_listener_params_t {
            field_mask: (ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_SOCK_ADDR
                | ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_CONN_HANDLER)
                .0 as u64,
            sockaddr: ucs_sock_addr {
                addr: sockaddr.as_ptr() as _,
                addrlen: sockaddr.len(),
            },
            accept_handler: ucp_listener_accept_handler_t {
                cb: None,
                arg: null_mut(),
            },
            conn_handler: ucp_listener_conn_handler_t {
                cb: Some(connect_handler),
                arg: &*incomings as *const RefCell<Queue> as _,
            },
        };
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_listener_create(worker.handle, &params, handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        trace!("create listener={:?}", handle);
        Listener {
            handle: unsafe { handle.assume_init() },
            incomings,
        }
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

    pub async fn next(&self) -> ConnectionRequest {
        poll_fn(|cx| {
            let mut incomings = self.incomings.borrow_mut();
            if let Some(connection) = incomings.items.pop_front() {
                Poll::Ready(connection)
            } else {
                incomings.wakers.push(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    /// Reject a connection.
    pub fn reject(&self, conn: ConnectionRequest) {
        let status = unsafe { ucp_listener_reject(self.handle, conn.handle) };
        assert_eq!(status, ucs_status_t::UCS_OK);
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
            let context = Context::new();
            let worker = context.create_worker();
            tokio::task::spawn_local(worker.clone().polling());
            let listener = worker.create_listener("0.0.0.0:0".parse().unwrap());
            let listen_port = listener.socket_addr().port();
            sender.send(listen_port).unwrap();
            let conn = listener.next().await;
            let _endpoint = worker.accept(conn);
        });
        spawn_thread!(async move {
            let context = Context::new();
            let worker = context.create_worker();
            let mut addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let listen_port = recver.await.unwrap();
            addr.set_port(listen_port);
            let _endpoint = worker.connect(addr);
        });
        f1.join().unwrap();
    }
}
