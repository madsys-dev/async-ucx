use crate::Error;

use super::*;
use futures::channel::mpsc;
use futures::stream::StreamExt;
use std::mem::MaybeUninit;
use std::net::SocketAddr;

/// Listening on a specific address and accepting connections from clients.
#[derive(Debug)]
pub struct Listener {
    handle: ucp_listener_h,
    #[allow(unused)]
    sender: Rc<mpsc::UnboundedSender<ConnectionRequest>>,
    recver: mpsc::UnboundedReceiver<ConnectionRequest>,
}

/// An incoming connection request.
///
/// The request must be explicitly accepted by [Worker::accept] or rejected by [Listener::reject].
#[derive(Debug)]
#[must_use = "connection must be accepted or rejected"]
pub struct ConnectionRequest {
    pub(super) handle: ucp_conn_request_h,
}

// connection can be send to other thread and accepted on its worker
unsafe impl Send for ConnectionRequest {}

impl ConnectionRequest {
    /// The address of the remote client that sent the connection request to the server.
    pub fn remote_addr(&self) -> Result<SocketAddr, Error> {
        let mut attr = ucp_conn_request_attr {
            field_mask: ucp_conn_request_attr_field::UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR.0
                as u64,
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };
        let status = unsafe { ucp_conn_request_query(self.handle, &mut attr) };
        Error::from_status(status)?;

        let sockaddr =
            unsafe { socket2::SockAddr::new(std::mem::transmute(attr.client_address), 8) };
        Ok(sockaddr.as_socket().unwrap())
    }
}

impl Listener {
    pub(super) fn new(worker: &Rc<Worker>, addr: SocketAddr) -> Result<Self, Error> {
        unsafe extern "C" fn connect_handler(conn_request: ucp_conn_request_h, arg: *mut c_void) {
            trace!("connect request={:?}", conn_request);
            let sender = &*(arg as *const mpsc::UnboundedSender<ConnectionRequest>);
            let connection = ConnectionRequest {
                handle: conn_request,
            };
            sender.unbounded_send(connection).unwrap();
        }
        let (sender, recver) = mpsc::unbounded();
        let sender = Rc::new(sender);
        let sockaddr = socket2::SockAddr::from(addr);
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
                arg: &*sender as *const mpsc::UnboundedSender<ConnectionRequest> as _,
            },
        };
        let mut handle = MaybeUninit::<*mut ucp_listener>::uninit();
        let status = unsafe { ucp_listener_create(worker.handle, &params, handle.as_mut_ptr()) };
        Error::from_status(status)?;
        trace!("create listener={:?}", handle);
        Ok(Listener {
            handle: unsafe { handle.assume_init() },
            sender,
            recver,
        })
    }

    /// Returns the local socket address of this listener.
    pub fn socket_addr(&self) -> Result<SocketAddr, Error> {
        let mut attr = ucp_listener_attr_t {
            field_mask: ucp_listener_attr_field::UCP_LISTENER_ATTR_FIELD_SOCKADDR.0 as u64,
            sockaddr: unsafe { MaybeUninit::zeroed().assume_init() },
        };
        let status = unsafe { ucp_listener_query(self.handle, &mut attr) };
        Error::from_status(status)?;
        let sockaddr = unsafe { socket2::SockAddr::new(std::mem::transmute(attr.sockaddr), 8) };

        Ok(sockaddr.as_socket().unwrap())
    }

    /// Waiting for the next connection request.
    pub async fn next(&mut self) -> ConnectionRequest {
        self.recver.next().await.unwrap()
    }

    /// Reject a connection.
    pub fn reject(&self, conn: ConnectionRequest) -> Result<(), Error> {
        let status = unsafe { ucp_listener_reject(self.handle, conn.handle) };
        Error::from_status(status)
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

    #[test_log::test]
    fn accept() {
        let (sender, recver) = tokio::sync::oneshot::channel();
        let f1 = spawn_thread!(async move {
            let context = Context::new().unwrap();
            let worker = context.create_worker().unwrap();
            tokio::task::spawn_local(worker.clone().polling());
            let mut listener = worker
                .create_listener("0.0.0.0:0".parse().unwrap())
                .unwrap();
            let listen_port = listener.socket_addr().unwrap().port();
            sender.send(listen_port).unwrap();
            let conn = listener.next().await;
            let _endpoint = worker.accept(conn).await.unwrap();
        });
        spawn_thread!(async move {
            let context = Context::new().unwrap();
            let worker = context.create_worker().unwrap();
            tokio::task::spawn_local(worker.clone().polling());
            let mut addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let listen_port = recver.await.unwrap();
            addr.set_port(listen_port);
            let _endpoint = worker.connect_socket(addr).await.unwrap();
        });
        f1.join().unwrap();
    }
}
