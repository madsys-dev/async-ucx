#![deny(warnings)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::Result;
use tokio::prelude::*;

mod reactor;
pub mod ucp;

/// A UCP stream between a local and a remote socket.
pub struct UcpStream {
    endpoint: ucp::Endpoint,
    read_future: Option<ucp::RequestHandle>,
    write_future: Option<ucp::RequestHandle>,
}

impl UcpStream {
    pub async fn connect(addr: SocketAddr) -> Result<UcpStream> {
        let worker = self::reactor::create_worker();
        let endpoint = worker.create_endpoint(addr);
        Ok(UcpStream::from(endpoint))
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        todo!()
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        todo!()
    }

    fn from(endpoint: ucp::Endpoint) -> Self {
        UcpStream {
            endpoint,
            read_future: None,
            write_future: None,
        }
    }
}

impl AsyncRead for UcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let mut future = self
            .read_future
            .take()
            .unwrap_or_else(|| self.endpoint.stream_recv(buf));
        let result = Pin::new(&mut future).poll(cx).map(|len| Ok(len));
        if result.is_pending() {
            self.read_future = Some(future);
        }
        result
    }
}

impl AsyncWrite for UcpStream {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let mut future = self
            .write_future
            .take()
            .unwrap_or_else(|| self.endpoint.stream_send(buf));
        let result = Pin::new(&mut future).poll(cx).map(|len| Ok(len));
        if result.is_pending() {
            self.write_future = Some(future);
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        todo!()
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        todo!()
    }
}

/// A UCP server, listening for connections.
pub struct UcpListener {
    listener: Arc<ucp::Listener>,
}

impl UcpListener {
    pub async fn bind(addr: SocketAddr) -> Result<UcpListener> {
        let worker = self::reactor::create_worker();
        let listener = worker.create_listener(addr);
        Ok(UcpListener { listener })
    }

    pub async fn accept(&self) -> Result<UcpStream> {
        let endpoint = self.listener.accept().await;
        Ok(UcpStream::from(endpoint))
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.socket_addr())
    }
}
