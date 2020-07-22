use futures::pin_mut;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::Result;
use tokio::prelude::*;

mod reactor;
pub mod ucx;

/// A UCP stream between a local and a remote socket.
pub struct UcpStream {
    endpoint: ucx::Endpoint,
}

impl UcpStream {
    pub async fn connect(addr: SocketAddr) -> Result<UcpStream> {
        let worker = self::reactor::create_worker();
        let endpoint = worker.create_endpoint(addr);
        Ok(UcpStream { endpoint })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        todo!()
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        todo!()
    }
}

impl AsyncRead for UcpStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let future = self.endpoint.stream_recv(buf);
        pin_mut!(future);
        future.poll(cx).map(|len| Ok(len))
    }
}

impl AsyncWrite for UcpStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let future = self.endpoint.stream_send(buf);
        pin_mut!(future);
        future.poll(cx).map(|_| Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        todo!()
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        todo!()
    }
}

/// A UCP server, listening for connections.
pub struct UcpListener {
    listener: Arc<ucx::Listener>,
}

impl UcpListener {
    pub async fn bind(addr: SocketAddr) -> Result<UcpListener> {
        let worker = self::reactor::create_worker();
        let listener = worker.create_listener(addr);
        Ok(UcpListener { listener })
    }

    pub async fn accept(&self) -> Result<UcpStream> {
        let endpoint = self.listener.accept().await;
        Ok(UcpStream { endpoint })
    }
}
