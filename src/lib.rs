#![deny(warnings)]

#[macro_use]
extern crate log;

use futures::pin_mut;
use std::future::Future;
use std::mem::MaybeUninit;
use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::Result;
use tokio::prelude::*;
use tokio::stream::Stream;

mod reactor;
pub mod ucp;

/// A UCP stream between a local and a remote socket.
pub struct UcpStream {
    endpoint: ucp::Endpoint,
    read_future: Option<ucp::RequestHandle>,
    write_future: Option<ucp::RequestHandle>,
}

impl UcpStream {
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<UcpStream> {
        let worker = self::reactor::create_worker();
        let addr = addr.to_socket_addrs()?.next().unwrap();
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
        let result = Pin::new(&mut future).poll(cx).map(Ok);
        if result.is_pending() {
            self.read_future = Some(future);
        }
        if let Poll::Ready(Ok(len)) = result {
            trace!("data={:?}", &buf[..len]);
        }
        trace!("poll_read => {:?}", result);
        result
    }

    unsafe fn prepare_uninitialized_buffer(&self, _buf: &mut [MaybeUninit<u8>]) -> bool {
        // override default function, don't zero the buffer.
        true
    }
}

impl AsyncWrite for UcpStream {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        trace!("data={:?}", buf);
        let mut future = self
            .write_future
            .take()
            .unwrap_or_else(|| self.endpoint.stream_send(buf));
        let result = Pin::new(&mut future).poll(cx).map(Ok);
        if result.is_pending() {
            self.write_future = Some(future);
        }
        trace!("poll_write => {:?}", result);
        result
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        trace!("poll_flush");
        Poll::Ready(Ok(()))
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
    pub async fn bind(addr: impl ToSocketAddrs) -> Result<UcpListener> {
        let worker = self::reactor::create_worker();
        let addr = addr.to_socket_addrs()?.next().unwrap();
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

impl Stream for UcpListener {
    type Item = Result<UcpStream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let future = self.listener.accept();
        pin_mut!(future);
        future
            .poll(cx)
            .map(|endpoint| Some(Ok(UcpStream::from(endpoint))))
    }
}
