use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::Result;

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

    // TODO: how to impl AsyncRead, AsyncWrite for UcpStream?

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let len = self.endpoint.stream_recv(buf).await;
        Ok(len)
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.endpoint.stream_send(buf).await;
        Ok(buf.len())
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        todo!()
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
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

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.socket_addr())
    }
}
