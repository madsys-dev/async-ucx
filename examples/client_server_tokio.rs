use std::io::Result;
use tokio::prelude::*;
use tokio_ucx::*;

#[tokio::main]
async fn main() -> Result<()> {
    if let Some(server_addr) = std::env::args().nth(1) {
        println!("client: connect to {:?}", server_addr);
        let mut stream = UcpStream::connect(server_addr.parse().unwrap()).await?;
        let msg = "Hello!";
        println!("send: {:?}", msg);
        stream.write(msg.as_bytes()).await?;
    } else {
        println!("server");
        let listener = UcpListener::bind("0.0.0.0:10000".parse().unwrap()).await?;
        println!("listening on {}", listener.local_addr()?);
        let mut stream = listener.accept().await?;
        println!("accept");

        let mut buf = [0; 10];
        let len = stream.read(&mut buf).await?;
        let msg = std::str::from_utf8(&buf[..len]);
        println!("recv: {:?}", msg);
    }
    Ok(())
}
