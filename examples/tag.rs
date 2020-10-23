use std::io::Result;
use tokio::prelude::*;
use tokio_ucx::*;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    const HELLO: &str = "Hello!";
    let long_msg: Vec<u8> = (0..0x1000).map(|x| x as u8).collect();

    if let Some(server_addr) = std::env::args().nth(1) {
        println!("client: connect to {:?}", server_addr);
        let mut stream = UcpStream::connect(server_addr).await?;
        println!("send: {:?}", HELLO);
        stream.endpoint().tag_send(100, HELLO.as_bytes()).await;
        stream.endpoint().tag_send(101, &long_msg).await;
    } else {
        println!("server");
        let listener = UcpListener::bind("0.0.0.0:10000").await?;
        println!("listening on {}", listener.local_addr()?);
        let mut stream = listener.accept().await?;
        println!("accept");

        let mut buf = [0; 0x1005];
        let len = stream.endpoint().tag_recv(100, &mut buf).await;
        let msg = std::str::from_utf8(&buf[..len]).unwrap();
        println!("recv: {:?}", msg);
        assert_eq!(msg, HELLO);

        let len = stream.endpoint().tag_recv(101, &mut buf).await?;
        println!("recv long message, len={}", len);
        assert_eq!(&buf[..len], long_msg.as_slice());
    }
    Ok(())
}
