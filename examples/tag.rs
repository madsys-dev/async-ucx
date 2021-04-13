use async_ucx::ucp::*;
use std::io::Result;
use std::mem::{transmute, MaybeUninit};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let local = tokio::task::LocalSet::new();
    if let Some(server_addr) = std::env::args().nth(1) {
        local.run_until(client(server_addr)).await?;
    } else {
        local.run_until(server()).await?;
    }
    Ok(())
}

const HELLO: &str = "Hello!";

async fn client(server_addr: String) -> Result<()> {
    println!("client: connect to {:?}", server_addr);
    let context = Context::new();
    let worker = context.create_worker();
    tokio::task::spawn_local(worker.clone().polling());

    let endpoint = worker.connect(server_addr.parse().unwrap());

    println!("send: {:?}", HELLO);
    endpoint.tag_send(100, HELLO.as_bytes()).await;

    let long_msg: Vec<u8> = (0..0x1000).map(|x| x as u8).collect();
    endpoint.tag_send(101, &long_msg).await;
    Ok(())
}

async fn server() -> Result<()> {
    println!("server");
    let context = Context::new();
    let worker = context.create_worker();
    tokio::task::spawn_local(worker.clone().polling());

    let listener = worker.create_listener("0.0.0.0:10000".parse().unwrap());
    println!("listening on {}", listener.socket_addr());
    let connection = listener.next().await;
    let _endpoint = worker.accept(connection);
    println!("accept");

    let mut buf = [MaybeUninit::uninit(); 0x1005];
    let len = worker.tag_recv(100, &mut buf).await;
    let msg = std::str::from_utf8(unsafe { transmute(&buf[..len]) }).unwrap();
    println!("recv: {:?}", msg);
    assert_eq!(msg, HELLO);

    let len = worker.tag_recv(101, &mut buf).await;
    println!("recv long message, len={}", len);
    let msg: &[u8] = unsafe { transmute(&buf[..len]) };
    let long_msg: Vec<u8> = (0..0x1000).map(|x| x as u8).collect();
    assert_eq!(msg, long_msg.as_slice());
    Ok(())
}
