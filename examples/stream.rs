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

async fn client(server_addr: String) -> Result<()> {
    println!("client: connect to {:?}", server_addr);
    let context = Context::new();
    let worker = context.create_worker();
    tokio::task::spawn_local(worker.clone().polling());

    let endpoint = worker.connect(server_addr.parse().unwrap());
    endpoint.print_to_stderr();

    endpoint.stream_send(b"Hello!").await;
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
    let endpoint = worker.accept(connection);
    println!("accept");
    endpoint.print_to_stderr();

    let mut buf = [MaybeUninit::uninit(); 10];
    let len = endpoint.stream_recv(&mut buf).await;
    let msg = std::str::from_utf8(unsafe { transmute(&buf[..len]) });
    println!("recv: {:?}", msg);
    Ok(())
}
