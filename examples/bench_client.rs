use std::io::Result;
use tokio::prelude::*;
use tokio_ucx::ucp::*;

#[tokio::main(core_threads = 1)]
async fn main() -> Result<()> {
    env_logger::init();
    let server_addr = std::env::args().nth(1).unwrap();
    println!("client: connect to {:?}", server_addr);

    let context = Context::new(&Config::default());
    let worker = context.create_worker();
    let endpoint = worker.create_endpoint(server_addr.parse().unwrap());
    endpoint.print_to_stderr();

    tokio::spawn(async move {
        loop {
            while worker.progress() != 0 {}
            tokio::task::yield_now().await;
        }
    });

    let mut id = [0u8];
    endpoint.tag_recv(100, &mut id).await;
    let tag = id[0] as u64 + 200;
    println!("client: got tag {:?}", tag);

    let long_msg: Vec<u8> = (0..47008).map(|x| x as u8).collect();
    loop {
        endpoint.tag_send(tag, &long_msg).await;
        endpoint.tag_recv(tag, &mut [0u8]).await;
    }
}
