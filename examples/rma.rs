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
    let context = Context::new().unwrap();
    let worker = context.create_worker().unwrap();
    #[cfg(not(feature = "event"))]
    tokio::task::spawn_local(worker.clone().polling());
    #[cfg(feature = "event")]
    tokio::task::spawn_local(worker.clone().event_poll());

    let endpoint = worker
        .connect_socket(server_addr.parse().unwrap())
        .await
        .unwrap();
    endpoint.print_to_stderr();
    // register memory region
    let mut buf: Vec<u8> = (0..0x1000).map(|x| x as u8).collect();
    let mem = MemoryHandle::register(&context, &mut buf);
    endpoint
        .stream_send(&(buf.as_ptr() as u64).to_ne_bytes())
        .await
        .unwrap();
    endpoint.stream_send(mem.pack().as_ref()).await.unwrap();
    println!("send memory handle");

    endpoint
        .stream_recv(&mut [MaybeUninit::uninit(); 1])
        .await
        .unwrap();
    Ok(())
}

async fn server() -> Result<()> {
    println!("server");
    let context = Context::new().unwrap();
    let worker = context.create_worker().unwrap();
    let mut listener = worker
        .create_listener("0.0.0.0:10000".parse().unwrap())
        .unwrap();
    #[cfg(not(feature = "event"))]
    tokio::task::spawn_local(worker.clone().polling());
    #[cfg(feature = "event")]
    tokio::task::spawn_local(worker.clone().event_poll());
    println!("listening on {}", listener.socket_addr().unwrap());
    let connection = listener.next().await;
    let endpoint = worker.accept(connection).await.unwrap();
    println!("accept");
    endpoint.print_to_stderr();

    let mut vaddr_buf = [MaybeUninit::<u8>::uninit(); 8];
    let len = endpoint.stream_recv(&mut vaddr_buf).await.unwrap();
    assert_eq!(len, 8);
    let vaddr = u64::from_ne_bytes(unsafe { transmute(vaddr_buf) });
    println!("recv: vaddr={:#x}", vaddr);

    let mut rkey_buf = [MaybeUninit::<u8>::uninit(); 100];
    let len = endpoint.stream_recv(&mut rkey_buf).await.unwrap();
    println!("recv rkey: len={}", len);

    let rkey = RKey::unpack(&endpoint, unsafe { transmute(&rkey_buf[..len]) });
    let mut buf = vec![0; 0x1000];
    endpoint.get(&mut buf, vaddr, &rkey).await.unwrap();
    println!("get remote memory");

    let expected: Vec<u8> = (0..0x1000).map(|x| x as u8).collect();
    assert_eq!(&buf, &expected);

    endpoint.stream_send(&[0; 1]).await.unwrap();
    Ok(())
}
