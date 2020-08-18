use tokio_ucx::ucp::*;

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Some(server_addr) = std::env::args().nth(1) {
        println!("client: connect to {:?}", server_addr);
        let config = Config::default();
        let context = Context::new(&config);
        let worker = context.create_worker();
        let endpoint = worker.create_endpoint(server_addr.parse().unwrap());
        endpoint.print_to_stderr();
        std::thread::spawn(move || loop {
            // worker.wait();
            worker.progress();
        });

        // register memory region
        let mut buf: Vec<u8> = (0..0x1000).map(|x| x as u8).collect();
        let mem = MemoryHandle::register(&context, &mut buf);
        endpoint
            .stream_send(&(buf.as_ptr() as u64).to_ne_bytes())
            .await;
        endpoint.stream_send(mem.pack().as_ref()).await;
        println!("send memory handle");

        endpoint.stream_recv(&mut [0; 1]).await;
    } else {
        println!("server");
        let config = Config::default();
        let context = Context::new(&config);
        let worker = context.create_worker();
        let listener = worker.create_listener("0.0.0.0:10000".parse().unwrap());
        std::thread::spawn(move || loop {
            // worker.wait();
            worker.progress();
        });
        println!("listening on {}", listener.socket_addr());
        let mut endpoint = listener.accept().await;
        println!("accept");
        endpoint.print_to_stderr();

        let mut vaddr_buf = [0; 8];
        let len = endpoint.stream_recv(&mut vaddr_buf).await;
        assert_eq!(len, 8);
        let vaddr = u64::from_ne_bytes(vaddr_buf);
        println!("recv: vaddr={:#x}", vaddr);

        let mut rkey_buf = [0; 100];
        let len = endpoint.stream_recv(&mut rkey_buf).await;
        println!("recv rkey: len={}", len);

        let rkey = RKey::unpack(&endpoint, &rkey_buf[..len]);
        let mut buf = vec![0; 0x1000];
        endpoint.get(&mut buf, vaddr, &rkey).await;
        println!("get remote memory");

        let expected: Vec<u8> = (0..0x1000).map(|x| x as u8).collect();
        assert_eq!(&buf, &expected);

        endpoint.stream_send(&[0; 1]).await;
    }
}
