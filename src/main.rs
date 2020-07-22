use tokio_ucx::ucx::*;

#[tokio::main]
async fn main() {
    if let Some(server_addr) = std::env::args().nth(1) {
        println!("client: connect to {:?}", server_addr);
        let config = Config::new();
        let context = Context::new(&config);
        let worker = context.create_worker();
        let endpoint = worker.create_endpoint(server_addr.parse().unwrap());
        endpoint.print_to_stderr();
        std::thread::spawn(move || loop {
            worker.progress();
        });

        endpoint.stream_send(b"Hello!").await;
    } else {
        println!("server");
        let config = Config::new();
        let context = Context::new(&config);
        let worker = context.create_worker();
        let listener = worker.create_listener("0.0.0.0:0".parse().unwrap());
        println!("listening on {}", listener.socket_addr());
        while worker.progress() == 0 {}
        let endpoint = listener.accept().unwrap();
        endpoint.print_to_stderr();
        std::thread::spawn(move || loop {
            worker.progress();
        });

        let mut buf = [0; 10];
        let len = endpoint.stream_recv(&mut buf).await;
        let msg = std::str::from_utf8(&buf[..len]);
        println!("recv: {:?}", msg);
    }
}
