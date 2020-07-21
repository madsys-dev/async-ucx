use tokio_ucx::ucx::*;

fn main() {
    if let Some(server_addr) = std::env::args().nth(1) {
        println!("client: connect to {:?}", server_addr);
        let config = Config::new();
        let context = Context::new(&config);
        let worker = context.create_worker();
        let endpoint = worker.create_endpoint(server_addr.parse().unwrap());
        println!("connected");
    } else {
        println!("server");
        let config = Config::new();
        let context = Context::new(&config);
        let worker = context.create_worker();
        let listener = worker.create_listener("0.0.0.0:0".parse().unwrap());
        println!("listening on {}", listener.socket_addr());
        loop {
            worker.progress();
        }
    }
}
