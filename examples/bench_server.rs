use std::io::Result;
use std::sync::atomic::*;
use tokio_ucx::ucp::*;

#[tokio::main(core_threads = 1)]
async fn main() -> Result<()> {
    env_logger::init();
    let server_addr = std::env::args().nth(1).unwrap();

    println!("server");
    let context = Context::new(&Config::default());
    let worker = context.create_worker();
    let listener = worker.create_listener(server_addr.parse().unwrap());
    tokio::spawn(async move {
        loop {
            while worker.progress() != 0 {}
            tokio::task::yield_now().await;
        }
    });
    tokio::spawn(async {
        loop {
            tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
            let count = COUNT.swap(0, Ordering::SeqCst);
            println!("{} IOPS", count);
        }
    });
    println!("listening on {}", listener.socket_addr());
    for i in 0u8.. {
        let ep = listener.accept().await;
        println!("accept {}", i);
        ep.tag_send(100, &[i]).await;
        tokio::spawn(async move {
            let tag = i as u64 + 200;
            let mut buf = vec![0; 50000];
            loop {
                ep.worker().tag_recv(tag, &mut buf).await;
                ep.tag_send(tag, &[0]).await;
                COUNT.fetch_add(1, Ordering::SeqCst);
            }
        });
    }
    Ok(())
}

static COUNT: AtomicUsize = AtomicUsize::new(0);
