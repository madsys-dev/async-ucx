use async_ucx::ucp::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Result;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::sync::atomic::*;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let local = tokio::task::LocalSet::new();
    if let Some(server_addr) = std::env::args().nth(1) {
        local.run_until(client(server_addr)).await;
    } else {
        local.run_until(server()).await;
    }
    Ok(())
}

async fn client(server_addr: String) -> ! {
    println!("client: connect to {:?}", server_addr);

    let context = Context::new();
    let worker = context.create_worker();
    tokio::task::spawn_local(worker.clone().polling());

    let endpoint = worker.connect(server_addr.parse().unwrap());
    endpoint.print_to_stderr();

    let mut tag = [MaybeUninit::uninit(); 8];
    endpoint.worker().tag_recv(100, &mut tag).await;
    let tag: u64 = unsafe { std::mem::transmute(tag) };
    println!("client: got tag {:#x}", tag);

    let long_msg: Vec<u8> = (0..8).map(|x| x as u8).collect();
    loop {
        endpoint.tag_send(tag, &long_msg).await;
        for _ in 0..500 {
            std::hint::spin_loop();
        }
        // endpoint
        //     .worker()
        //     .tag_recv(tag, &mut [MaybeUninit::uninit()])
        //     .await;
    }
}

async fn server() -> ! {
    println!("server");
    let context = Context::new();
    let mut worker_threads = vec![];
    let mut counters = vec![];
    for _ in 0..4 {
        let wt = WorkerThread::new(&context, |ep, addr, counter| {
            println!("accept: {:?}", addr);
            tokio::task::spawn_local(async move {
                let mut hasher = DefaultHasher::new();
                addr.hash(&mut hasher);
                let tag = hasher.finish();
                ep.tag_send(100, &tag.to_ne_bytes()).await;

                let mut buf = vec![MaybeUninit::uninit(); 50000];
                loop {
                    ep.worker().tag_recv(tag, &mut buf).await;
                    // ep.tag_send(tag, &[0]).await;
                    unsafe { *(&*counter as *const AtomicUsize as *mut usize) += 1 };
                }
            });
        });
        counters.push(wt.counter.clone());
        worker_threads.push(wt);
    }

    let worker = context.create_worker();
    tokio::task::spawn_local(worker.clone().polling());

    let listener = worker.create_listener("0.0.0.0:0".parse().unwrap());
    tokio::task::spawn_local(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let count: usize = counters.iter().map(|c| c.swap(0, Ordering::SeqCst)).sum();
            println!("{} IOPS", count);
        }
    });
    println!("listening on {}", listener.socket_addr());

    for i in 0.. {
        let conn = listener.next().await;
        let n = worker_threads.len();
        worker_threads[i % n].accept(conn);
    }
    unreachable!()
}

struct WorkerThread {
    sender: mpsc::UnboundedSender<ConnectionRequest>,
    counter: Arc<AtomicUsize>,
}

impl WorkerThread {
    fn new(context: &Arc<Context>, handle_ep: fn(Endpoint, SocketAddr, Arc<AtomicUsize>)) -> Self {
        let context = context.clone();
        let (sender, mut recver) = mpsc::unbounded_channel::<ConnectionRequest>();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter1 = counter.clone();
        std::thread::spawn(move || {
            let worker = context.create_worker();
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            local.spawn_local(worker.clone().polling());
            local.block_on(&rt, async move {
                while let Some(conn) = recver.recv().await {
                    let addr = conn.remote_addr();
                    let ep = worker.accept(conn);
                    handle_ep(ep, addr, counter1.clone());
                }
            });
        });
        WorkerThread { sender, counter }
    }

    fn accept(&mut self, conn: ConnectionRequest) {
        self.sender.send(conn).unwrap();
    }
}
