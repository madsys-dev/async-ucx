#![deny(warnings)]

#[macro_use]
extern crate log;

#[cfg(test)]
macro_rules! spawn_thread {
    ($future:expr) => {
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, $future);
            println!("after block!");
        })
    };
}

pub mod ucp;
