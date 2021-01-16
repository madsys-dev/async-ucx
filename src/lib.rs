#![deny(warnings)]

#[macro_use]
extern crate log;

macro_rules! spawn_thread {
    ($future:expr) => {
        std::thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, $future);
        })
    };
}

pub mod ucp;
