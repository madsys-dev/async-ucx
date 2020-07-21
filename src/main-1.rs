use ucx::ucp::*;
// use ucx_sys::ucp_address_t;

fn main() {
    let config = Configuration::readFromEnvironmentVariables("").unwrap();
    let features = ApplicationContextFeaturesIdeallySupported {
        remoteMemoryAccess: true,
        ..ApplicationContextFeaturesIdeallySupported::default()
    };
    let context = config.initialiseApplicationContext(&features, false, 0, 100).unwrap();
    let worker = context.createWorker(WorkerThreadMode::OnlyEverUsedFromThisThread);
    let addr = worker.addressHandle();
    // worker.createEndPoint(ErrorHandler, );
    println!("{:#?}", worker);
    println!("{:#?}", addr);
}

struct ErrorHandler;

// impl EndPointErrorHandler for ErrorHandler {
//     fn shouldWeReconnect(
//         &mut self, 
//         status: ucs_status_t
//     ) -> *const ucp_address_t {
//         todo!()
//     }
// }
