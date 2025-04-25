use super::*;

/// A memory region allocated through UCP library,
/// which is optimized for remote memory access operations.
#[derive(Debug)]
pub struct MemoryHandle {
    handle: ucp_mem_h,
    context: Arc<Context>,
}

impl MemoryHandle {
    /// Register memory region.
    pub fn register(context: &Arc<Context>, region: &mut [u8]) -> Self {
        #[allow(invalid_value)]
        #[allow(clippy::uninit_assumed_init)]
        let params = ucp_mem_map_params_t {
            field_mask: (ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_ADDRESS
                | ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_LENGTH)
                .0 as u64,
            address: region.as_ptr() as _,
            length: region.len() as _,
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };
        let mut handle = MaybeUninit::<*mut ucp_mem>::uninit();
        let status = unsafe { ucp_mem_map(context.handle, &params, handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        MemoryHandle {
            handle: unsafe { handle.assume_init() },
            context: context.clone(),
        }
    }

    /// Packs into the buffer a remote access key (RKEY) object.
    pub fn pack(&self) -> RKeyBuffer {
        let mut buf = MaybeUninit::<*mut c_void>::uninit();
        let mut len = MaybeUninit::<usize>::uninit();
        let status = unsafe {
            ucp_rkey_pack(
                self.context.handle,
                self.handle,
                buf.as_mut_ptr(),
                len.as_mut_ptr(),
            )
        };
        assert_eq!(status, ucs_status_t::UCS_OK);
        RKeyBuffer {
            buf: unsafe { buf.assume_init() },
            len: unsafe { len.assume_init() },
        }
    }
}

impl Drop for MemoryHandle {
    fn drop(&mut self) {
        unsafe { ucp_mem_unmap(self.context.handle, self.handle) };
    }
}

/// An owned buffer containing remote access key.
#[derive(Debug)]
pub struct RKeyBuffer {
    buf: *mut c_void,
    len: usize,
}

impl AsRef<[u8]> for RKeyBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buf as _, self.len as _) }
    }
}

impl Drop for RKeyBuffer {
    fn drop(&mut self) {
        unsafe { ucp_rkey_buffer_release(self.buf as _) }
    }
}

/// Remote access key.
#[derive(Debug)]
pub struct RKey {
    handle: ucp_rkey_h,
}

unsafe impl Send for RKey {}
unsafe impl Sync for RKey {}

impl RKey {
    /// Create remote access key from packed buffer.
    pub fn unpack(endpoint: &Endpoint, rkey_buffer: &[u8]) -> Self {
        let mut handle = MaybeUninit::<*mut ucp_rkey>::uninit();
        let status = unsafe {
            ucp_ep_rkey_unpack(
                endpoint.handle,
                rkey_buffer.as_ptr() as _,
                handle.as_mut_ptr(),
            )
        };
        assert_eq!(status, ucs_status_t::UCS_OK);
        RKey {
            handle: unsafe { handle.assume_init() },
        }
    }
}

impl Drop for RKey {
    fn drop(&mut self) {
        unsafe { ucp_rkey_destroy(self.handle) }
    }
}

impl Endpoint {
    /// Stores a contiguous block of data into remote memory.
    pub async fn put(&self, buf: &[u8], remote_addr: u64, rkey: &RKey) -> Result<(), Error> {
        trace!("put: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
            trace!("put: complete. req={:?}, status={:?}", request, status);
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let status = unsafe {
            ucp_put_nb(
                self.get_handle()?,
                buf.as_ptr() as _,
                buf.len() as _,
                remote_addr,
                rkey.handle,
                Some(callback),
            )
        };
        if status.is_null() {
            trace!("put: complete.");
            Ok(())
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle {
                ptr: status,
                poll_fn: poll_normal,
            }
            .await
        } else {
            Error::from_ptr(status)
        }
    }

    /// Loads a contiguous block of data from remote memory.
    pub async fn get(&self, buf: &mut [u8], remote_addr: u64, rkey: &RKey) -> Result<(), Error> {
        trace!("get: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
            trace!("get: complete. req={:?}, status={:?}", request, status);
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let status = unsafe {
            ucp_get_nb(
                self.get_handle()?,
                buf.as_mut_ptr() as _,
                buf.len() as _,
                remote_addr,
                rkey.handle,
                Some(callback),
            )
        };
        if status.is_null() {
            trace!("get: complete.");
            Ok(())
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle {
                ptr: status,
                poll_fn: poll_normal,
            }
            .await
        } else {
            Error::from_ptr(status)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test_log::test]
    fn put_get() {
        spawn_thread!(_put_get()).join().unwrap();
    }

    async fn _put_get() {
        let context1 = Context::new().unwrap();
        let worker1 = context1.create_worker().unwrap();
        let context2 = Context::new().unwrap();
        let worker2 = context2.create_worker().unwrap();
        tokio::task::spawn_local(worker1.clone().polling());
        tokio::task::spawn_local(worker2.clone().polling());

        // connect with each other
        let mut listener = worker1
            .create_listener("0.0.0.0:0".parse().unwrap())
            .unwrap();
        let listen_port = listener.socket_addr().unwrap().port();
        let mut addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        addr.set_port(listen_port);
        let (endpoint1, endpoint2) = tokio::join!(
            async {
                let conn1 = listener.next().await;
                worker1.accept(conn1).await.unwrap()
            },
            async { worker2.connect_socket(addr).await.unwrap() },
        );

        let mut buf1: Vec<u8> = vec![0; 0x1000];
        let mut buf2: Vec<u8> = (0..0x1000).map(|x| x as u8).collect();

        // register memory region
        let mem1 = MemoryHandle::register(&context1, &mut buf1);
        let rkey_buf = mem1.pack();
        let rkey2 = RKey::unpack(&endpoint2, rkey_buf.as_ref());

        // test put
        endpoint2
            .put(&buf2[..], buf1.as_mut_ptr() as u64, &rkey2)
            .await
            .unwrap();
        // The completion of the put operation only means that the reference to the buffer can be released.
        // Remote side may can't see the result of put operation, need flush or barray.
        // Otherwise, there is a certain chance that the test will fail.
        endpoint1.flush().await.unwrap();
        endpoint2.flush().await.unwrap();
        assert_eq!(&buf1[..], &buf2[..]);

        // test get
        buf1.iter_mut().for_each(|x| *x = 0);
        endpoint2
            .get(&mut buf2[..], buf1.as_ptr() as u64, &rkey2)
            .await
            .unwrap();
        assert_eq!(&buf1[..], &buf2[..]);

        assert_eq!(endpoint1.get_rc(), (1, 1));
        assert_eq!(endpoint2.get_rc(), (1, 1));
        assert_eq!(endpoint1.close(false).await, Ok(()));
        assert_eq!(endpoint2.close(false).await, Err(Error::ConnectionReset));
        assert_eq!(endpoint1.get_rc(), (1, 0));
        assert_eq!(endpoint2.get_rc(), (1, 1));
        assert_eq!(endpoint2.close(true).await, Ok(()));
        assert_eq!(endpoint2.get_rc(), (1, 0));
    }
}
