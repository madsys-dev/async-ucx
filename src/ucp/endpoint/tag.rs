use super::*;
use std::io::{IoSlice, IoSliceMut};

impl Worker {
    pub async fn tag_recv(&self, tag: u64, buf: &mut [MaybeUninit<u8>]) -> Result<usize, Error> {
        self.tag_recv_mask(tag, u64::max_value(), buf)
            .await
            .map(|info| info.1)
    }

    pub async fn tag_recv_mask(
        &self,
        tag: u64,
        tag_mask: u64,
        buf: &mut [MaybeUninit<u8>],
    ) -> Result<(u64, usize), Error> {
        trace!(
            "tag_recv: worker={:?}, tag={}, mask={:#x} len={}",
            self.handle,
            tag,
            tag_mask,
            buf.len()
        );
        unsafe extern "C" fn callback(
            request: *mut c_void,
            status: ucs_status_t,
            info: *mut ucp_tag_recv_info,
        ) {
            let length = (*info).length;
            trace!(
                "tag_recv: complete. req={:?}, status={:?}, len={}",
                request,
                status,
                length
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let status = unsafe {
            ucp_tag_recv_nb(
                self.handle,
                buf.as_mut_ptr() as _,
                buf.len() as _,
                ucp_dt_make_contig(1),
                tag,
                tag_mask,
                Some(callback),
            )
        };

        Error::from_ptr(status)?;
        RequestHandle {
            ptr: status,
            poll_fn: poll_tag,
        }
        .await
    }

    pub async fn tag_recv_vectored(
        &self,
        tag: u64,
        iov: &mut [IoSliceMut<'_>],
    ) -> Result<usize, Error> {
        trace!(
            "tag_recv_vectored: worker={:?} iov.len={}",
            self.handle,
            iov.len()
        );
        unsafe extern "C" fn callback(
            request: *mut c_void,
            status: ucs_status_t,
            info: *mut ucp_tag_recv_info,
        ) {
            let length = (*info).length;
            trace!(
                "tag_recv_vectored: complete. req={:?}, status={:?}, len={}",
                request,
                status,
                length
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let status = unsafe {
            ucp_tag_recv_nb(
                self.handle,
                iov.as_ptr() as _,
                iov.len() as _,
                ucp_dt_type::UCP_DATATYPE_IOV as _,
                tag,
                u64::max_value(),
                Some(callback),
            )
        };
        Error::from_ptr(status)?;
        RequestHandle {
            ptr: status,
            poll_fn: poll_tag,
        }
        .await
        .map(|info| info.1)
    }
}

impl Endpoint {
    pub async fn tag_send(&self, tag: u64, buf: &[u8]) -> Result<usize, Error> {
        trace!("tag_send: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
            trace!("tag_send: complete. req={:?}, status={:?}", request, status);
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let status = unsafe {
            ucp_tag_send_nb(
                self.get_handle()?,
                buf.as_ptr() as _,
                buf.len() as _,
                ucp_dt_make_contig(1),
                tag,
                Some(callback),
            )
        };
        if status.is_null() {
            trace!("tag_send: complete");
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle {
                ptr: status,
                poll_fn: poll_normal,
            }
            .await?;
        } else {
            return Err(Error::from_ptr(status).unwrap_err());
        }
        Ok(buf.len())
    }

    pub async fn tag_send_vectored(&self, tag: u64, iov: &[IoSlice<'_>]) -> Result<usize, Error> {
        trace!(
            "tag_send_vectored: endpoint={:?} iov.len={}",
            self.handle,
            iov.len()
        );
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
            trace!(
                "tag_send_vectored: complete. req={:?}, status={:?}",
                request,
                status
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let status = unsafe {
            ucp_tag_send_nb(
                self.get_handle()?,
                iov.as_ptr() as _,
                iov.len() as _,
                ucp_dt_type::UCP_DATATYPE_IOV as _,
                tag,
                Some(callback),
            )
        };
        let total_len = iov.iter().map(|v| v.len()).sum();
        if status.is_null() {
            trace!("tag_send_vectored: complete");
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle {
                ptr: status,
                poll_fn: poll_normal,
            }
            .await?;
        } else {
            return Err(Error::from_ptr(status).unwrap_err());
        }
        Ok(total_len)
    }
}

unsafe fn poll_tag(ptr: ucs_status_ptr_t) -> Poll<Result<(u64, usize), Error>> {
    let mut info = MaybeUninit::<ucp_tag_recv_info>::uninit();
    let status = ucp_tag_recv_request_test(ptr as _, info.as_mut_ptr() as _);
    match status {
        ucs_status_t::UCS_INPROGRESS => Poll::Pending,
        ucs_status_t::UCS_OK => {
            let info = info.assume_init();
            Poll::Ready(Ok((info.sender_tag, info.length as usize)))
        }
        status => Poll::Ready(Err(Error::from_error(status))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test_log::test]
    fn tag() {
        for i in 0..20_usize {
            spawn_thread!(_tag(4 << i)).join().unwrap();
        }
    }

    async fn _tag(msg_size: usize) {
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
        println!("listen at port {}", listen_port);
        let mut addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        addr.set_port(listen_port);

        let (endpoint1, endpoint2) = tokio::join!(
            async {
                let conn1 = listener.next().await;
                worker1.accept(conn1).await.unwrap()
            },
            async { worker2.connect_socket(addr).await.unwrap() },
        );

        // send tag message
        tokio::join!(
            async {
                // send
                let mut buf = vec![0; msg_size];
                endpoint2.tag_send(1, &mut buf).await.unwrap();
                println!("tag sended");
            },
            async {
                // recv
                let mut buf = vec![MaybeUninit::uninit(); msg_size];
                worker1.tag_recv(1, &mut buf).await.unwrap();
                println!("tag recved");
            }
        );

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
