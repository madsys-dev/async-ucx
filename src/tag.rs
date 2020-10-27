use super::*;

impl Worker {
    pub fn tag_recv(&self, tag: u64, buf: &mut [MaybeUninit<u8>]) -> TagRequest {
        self.tag_recv_mask(tag, u64::max_value(), buf)
    }

    pub fn tag_recv_mask(
        &self,
        tag: u64,
        tag_mask: u64,
        buf: &mut [MaybeUninit<u8>],
    ) -> TagRequest {
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
        if UCS_PTR_IS_PTR(status) {
            TagRequest { status }
        } else {
            panic!("failed to recv tag: {:?}", UCS_PTR_RAW_STATUS(status));
        }
    }

    pub fn tag_recv_vectored(&self, tag: u64, iov: &mut [IoSliceMut<'_>]) -> TagRequest {
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
        if UCS_PTR_IS_PTR(status) {
            TagRequest { status }
        } else {
            panic!("failed to recv tag: {:?}", UCS_PTR_RAW_STATUS(status));
        }
    }
}

impl Endpoint {
    pub fn tag_send(&self, tag: u64, buf: &[u8]) -> RequestHandle {
        trace!("tag_send: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
            trace!("tag_send: complete. req={:?}, status={:?}", request, status);
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let status = unsafe {
            ucp_tag_send_nb(
                self.handle,
                buf.as_ptr() as _,
                buf.len() as _,
                ucp_dt_make_contig(1),
                tag,
                Some(callback),
            )
        };
        if status.is_null() {
            trace!("tag_send: complete");
            RequestHandle::Ready(buf.len())
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle::Send(status, buf.len())
        } else {
            panic!("failed to send tag: {:?}", UCS_PTR_RAW_STATUS(status));
        }
    }

    pub fn tag_send_vectored(&self, tag: u64, iov: &[IoSlice<'_>]) -> RequestHandle {
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
                self.handle,
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
            RequestHandle::Ready(total_len)
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle::Send(status, total_len)
        } else {
            panic!("failed to send tag: {:?}", UCS_PTR_RAW_STATUS(status));
        }
    }
}

pub struct TagRequest {
    status: ucs_status_ptr_t,
}

impl TagRequest {
    fn test(&self) -> Poll<(u64, usize)> {
        let mut info = MaybeUninit::<ucp_tag_recv_info>::uninit();
        let status = unsafe { ucp_tag_recv_request_test(self.status as _, info.as_mut_ptr() as _) };
        if status == ucs_status_t::UCS_INPROGRESS {
            return Poll::Pending;
        }
        let info = unsafe { info.assume_init() };
        Poll::Ready((info.sender_tag, info.length as usize))
    }

    fn register_waker(&mut self, waker: &Waker) {
        unsafe { &mut *(self.status as *mut Request) }
            .waker
            .register(waker);
    }
}

impl Future for TagRequest {
    type Output = (u64, usize);
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        if let r @ Poll::Ready(_) = self.test() {
            return r;
        }
        self.register_waker(cx.waker());
        self.test()
    }
}

impl Drop for TagRequest {
    fn drop(&mut self) {
        unsafe { ucp_request_free(self.status as _) };
    }
}
