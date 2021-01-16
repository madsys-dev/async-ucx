use super::*;

impl Endpoint {
    pub fn stream_send(&self, buf: &[u8]) -> RequestHandle {
        trace!("stream_send: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
            trace!(
                "stream_send: complete. req={:?}, status={:?}",
                request,
                status
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let status = unsafe {
            ucp_stream_send_nb(
                self.handle,
                buf.as_ptr() as _,
                buf.len() as _,
                ucp_dt_make_contig(1),
                Some(callback),
                0,
            )
        };
        if status.is_null() {
            trace!("stream_send: complete");
            RequestHandle::Ready(buf.len())
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle::Send(status, buf.len())
        } else {
            panic!("failed to send stream: {:?}", UCS_PTR_RAW_STATUS(status));
        }
    }

    pub fn stream_recv(&self, buf: &mut [MaybeUninit<u8>]) -> RequestHandle {
        trace!("stream_recv: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t, length: u64) {
            trace!(
                "stream_recv: complete. req={:?}, status={:?}, len={}",
                request,
                status,
                length
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let mut length = MaybeUninit::uninit();
        let status = unsafe {
            ucp_stream_recv_nb(
                self.handle,
                buf.as_mut_ptr() as _,
                buf.len() as _,
                ucp_dt_make_contig(1),
                Some(callback),
                length.as_mut_ptr(),
                0,
            )
        };
        if status.is_null() {
            let length = unsafe { length.assume_init() } as usize;
            trace!("stream_recv: complete. len={}", length);
            RequestHandle::Ready(length)
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle::Stream(status)
        } else {
            panic!("failed to recv stream: {:?}", UCS_PTR_RAW_STATUS(status));
        }
    }
}
