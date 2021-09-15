use tokio::sync::Notify;

use super::*;
use std::{
    collections::VecDeque,
    io::{IoSlice, IoSliceMut},
    slice,
    sync::{atomic::AtomicBool, Mutex},
};

#[derive(Debug, PartialEq, Eq)]
pub enum AmDataType {
    None,
    Data,
    Rndv,
}

struct RawMsg {
    id: u32,
    header: Vec<u8>,
    data: &'static [u8],
    reply_ep: ucp_ep_h,
    attr: u64,
}

impl RawMsg {
    fn from_raw(
        id: u32,
        header: &[u8],
        data: &'static [u8],
        reply_ep: ucp_ep_h,
        attr: u64,
    ) -> Self {
        RawMsg {
            id,
            header: header.to_owned(),
            data,
            reply_ep,
            attr,
        }
    }
}

pub struct AmMsg<'a> {
    worker: &'a Worker,
    msg: RawMsg,
}

impl<'a> AmMsg<'a> {
    fn from_raw(worker: &'a Worker, msg: RawMsg) -> Self {
        AmMsg { worker, msg }
    }

    #[inline]
    pub fn id(&self) -> u32 {
        self.msg.id
    }

    #[inline]
    pub fn header(&self) -> &[u8] {
        self.msg.header.as_ref()
    }

    #[inline]
    pub fn contains_data(&self) -> bool {
        self.data_type() != AmDataType::None
    }

    pub fn data_type(&self) -> AmDataType {
        if self.msg.attr & (ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_RNDV as u64) != 0 {
            AmDataType::Rndv
        } else if self.msg.attr & (ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_DATA as u64) != 0 {
            AmDataType::Data
        } else {
            AmDataType::None
        }
    }

    #[inline]
    pub fn get_data(&self) -> Option<&'a [u8]> {
        if self.msg.attr & (ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_DATA as u64) == 0 {
            None
        } else {
            Some(self.msg.data)
        }
    }

    #[inline]
    pub fn data_len(&self) -> usize {
        self.msg.data.len()
    }

    pub async fn recv_data(&mut self, buf: &mut [u8]) -> Result<usize, ()> {
        if !self.contains_data() {
            Ok(0)
        } else {
            let iov = [IoSliceMut::new(buf)];
            self.recv_data_vectored(&iov).await
        }
    }

    pub async fn recv_data_vectored(&mut self, iov: &[IoSliceMut<'_>]) -> Result<usize, ()> {
        if !self.contains_data() {
            Ok(0)
        } else {
            // clear data attr
            self.msg.attr ^= ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_RNDV as u64
                & ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_DATA as u64;
            let data_desc = self.msg.data.as_ptr();
            let data_len = self.msg.data.len();
            self.msg.data = &[];

            unsafe extern "C" fn callback(
                request: *mut c_void,
                status: ucs_status_t,
                _length: u64,
                _data: *mut c_void,
            ) {
                // todo: handle error & fix real data length
                trace!(
                    "recv_data_vectored: complete, req={:?}, status={:?}",
                    request,
                    status
                );
                let request = &mut *(request as *mut Request);
                request.waker.wake();
            }
            trace!(
                "recv_data_vectored: worker={:?} iov.len={}",
                self.worker.handle,
                iov.len()
            );
            let mut param = MaybeUninit::<ucp_request_param_t>::uninit();
            let (buffer, count) = unsafe {
                let param = &mut *param.as_mut_ptr();
                param.op_attr_mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                    | ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32;
                param.cb = ucp_request_param_t__bindgen_ty_1 {
                    recv_am: Some(callback),
                };

                if iov.len() == 1 {
                    param.datatype = ucp_dt_make_contig(1);
                    (iov[0].as_ptr(), iov[0].len())
                } else {
                    param.datatype = ucp_dt_type::UCP_DATATYPE_IOV as _;
                    (iov.as_ptr() as _, iov.len())
                }
            };

            let status = unsafe {
                ucp_am_recv_data_nbx(
                    self.worker.handle,
                    data_desc as _,
                    buffer as _,
                    count as _,
                    param.as_ptr(),
                )
            };
            if status.is_null() {
                trace!("recv_data_vectored: complete");
                Ok(data_len)
            } else if UCS_PTR_IS_PTR(status) {
                RequestHandle {
                    ptr: status,
                    poll_fn: poll_recv,
                }
                .await;
                Ok(data_len)
            } else {
                panic!("failed to recv data: {:?}", UCS_PTR_RAW_STATUS(status));
            }
        }
    }

    #[inline]
    pub fn need_reply(&self) -> bool {
        self.msg.attr & (ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FIELD_REPLY_EP as u64) != 0
            && !self.msg.reply_ep.is_null()
    }

    // Send reply to endpoint
    pub async fn reply(self) {
        // todo: we should prevent endpoint from being freed
        //       currently, ucx doesn't provide such function.
        todo!()
    }
}

impl<'a> Drop for AmMsg<'a> {
    fn drop(&mut self) {
        if self.contains_data() {
            unsafe {
                ucp_am_data_release(self.worker.handle, self.msg.data.as_ptr() as _);
            }
        }
    }
}

pub(crate) struct AmHandler {
    id: u32,
    msgs: Mutex<VecDeque<RawMsg>>,
    notify: Notify,
    unregistered: AtomicBool,
}

impl AmHandler {
    // new active message handler
    fn new(id: u32) -> Self {
        Self {
            id,
            msgs: Mutex::new(VecDeque::with_capacity(512)),
            notify: Notify::new(),
            unregistered: AtomicBool::new(false),
        }
    }

    // unregister
    fn unregister(&self) {
        self.unregistered
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    // callback function
    fn callback(&self, header: &[u8], data: &'static [u8], reply: ucp_ep_h, attr: u64) {
        let msg = RawMsg::from_raw(self.id, header, data, reply, attr);
        self.msgs.lock().unwrap().push_back(msg);
    }

    // wait active message
    async fn wait_msg<'a>(&self, worker: &'a Worker) -> Option<AmMsg<'a>> {
        while !self.unregistered.load(std::sync::atomic::Ordering::Relaxed) {
            if let Some(msg) = self.msgs.lock().unwrap().pop_front() {
                return Some(AmMsg::from_raw(worker, msg));
            }

            self.notify.notified().await;
        }

        self.msgs
            .lock()
            .unwrap()
            .pop_front()
            .map(|msg| AmMsg::from_raw(worker, msg))
    }
}

impl Worker {
    pub fn am_register(&self, id: u32) {
        unsafe extern "C" fn callback(
            arg: *mut c_void,
            header: *const c_void,
            header_len: u64,
            data: *mut c_void,
            data_len: u64,
            param: *const ucp_am_recv_param_t,
        ) -> ucs_status_t {
            let handler = &*(arg as *const AmHandler);
            let header = slice::from_raw_parts(header as *const u8, header_len as usize);
            let data = slice::from_raw_parts(data as *const u8, data_len as usize);

            if !param.is_null() {
                let param = &*param;
                handler.callback(header, data, param.reply_ep, param.recv_attr);
            } else {
                handler.callback(header, data, std::ptr::null_mut(), 0);
            }

            ucs_status_t::UCS_OK
        }

        if self.am_handlers.read().unwrap().contains_key(&id) {
            return;
        }

        let handler = Rc::new(AmHandler::new(id));
        let mut guard = self.am_handlers.write().unwrap();
        if guard.contains_key(&id) {
            return;
        }
        unsafe {
            let param = ucp_am_handler_param_t {
                id,
                cb: Some(callback),
                arg: Rc::into_raw(handler.clone()) as _,
                field_mask: 0,
                flags: (ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_ID
                    | ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_FLAGS
                    | ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_CB
                    | ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_ARG)
                    .0,
            };
            ucp_worker_set_am_recv_handler(self.handle, &param as _);
        }
        guard.insert(id, handler);
    }

    // Unregister active message handler
    pub fn am_unregister(&self, id: u32) {
        let handler = self.am_handlers.write().unwrap().remove(&id);
        if let Some(handler) = handler {
            handler.unregister();
        }
    }

    pub async fn am_recv<'a>(&'a self, id: u32) -> Option<AmMsg<'a>> {
        let handler = self
            .am_handlers
            .read()
            .unwrap()
            .get(&id)
            .cloned();
        if let Some(handler) = handler {
            handler.wait_msg(self).await
        } else {
            None
        }
    }
}

impl Endpoint {
    pub async fn am_send(&self, id: u32, header: &[u8], data: &[u8]) -> Result<(), ()> {
        let data = [IoSlice::new(data)];
        self.am_send_vectorized(id, header, &data).await
    }

    pub async fn am_send_vectorized(
        &self,
        id: u32,
        header: &[u8],
        data: &[IoSlice<'_>],
    ) -> Result<(), ()> {
        let endpoint = self.handle;
        am_send(endpoint, id, header, data).await
    }
}

async fn am_send(
    endpoint: ucp_ep_h,
    id: u32,
    header: &[u8],
    data: &[IoSlice<'_>],
) -> Result<(), ()> {
    unsafe extern "C" fn callback(request: *mut c_void, _status: ucs_status_t, _data: *mut c_void) {
        trace!("am_send: complete");
        let request = &mut *(request as *mut Request);
        request.waker.wake();
    }

    let mut param = MaybeUninit::<ucp_request_param_t>::uninit();
    let (buffer, count) = unsafe {
        let param = &mut *param.as_mut_ptr();
        param.op_attr_mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
            | ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32;
        param.cb = ucp_request_param_t__bindgen_ty_1 {
            send: Some(callback),
        };

        if data.len() == 1 {
            param.datatype = ucp_dt_make_contig(1);
            (data[0].as_ptr(), data[0].len())
        } else {
            param.datatype = ucp_dt_type::UCP_DATATYPE_IOV as _;
            (data.as_ptr() as _, data.len())
        }
    };

    let status = unsafe {
        ucp_am_send_nbx(
            endpoint,
            id,
            header.as_ptr() as _,
            header.len() as _,
            buffer as _,
            count as _,
            param.as_ptr(),
        )
    };
    if status.is_null() {
        trace!("am_send: complete");
        Ok(())
    } else if UCS_PTR_IS_PTR(status) {
        RequestHandle {
            ptr: status,
            poll_fn: poll_normal,
        }
        .await;
        Ok(())
    } else {
        panic!("failed to send am: {:?}", UCS_PTR_RAW_STATUS(status));
    }
}

unsafe fn poll_recv(ptr: ucs_status_ptr_t) -> Poll<()> {
    let status = ucp_request_check_status(ptr as _);
    if status == ucs_status_t::UCS_INPROGRESS {
        Poll::Pending
    } else {
        Poll::Ready(())
    }
}
