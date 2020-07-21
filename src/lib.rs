use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::Result;
use tokio::prelude::*;

pub mod ucx;

pub struct UcxStream {}

impl UcxStream {
    fn new() {}
}

impl AsyncRead for UcxStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        todo!()
    }
}
