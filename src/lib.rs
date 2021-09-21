#![deny(warnings)]

use ucx_sys::ucs_status_ptr_t;
use ucx_sys::ucs_status_t;
use ucx_sys::UCS_PTR_IS_ERR;
use ucx_sys::UCS_PTR_RAW_STATUS;

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

#[repr(i8)]
#[derive(Debug)]
pub enum Error {
    Inprogress,

    NoMessage,
    NoReource,
    IoError,
    NoMemory,
    InvalidParam,
    Unreachable,
    InvalidAddr,
    NotImplemented,
    MessageTruncated,
    NoProgress,
    BufferTooSmall,
    NoElem,
    SomeConnectsFailed,
    NoDevice,
    Busy,
    Canceled,
    ShmemSegment,
    AlreadyExists,
    OutOfRange,
    Timeout,
    ExceedsLimit,
    Unsupported,
    Rejected,
    NotConnected,
    ConnectionReset,

    FirstLinkFailure,
    LastLinkFailure,
    FirstEndpointFailure,
    LastEndpointFailure,
    EndpointTimeout,

    Unknown,
}

impl Error {
    // status != UCS_OK
    fn from_error(status: ucs_status_t) -> Self {
        debug_assert_ne!(status, ucs_status_t::UCS_OK);

        match status {
            ucs_status_t::UCS_INPROGRESS => Self::Inprogress,
            ucs_status_t::UCS_ERR_NO_MESSAGE => Self::NoMessage,
            ucs_status_t::UCS_ERR_NO_RESOURCE => Self::NoReource,
            ucs_status_t::UCS_ERR_IO_ERROR => Self::IoError,
            ucs_status_t::UCS_ERR_NO_MEMORY => Self::NoMemory,
            ucs_status_t::UCS_ERR_INVALID_PARAM => Self::InvalidParam,
            ucs_status_t::UCS_ERR_UNREACHABLE => Self::Unreachable,
            ucs_status_t::UCS_ERR_INVALID_ADDR => Self::InvalidAddr,
            ucs_status_t::UCS_ERR_NOT_IMPLEMENTED => Self::NotImplemented,
            ucs_status_t::UCS_ERR_MESSAGE_TRUNCATED => Self::MessageTruncated,
            ucs_status_t::UCS_ERR_NO_PROGRESS => Self::NoProgress,
            ucs_status_t::UCS_ERR_BUFFER_TOO_SMALL => Self::BufferTooSmall,
            ucs_status_t::UCS_ERR_NO_ELEM => Self::NoElem,
            ucs_status_t::UCS_ERR_SOME_CONNECTS_FAILED => Self::SomeConnectsFailed,
            ucs_status_t::UCS_ERR_NO_DEVICE => Self::NoDevice,
            ucs_status_t::UCS_ERR_BUSY => Self::Busy,
            ucs_status_t::UCS_ERR_CANCELED => Self::Canceled,
            ucs_status_t::UCS_ERR_SHMEM_SEGMENT => Self::ShmemSegment,
            ucs_status_t::UCS_ERR_ALREADY_EXISTS => Self::AlreadyExists,
            ucs_status_t::UCS_ERR_OUT_OF_RANGE => Self::OutOfRange,
            ucs_status_t::UCS_ERR_TIMED_OUT => Self::Timeout,
            ucs_status_t::UCS_ERR_EXCEEDS_LIMIT => Self::ExceedsLimit,
            ucs_status_t::UCS_ERR_UNSUPPORTED => Self::Unsupported,
            ucs_status_t::UCS_ERR_REJECTED => Self::Rejected,
            ucs_status_t::UCS_ERR_NOT_CONNECTED => Self::NotConnected,
            ucs_status_t::UCS_ERR_CONNECTION_RESET => Self::ConnectionReset,

            ucs_status_t::UCS_ERR_FIRST_LINK_FAILURE => Self::FirstLinkFailure,
            ucs_status_t::UCS_ERR_LAST_LINK_FAILURE => Self::LastLinkFailure,
            ucs_status_t::UCS_ERR_FIRST_ENDPOINT_FAILURE => Self::FirstEndpointFailure,
            ucs_status_t::UCS_ERR_ENDPOINT_TIMEOUT => Self::EndpointTimeout,
            ucs_status_t::UCS_ERR_LAST_ENDPOINT_FAILURE => Self::LastEndpointFailure,

            _ => Self::Unknown,
        }
    }

    #[inline]
    fn from_status(status: ucs_status_t) -> Result<(), Self> {
        if status == ucs_status_t::UCS_OK {
            Ok(())
        } else {
            Err(Self::from_error(status))
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn from_ptr(ptr: ucs_status_ptr_t) -> Result<(), Self> {
        if UCS_PTR_IS_ERR(ptr) {
            Err(Self::from_error(UCS_PTR_RAW_STATUS(ptr)))
        } else {
            Ok(())
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            Error::Inprogress => "Operation in progress",
            Error::NoMessage => "No pending message",
            Error::NoReource => "No resources are available to initiate the operation",
            Error::IoError => "Input/output error",
            Error::NoMemory => "Out of memory",
            Error::InvalidParam => "Invalid parameter",
            Error::Unreachable => "Destination is unreachable",
            Error::InvalidAddr => "Address not valid",
            Error::NotImplemented => "Function not implemented",
            Error::MessageTruncated => "Message truncated",
            Error::NoProgress => "No progress",
            Error::BufferTooSmall => "Provided buffer is too small",
            Error::NoElem => "No such element",
            Error::SomeConnectsFailed => "Failed to connect some of the requested endpoints",
            Error::NoDevice => "No such device",
            Error::Busy => "Device is busy",
            Error::Canceled => "Request canceled",
            Error::ShmemSegment => "Shared memory error",
            Error::AlreadyExists => "Element already exists",
            Error::OutOfRange => "Index out of range",
            Error::Timeout => "Operation timed out",
            Error::ExceedsLimit => "User-defined limit was reached",
            Error::Unsupported => "Unsupported operation",
            Error::Rejected => "Operation rejected by remote peer",
            Error::NotConnected => "Endpoint is not connected",
            Error::ConnectionReset => "Connection reset by remote peer",

            Error::FirstLinkFailure => "First link failure",
            Error::LastLinkFailure => "Last link failure",
            Error::FirstEndpointFailure => "First endpoint failure",
            Error::LastEndpointFailure => "Last endpoint failure",
            Error::EndpointTimeout => "Endpoint timeout",

            Error::Unknown => "Unknown error",
        };

        f.write_str(msg)
    }
}

impl std::error::Error for Error {}
