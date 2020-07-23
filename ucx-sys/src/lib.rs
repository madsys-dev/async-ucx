#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(improper_ctypes)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

/// @ingroup UCP_DATATYPE
/// @brief Generate an identifier for contiguous data type.
///
/// This macro creates an identifier for contiguous datatype that is defined by
/// the size of the basic element.
///
/// @param [in]  _elem_size    Size of the basic element of the type.
///
/// @return Data-type identifier.
///
/// @note In case of partial receive, the buffer will be filled with integral
///       count of elements.
pub const fn ucp_dt_make_contig(elem_size: usize) -> ucp_datatype_t {
    ((elem_size as ucp_datatype_t) << (ucp_dt_type::UCP_DATATYPE_SHIFT as ucp_datatype_t))
        | ucp_dt_type::UCP_DATATYPE_CONTIG as ucp_datatype_t
}

pub fn UCS_PTR_IS_ERR(ptr: ucs_status_ptr_t) -> bool {
    ptr as usize >= ucs_status_t::UCS_ERR_LAST as usize
}

pub fn UCS_PTR_IS_PTR(ptr: ucs_status_ptr_t) -> bool {
    ptr as usize - 1 < ucs_status_t::UCS_ERR_LAST as usize - 1
}

pub fn UCS_PTR_RAW_STATUS(ptr: ucs_status_ptr_t) -> ucs_status_t {
    unsafe { std::mem::transmute(ptr as i8) }
}

pub fn UCS_PTR_STATUS(ptr: ucs_status_ptr_t) -> ucs_status_t {
    if UCS_PTR_IS_PTR(ptr) {
        ucs_status_t::UCS_INPROGRESS
    } else {
        UCS_PTR_RAW_STATUS(ptr)
    }
}

// #define UCS_PTR_IS_ERR(_ptr)       (((uintptr_t)(_ptr)) >= ((uintptr_t)UCS_ERR_LAST))
// #define UCS_PTR_IS_PTR(_ptr)       (((uintptr_t)(_ptr) - 1) < ((uintptr_t)UCS_ERR_LAST - 1))
// #define UCS_PTR_RAW_STATUS(_ptr)   ((ucs_status_t)(intptr_t)(_ptr))
// #define UCS_PTR_STATUS(_ptr)       (UCS_PTR_IS_PTR(_ptr) ? UCS_INPROGRESS : UCS_PTR_RAW_STATUS(_ptr))
// #define UCS_STATUS_PTR(_status)    ((void*)(intptr_t)(_status))
// #define UCS_STATUS_IS_ERR(_status) ((_status) < 0)
