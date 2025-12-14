use std::ffi::{CStr, CString};
use libc::{c_char, c_int, c_longlong};
use crate::ffi::{freeString, manageDatabase};
use uuid::Uuid;

/// Helper to process the result from a Go FFI call.
/// It converts the returned C string to a Rust String, frees the C string,
/// and returns Option<String>.
pub unsafe fn process_go_result(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let res = CStr::from_ptr(ptr).to_string_lossy().into_owned();
    freeString(ptr);
    Some(res)
}

/// Helper to process the result from a Go FFI call that returns a single string.
/// It attempts to parse the string as a UUID. If successful, it returns the UUID string.
/// If parsing fails, it treats the string as an error message.
/// If the string is null, it returns Ok(None).
pub unsafe fn process_go_result_uuid(ptr: *mut c_char) -> Result<Option<String>, String> {
    let s = process_go_result(ptr);
    match s {
        Some(val) => {
            if Uuid::parse_str(&val).is_ok() {
                Ok(Some(val))
            } else {
                Err(val)
            }
        },
        None => Ok(None)
    }
}

/// Helper to call manageDatabase and process the result.
/// It handles CString conversion and UUID parsing.
pub fn manage_database_safe(ctx_id: i64, action: i32, target_id: String, payload: String) -> Result<Option<String>, String> {
    let c_target = CString::new(target_id).map_err(|e| e.to_string())?;
    let c_payload = CString::new(payload).map_err(|e| e.to_string())?;

    unsafe {
        let res = manageDatabase(ctx_id as c_longlong, action as c_int, c_target.into_raw(), c_payload.into_raw());
        process_go_result_uuid(res)
    }
}