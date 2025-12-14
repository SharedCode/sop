use crate::ffi::*;
use std::ffi::CString;

pub fn open_redis_connection(uri: &str) -> Result<(), String> {
    let c_uri = CString::new(uri).unwrap();
    unsafe {
        let ptr = openRedisConnection(c_uri.into_raw());
        let res = crate::utils::process_go_result(ptr);
        if let Some(err_str) = res {
            if err_str.is_empty() {
                Ok(())
            } else {
                Err(err_str)
            }
        } else {
            Ok(())
        }
    }
}

pub fn close_redis_connection() -> Result<(), String> {
    unsafe {
        let ptr = closeRedisConnection();
        let res = crate::utils::process_go_result(ptr);
        if let Some(err_str) = res {
            if err_str.is_empty() {
                Ok(())
            } else {
                Err(err_str)
            }
        } else {
            Ok(())
        }
    }
}
