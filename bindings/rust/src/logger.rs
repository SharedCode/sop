use crate::ffi::*;
use std::ffi::CString;
use libc::c_int;

pub enum LogLevel {
    Debug = 0,
    #[allow(dead_code)]
    Info = 1,
    #[allow(dead_code)]
    Warn = 2,
    #[allow(dead_code)]
    Error = 3,
}

pub fn manage_logging(level: LogLevel, log_path: &str) -> Result<(), String> {
    let c_path = CString::new(log_path).unwrap();
    unsafe {
        let ptr = manageLogging(level as c_int, c_path.into_raw());
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
