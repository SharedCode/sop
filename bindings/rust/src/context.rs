use crate::ffi::{createContext, removeContext, contextError};

pub struct Context {
    pub id: i64,
}

impl Context {
    pub fn new() -> Self {
        unsafe {
            let id = createContext();
            Context { id }
        }
    }

    pub fn error(&self) -> Option<String> {
        unsafe {
            let ptr = contextError(self.id);
            let res = crate::utils::process_go_result(ptr);
            if let Some(err_str) = res {
                if err_str.is_empty() {
                    None
                } else {
                    Some(err_str)
                }
            } else {
                None
            }
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe { removeContext(self.id) };
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}
