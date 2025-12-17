use crate::ffi::{createContext, removeContext, contextError};

/// Represents a context for SOP operations.
///
/// The context is used to manage the lifecycle of operations and handle errors.
pub struct Context {
    /// The context ID.
    pub id: i64,
}

impl Context {
    /// Creates a new context.
    pub fn new() -> Self {
        unsafe {
            let id = createContext();
            Context { id }
        }
    }

    /// Checks for errors in the context.
    ///
    /// # Returns
    ///
    /// An option containing the error message if an error occurred, or `None` otherwise.
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
