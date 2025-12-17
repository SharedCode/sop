use crate::context::Context;
use crate::ffi::manageTransaction;
use std::ffi::CString;
use libc::c_int;

enum TransactionAction {
    #[allow(dead_code)]
    Begin = 2,
    Commit = 3,
    Rollback = 4,
}

/// Represents a transaction in the SOP library.
#[derive(Clone)]
pub struct Transaction {
    /// The transaction ID.
    pub id: String,
    /// The database ID associated with the transaction.
    pub database_id: String,
}

impl Transaction {
    /// Creates a new transaction.
    ///
    /// # Arguments
    ///
    /// * `id` - The transaction ID.
    /// * `database_id` - The database ID.
    pub fn new(id: String, database_id: String) -> Self {
        Self { id, database_id }
    }

    /// Commits the transaction.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn commit(&self, ctx: &Context) -> Result<(), String> {
        self.manage(ctx, TransactionAction::Commit)
    }

    /// Rolls back the transaction.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn rollback(&self, ctx: &Context) -> Result<(), String> {
        self.manage(ctx, TransactionAction::Rollback)
    }

    fn manage(&self, ctx: &Context, action: TransactionAction) -> Result<(), String> {
        let c_payload = CString::new(self.id.clone()).unwrap();
        unsafe {
            let ptr = manageTransaction(ctx.id, action as c_int, c_payload.into_raw());
            let res = crate::utils::process_go_result(ptr);
            if let Some(err) = res {
                return Err(err);
            }
            if let Some(err) = ctx.error() {
                return Err(err);
            }
            Ok(())
        }
    }
}
