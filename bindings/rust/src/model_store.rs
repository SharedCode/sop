use crate::context::Context;
use crate::ffi::*;
use serde::Serialize;
use std::ffi::CString;
use libc::c_int;

enum ModelStoreAction {
    Save = 1,
    Load = 2,
    Delete = 3,
}

/// Represents a model store in the SOP library.
#[derive(Clone)]
pub struct ModelStore {
    /// The model store ID.
    pub id: String,
    /// The transaction ID associated with the model store.
    pub transaction_id: String,
}

impl ModelStore {
    fn get_metadata(&self) -> Result<String, String> {
        #[derive(Serialize)]
        struct Metadata {
            id: String,
            transaction_id: String,
        }
        let meta = Metadata {
            id: self.id.clone(),
            transaction_id: self.transaction_id.clone(),
        };
        serde_json::to_string(&meta).map_err(|e| e.to_string())
    }

    /// Saves a model to the store.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `category` - The category of the model.
    /// * `name` - The name of the model.
    /// * `data` - The model data.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn save(&self, ctx: &Context, category: &str, name: &str, data: Vec<u8>) -> Result<(), String> {
        #[derive(Serialize)]
        struct SaveParams {
            category: String,
            name: String,
            data: Vec<u8>,
        }
        let params = SaveParams {
            category: category.to_string(),
            name: name.to_string(),
            data,
        };
        let payload = serde_json::to_string(&params).map_err(|e| e.to_string())?;
        self.manage(ctx, ModelStoreAction::Save, payload)
    }

    /// Loads a model from the store.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `category` - The category of the model.
    /// * `name` - The name of the model.
    ///
    /// # Returns
    ///
    /// A result containing the model data or an error message.
    pub fn load(&self, ctx: &Context, category: &str, name: &str) -> Result<Vec<u8>, String> {
        #[derive(Serialize)]
        struct LoadParams {
            category: String,
            name: String,
        }
        let params = LoadParams {
            category: category.to_string(),
            name: name.to_string(),
        };
        let payload = serde_json::to_string(&params).map_err(|e| e.to_string())?;
        let c_payload = CString::new(payload).unwrap();
        let meta = self.get_metadata()?;
        let c_target = CString::new(meta).unwrap();

        unsafe {
            let ptr = manageModelStore(ctx.id, ModelStoreAction::Load as c_int, c_target.into_raw(), c_payload.into_raw());
            let res = crate::utils::process_go_result(ptr);
            if res.is_none() {
                if let Some(err) = ctx.error() {
                    return Err(err);
                }
                return Err("Model not found".to_string());
            }
            
            let json_str = res.unwrap();
            if json_str.is_empty() {
                return Err("Model data is empty".to_string());
            }
            let data: Vec<u8> = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
            Ok(data)
        }
    }

    /// Deletes a model from the store.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `category` - The category of the model.
    /// * `name` - The name of the model.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn delete(&self, ctx: &Context, category: &str, name: &str) -> Result<(), String> {
        #[derive(Serialize)]
        struct DeleteParams {
            category: String,
            name: String,
        }
        let params = DeleteParams {
            category: category.to_string(),
            name: name.to_string(),
        };
        let payload = serde_json::to_string(&params).map_err(|e| e.to_string())?;
        self.manage(ctx, ModelStoreAction::Delete, payload)
    }

    fn manage(&self, ctx: &Context, action: ModelStoreAction, payload: String) -> Result<(), String> {
        let c_payload = CString::new(payload).unwrap();
        let meta = self.get_metadata()?;
        let c_target = CString::new(meta).unwrap();

        unsafe {
            let ptr = manageModelStore(ctx.id, action as c_int, c_target.into_raw(), c_payload.into_raw());
            crate::utils::process_go_result(ptr);
            if let Some(err) = ctx.error() {
                return Err(err);
            }
            Ok(())
        }
    }
}
