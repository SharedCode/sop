use crate::context::Context;
use crate::ffi::*;
use serde::{Serialize, Deserialize};
use std::ffi::CString;
use libc::c_int;

/// Represents an item in the vector store.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VectorItem {
    /// The ID of the item.
    #[serde(rename = "id")]
    pub id: String,
    /// The vector data.
    #[serde(rename = "vector")]
    pub vector: Vec<f32>,
    /// Additional payload data.
    #[serde(rename = "payload")]
    pub payload: std::collections::HashMap<String, serde_json::Value>,
}

/// Options for querying the vector store.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VectorQueryOptions {
    /// The query vector.
    #[serde(rename = "vector")]
    pub vector: Vec<f32>,
    /// The number of nearest neighbors to return.
    #[serde(rename = "k")]
    pub k: i32,
    /// Optional filter for the query.
    #[serde(rename = "filter", skip_serializing_if = "Option::is_none")]
    pub filter: Option<std::collections::HashMap<String, serde_json::Value>>,
}

/// Represents a result from a vector search.
#[derive(Deserialize, Debug, Clone)]
pub struct VectorSearchResult {
    /// The ID of the result item.
    #[serde(rename = "id")]
    pub id: String,
    /// The similarity score.
    #[serde(rename = "score")]
    pub score: f32,
    /// The payload of the result item.
    #[serde(rename = "payload")]
    pub payload: std::collections::HashMap<String, serde_json::Value>,
}

enum VectorAction {
    UpsertVector = 1,
    #[allow(dead_code)]
    UpsertBatchVector = 2,
    #[allow(dead_code)]
    GetVector = 3,
    #[allow(dead_code)]
    DeleteVector = 4,
    QueryVector = 5,
    #[allow(dead_code)]
    VectorCount = 6,
}

/// Represents a vector store in the SOP library.
#[derive(Clone)]
pub struct VectorStore {
    /// The vector store ID.
    pub id: String,
    /// The transaction ID associated with the vector store.
    pub transaction_id: String,
}

impl VectorStore {
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

    /// Upserts an item into the vector store.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `item` - The item to upsert.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn upsert(&self, ctx: &Context, item: VectorItem) -> Result<(), String> {
        let payload = serde_json::to_string(&item).map_err(|e| e.to_string())?;
        self.manage(ctx, VectorAction::UpsertVector, payload)
    }

    /// Upserts a batch of items into the vector store.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `items` - The items to upsert.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn upsert_batch(&self, ctx: &Context, items: Vec<VectorItem>) -> Result<(), String> {
        let payload = serde_json::to_string(&items).map_err(|e| e.to_string())?;
        self.manage(ctx, VectorAction::UpsertBatchVector, payload)
    }

    /// Searches the vector store.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `query` - The query options.
    ///
    /// # Returns
    ///
    /// A result containing the search results or an error message.
    pub fn search(&self, ctx: &Context, query: VectorQueryOptions) -> Result<Vec<VectorSearchResult>, String> {
        let payload = serde_json::to_string(&query).map_err(|e| e.to_string())?;
        let c_payload = CString::new(payload).unwrap();
        let meta = self.get_metadata()?;
        let c_target = CString::new(meta).unwrap();

        unsafe {
            let ptr = manageVectorDB(ctx.id, VectorAction::QueryVector as c_int, c_target.into_raw(), c_payload.into_raw());
            let res = crate::utils::process_go_result(ptr);
            if res.is_none() {
                if let Some(err) = ctx.error() {
                    return Err(err);
                }
                return Ok(Vec::new());
            }
            
            let json_str = res.unwrap();
            println!("DEBUG: search response: '{}'", json_str);
            if json_str.is_empty() {
                return Ok(Vec::new());
            }
            let results: Vec<VectorSearchResult> = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
            Ok(results)
        }
    }

    fn manage(&self, ctx: &Context, action: VectorAction, payload: String) -> Result<(), String> {
        let c_payload = CString::new(payload).unwrap();
        let meta = self.get_metadata()?;
        let c_target = CString::new(meta).unwrap();

        unsafe {
            let ptr = manageVectorDB(ctx.id, action as c_int, c_target.into_raw(), c_payload.into_raw());
            crate::utils::process_go_result(ptr);
            if let Some(err) = ctx.error() {
                return Err(err);
            }
            Ok(())
        }
    }
}
