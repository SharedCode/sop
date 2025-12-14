use crate::context::Context;
use crate::ffi::*;
use serde::{Serialize, Deserialize};
use std::ffi::CString;
use libc::c_int;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VectorItem {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "vector")]
    pub vector: Vec<f32>,
    #[serde(rename = "payload")]
    pub payload: std::collections::HashMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VectorQueryOptions {
    #[serde(rename = "vector")]
    pub vector: Vec<f32>,
    #[serde(rename = "k")]
    pub k: i32,
    #[serde(rename = "filter", skip_serializing_if = "Option::is_none")]
    pub filter: Option<std::collections::HashMap<String, serde_json::Value>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct VectorSearchResult {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "score")]
    pub score: f32,
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

#[derive(Clone)]
pub struct VectorStore {
    pub id: String,
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

    pub fn upsert(&self, ctx: &Context, item: VectorItem) -> Result<(), String> {
        let payload = serde_json::to_string(&item).map_err(|e| e.to_string())?;
        self.manage(ctx, VectorAction::UpsertVector, payload)
    }

    pub fn upsert_batch(&self, ctx: &Context, items: Vec<VectorItem>) -> Result<(), String> {
        let payload = serde_json::to_string(&items).map_err(|e| e.to_string())?;
        self.manage(ctx, VectorAction::UpsertBatchVector, payload)
    }

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
