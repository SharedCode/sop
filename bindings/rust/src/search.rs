use crate::context::Context;
use crate::ffi::*;
use serde::{Serialize, Deserialize};
use std::ffi::CString;
use libc::c_int;

#[derive(Deserialize, Debug, Clone)]
pub struct SearchResult {
    #[serde(rename = "doc_id")]
    pub doc_id: String,
    #[serde(rename = "score")]
    pub score: f32,
    #[serde(rename = "text")]
    pub text: String,
}

enum SearchAction {
    Add = 1,
    #[allow(dead_code)]
    Update = 2,
    #[allow(dead_code)]
    Remove = 3,
    Search = 4,
}

#[derive(Clone)]
pub struct Search {
    pub id: String,
}

impl Search {
    pub fn add(&self, ctx: &Context, doc_id: &str, text: &str) -> Result<(), String> {
        #[derive(Serialize)]
        struct AddParams {
            doc_id: String,
            text: String,
        }
        let params = AddParams {
            doc_id: doc_id.to_string(),
            text: text.to_string(),
        };
        let payload = serde_json::to_string(&params).map_err(|e| e.to_string())?;
        self.manage(ctx, SearchAction::Add, payload)
    }

    pub fn search(&self, ctx: &Context, query: &str) -> Result<Vec<SearchResult>, String> {
        let c_payload = CString::new(query).unwrap();
        let c_target = CString::new(self.id.clone()).unwrap();

        unsafe {
            let ptr = manageSearch(ctx.id, SearchAction::Search as c_int, c_target.into_raw(), c_payload.into_raw());
            let res = crate::utils::process_go_result(ptr);
            if res.is_none() {
                if let Some(err) = ctx.error() {
                    return Err(err);
                }
                return Ok(Vec::new());
            }
            
            let json_str = res.unwrap();
            let results: Vec<SearchResult> = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
            Ok(results)
        }
    }

    fn manage(&self, ctx: &Context, action: SearchAction, payload: String) -> Result<(), String> {
        let c_payload = CString::new(payload).unwrap();
        let c_target = CString::new(self.id.clone()).unwrap();

        unsafe {
            let ptr = manageSearch(ctx.id, action as c_int, c_target.into_raw(), c_payload.into_raw());
            crate::utils::process_go_result(ptr);
            if let Some(err) = ctx.error() {
                return Err(err);
            }
            Ok(())
        }
    }
}
