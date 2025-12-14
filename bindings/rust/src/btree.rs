use crate::context::Context;
use crate::ffi::*;
use crate::transaction::Transaction;
use serde::{Serialize, Deserialize};
use std::marker::PhantomData;
use std::ffi::CString;
use libc::c_int;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BtreeOptions {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "is_unique")]
    pub is_unique: bool,
    #[serde(rename = "is_primitive_key")]
    pub is_primitive_key: bool,
    #[serde(rename = "slot_length")]
    pub slot_length: i32,
    #[serde(rename = "description")]
    pub description: String,
    #[serde(rename = "is_value_data_in_node_segment")]
    pub is_value_data_in_node_segment: bool,
    #[serde(rename = "is_value_data_actively_persisted")]
    pub is_value_data_actively_persisted: bool,
    #[serde(rename = "is_value_data_globally_cached")]
    pub is_value_data_globally_cached: bool,
    #[serde(rename = "leaf_load_balancing")]
    pub leaf_load_balancing: bool,
    #[serde(rename = "index_specification", skip_serializing_if = "Option::is_none")]
    pub index_specification: Option<String>,
    #[serde(rename = "transaction_id")]
    pub transaction_id: String,
}

impl Default for BtreeOptions {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            is_unique: false,
            is_primitive_key: true,
            slot_length: 500,
            description: "".to_string(),
            is_value_data_in_node_segment: true,
            is_value_data_actively_persisted: false,
            is_value_data_globally_cached: false,
            leaf_load_balancing: false,
            index_specification: None,
            transaction_id: "".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Item<K, V> {
    #[serde(rename = "key")]
    pub key: K,
    #[serde(rename = "value")]
    pub value: Option<V>,
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl<K, V> Item<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self { key, value: Some(value), id: None }
    }
}

#[derive(Serialize)]
struct ManageBtreePayload<K, V> {
    items: Vec<Item<K, V>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    paging_info: Option<PagingInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PagingInfo {
    #[serde(rename = "page_size")]
    pub page_size: i32,
    #[serde(rename = "page_offset")]
    pub page_offset: i32,
}

#[derive(Clone)]
pub struct Btree<K, V> {
    pub id: String,
    pub transaction_id: String,
    _marker: PhantomData<(K, V)>,
}

enum BtreeAction {
    Add = 1,
    AddIfNotExist = 2,
    Update = 3,
    Upsert = 4,
    Remove = 5,
    Find = 6,
    #[allow(dead_code)]
    FindWithId = 7,
    GetItems = 8,
    GetValues = 9,
    GetKeys = 10,
    MoveFirst = 11,
    MoveLast = 12,
    #[allow(dead_code)]
    IsUnique = 13,
    #[allow(dead_code)]
    Count = 14,
    #[allow(dead_code)]
    GetStoreInfo = 15,
    UpdateKey = 16,
    UpdateCurrentKey = 17,
    GetCurrentKey = 18,
    MoveNext = 19,
    MovePrevious = 20,
    GetCurrentValue = 21,
}

enum DatabaseAction {
    NewBtree = 3,
    OpenBtree = 4,
}

impl<K, V> Btree<K, V> {
    fn new_internal(id: String, transaction_id: String) -> Self {
        Self {
            id,
            transaction_id,
            _marker: PhantomData,
        }
    }

    pub fn create(ctx: &Context, name: &str, trans: &Transaction, options: Option<BtreeOptions>) -> Result<Self, String> {
        let mut opts = options.unwrap_or_default();
        opts.name = name.to_string();
        opts.transaction_id = trans.id.clone();
        
        // Auto-detect primitive key if not explicitly set?
        // In C#, it does: bool isPrimitive = typeof(TK).IsPrimitive || typeof(TK) == typeof(string);
        // In Rust, we can't easily check this at runtime without specialization or trait bounds.
        // But we can assume the user sets it correctly in options, or default to true.
        // For now, let's leave it as default (true) or what user provided.
        
        let payload = serde_json::to_string(&opts).map_err(|e| e.to_string())?;
        Self::manage_database(ctx, DatabaseAction::NewBtree, trans.database_id.clone(), payload, trans.id.clone())
    }

    pub fn open(ctx: &Context, name: &str, trans: &Transaction, options: Option<BtreeOptions>) -> Result<Self, String> {
        let mut opts = options.unwrap_or_default();
        opts.name = name.to_string();
        opts.transaction_id = trans.id.clone();
        
        let payload = serde_json::to_string(&opts).map_err(|e| e.to_string())?;
        Self::manage_database(ctx, DatabaseAction::OpenBtree, trans.database_id.clone(), payload, trans.id.clone())
    }

    fn manage_database(ctx: &Context, action: DatabaseAction, db_id: String, payload: String, trans_id: String) -> Result<Self, String> {
        let processed = crate::utils::manage_database_safe(ctx.id, action as i32, db_id, payload)?;
        
        if let Some(id) = processed {
            Ok(Btree::new_internal(id, trans_id))
        } else {
            Err("Failed to create/open btree: no ID returned".to_string())
        }
    }

    fn get_meta_json(&self) -> String {
        #[derive(Serialize)]
        struct Meta {
            btree_id: String,
            transaction_id: String,
        }
        let meta = Meta {
            btree_id: self.id.clone(),
            transaction_id: self.transaction_id.clone(),
        };
        serde_json::to_string(&meta).unwrap()
    }

    pub fn add(&self, ctx: &Context, key: K, value: V) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let item = Item::new(key, value);
        self.add_batch(ctx, vec![item])
    }

    pub fn add_batch(&self, ctx: &Context, items: Vec<Item<K, V>>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::Add, json_payload)? {
            true => Ok(()),
            false => Err("Add operation returned false".to_string()),
        }
    }

    pub fn add_if_not_exist(&self, ctx: &Context, key: K, value: V) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let item = Item::new(key, value);
        self.add_if_not_exist_batch(ctx, vec![item])
    }

    pub fn add_if_not_exist_batch(&self, ctx: &Context, items: Vec<Item<K, V>>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::AddIfNotExist, json_payload)? {
            true => Ok(()),
            false => Ok(()),
        }
    }

    pub fn upsert(&self, ctx: &Context, key: K, value: V) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let item = Item::new(key, value);
        self.upsert_batch(ctx, vec![item])
    }

    pub fn upsert_batch(&self, ctx: &Context, items: Vec<Item<K, V>>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::Upsert, json_payload)? {
            true => Ok(()),
            false => Err("Upsert operation returned false".to_string()),
        }
    }

    pub fn update(&self, ctx: &Context, key: K, value: V) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let item = Item::new(key, value);
        self.update_batch(ctx, vec![item])
    }

    pub fn update_batch(&self, ctx: &Context, items: Vec<Item<K, V>>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::Update, json_payload)? {
            true => Ok(()),
            false => Err("Update operation returned false".to_string()),
        }
    }

    pub fn update_key(&self, ctx: &Context, item: Item<K, V>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        self.update_keys(ctx, vec![item])
    }

    pub fn update_keys(&self, ctx: &Context, items: Vec<Item<K, V>>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::UpdateKey, json_payload)? {
            true => Ok(()),
            false => Err("UpdateKey operation returned false".to_string()),
        }
    }

    pub fn update_current_key(&self, ctx: &Context, item: Item<K, V>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items: vec![item], paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::UpdateCurrentKey, json_payload)? {
            true => Ok(()),
            false => Err("UpdateCurrentKey operation returned false".to_string()),
        }
    }

    pub fn remove(&self, ctx: &Context, key: K) -> Result<(), String> 
    where K: Serialize {
        self.remove_batch(ctx, vec![key])
    }

    pub fn remove_batch(&self, ctx: &Context, keys: Vec<K>) -> Result<(), String> 
    where K: Serialize {
        let json_payload = serde_json::to_string(&keys).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::Remove, json_payload)? {
            true => Ok(()),
            false => Err("Remove operation returned false".to_string()),
        }
    }

    pub fn find(&self, ctx: &Context, key: K) -> Result<bool, String> 
    where K: Serialize, V: Serialize {
        let item: Item<K, V> = Item { key, value: None, id: None };
        let payload = ManageBtreePayload { items: vec![item], paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        
        let c_payload = CString::new(json_payload).unwrap();
        let c_meta = CString::new(self.get_meta_json()).unwrap();

        unsafe {
            let ptr = navigateBtree(ctx.id, BtreeAction::Find as c_int, c_meta.into_raw(), c_payload.into_raw());
            let res = crate::utils::process_go_result(ptr);
            if res.is_none() {
                if let Some(err) = ctx.error() {
                    return Err(err);
                }
                return Ok(false);
            }
            let res_str = res.unwrap();
            Ok(res_str == "true")
        }
    }

    pub fn get_value(&self, ctx: &Context, key: K) -> Result<Option<Item<K, V>>, String> 
    where K: Serialize + for<'a> Deserialize<'a> + Clone, V: for<'a> Deserialize<'a> + Serialize {
        let item: Item<K, V> = Item { key: key.clone(), value: None, id: None };
        let payload = ManageBtreePayload { items: vec![item], paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        
        let c_payload = CString::new(json_payload).unwrap();
        let c_meta = CString::new(self.get_meta_json()).unwrap();

        unsafe {
            let ret = getFromBtree(ctx.id, BtreeAction::GetValues as c_int, c_meta.into_raw(), c_payload.into_raw());
            let err_str = crate::utils::process_go_result(ret.r1);
            if let Some(err) = err_str {
                crate::utils::process_go_result(ret.r0);
                return Err(err);
            }
            let res_str = crate::utils::process_go_result(ret.r0);
            if res_str.is_none() {
                return Ok(None);
            }
            let json_str = res_str.unwrap();
            if json_str.is_empty() {
                return Ok(None);
            }
            
            let values: Vec<Item<K, V>> = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
            
            if let Some(item) = values.into_iter().next() {
                Ok(Some(item))
            } else {
                Ok(None)
            }
        }
    }

    pub fn get_values(&self, ctx: &Context, keys: Vec<K>) -> Result<Vec<Item<K, V>>, String> 
    where K: Serialize + for<'a> Deserialize<'a> + Clone, V: for<'a> Deserialize<'a> + Serialize {
        let items_req: Vec<Item<K, V>> = keys.iter().map(|k| Item { key: k.clone(), value: None, id: None }).collect();
        let payload = ManageBtreePayload { items: items_req, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        
        let c_payload = CString::new(json_payload).unwrap();
        let c_meta = CString::new(self.get_meta_json()).unwrap();

        unsafe {
            let ret = getFromBtree(ctx.id, BtreeAction::GetValues as c_int, c_meta.into_raw(), c_payload.into_raw());
            let err_str = crate::utils::process_go_result(ret.r1);
            if let Some(err) = err_str {
                crate::utils::process_go_result(ret.r0);
                return Err(err);
            }
            let res_str = crate::utils::process_go_result(ret.r0);
            if res_str.is_none() {
                return Ok(Vec::new());
            }
            let json_str = res_str.unwrap();
            if json_str.is_empty() {
                return Ok(Vec::new());
            }
            
            let items: Vec<Item<K, V>> = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
            Ok(items)
        }
    }

    pub fn get_keys(&self, ctx: &Context, paging: Option<PagingInfo>) -> Result<Vec<K>, String> 
    where K: for<'a> Deserialize<'a> + Serialize, V: Serialize {
        let payload: ManageBtreePayload<K, V> = ManageBtreePayload { items: Vec::new(), paging_info: paging };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        let c_meta = CString::new(self.get_meta_json()).unwrap();
        let c_payload = CString::new(json_payload).unwrap();
        
        unsafe {
            let ret = getFromBtree(ctx.id, BtreeAction::GetKeys as c_int, c_meta.into_raw(), c_payload.into_raw());
            let err_str = crate::utils::process_go_result(ret.r1);
            if let Some(err) = err_str {
                crate::utils::process_go_result(ret.r0);
                return Err(err);
            }
            let res_str = crate::utils::process_go_result(ret.r0);
            if res_str.is_none() {
                return Ok(Vec::new());
            }
            let json_str = res_str.unwrap();
            if json_str.is_empty() {
                return Ok(Vec::new());
            }
            let items: Vec<K> = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
            Ok(items)
        }
    }

    pub fn count(&self) -> Result<i64, String> {
        let c_meta = CString::new(self.get_meta_json()).unwrap();
        unsafe {
            let ret = getBtreeItemCount(c_meta.into_raw());
            let err_str = crate::utils::process_go_result(ret.r1);
            if let Some(err) = err_str {
                return Err(err);
            }
            Ok(ret.r0)
        }
    }

    pub fn get_items(&self, ctx: &Context) -> Result<Vec<Item<K, V>>, String> 
    where K: for<'a> Deserialize<'a>, V: for<'a> Deserialize<'a> {
        self.get_items_internal(ctx, BtreeAction::GetItems, "".to_string())
    }

    pub fn first(&self, ctx: &Context) -> Result<bool, String> {
        self.navigate(ctx, BtreeAction::MoveFirst)
    }

    pub fn last(&self, ctx: &Context) -> Result<bool, String> {
        self.navigate(ctx, BtreeAction::MoveLast)
    }

    pub fn next(&self, ctx: &Context) -> Result<bool, String> {
        self.navigate(ctx, BtreeAction::MoveNext)
    }

    pub fn previous(&self, ctx: &Context) -> Result<bool, String> {
        self.navigate(ctx, BtreeAction::MovePrevious)
    }

    pub fn current_key(&self, ctx: &Context) -> Result<Option<Item<K, V>>, String> 
    where K: for<'a> Deserialize<'a>, V: for<'a> Deserialize<'a> {
        let c_meta = CString::new(self.get_meta_json()).unwrap();
        let c_payload = CString::new("{}").unwrap();
        
        unsafe {
            let ret = getFromBtree(ctx.id, BtreeAction::GetCurrentKey as c_int, c_meta.into_raw(), c_payload.into_raw());
            let err_str = crate::utils::process_go_result(ret.r1);
            if let Some(err) = err_str {
                crate::utils::process_go_result(ret.r0);
                return Err(err);
            }
            let res_str = crate::utils::process_go_result(ret.r0);
            if res_str.is_none() {
                return Ok(None);
            }
            let json_str = res_str.unwrap();
            if json_str.is_empty() {
                return Ok(None);
            }
            // Go backend returns a list of items (usually one)
            let items: Vec<Item<K, V>> = serde_json::from_str(&json_str).map_err(|e| {
                format!("Failed to deserialize Item list: {}. JSON: {}", e, json_str)
            })?;
            
            if let Some(item) = items.into_iter().next() {
                Ok(Some(item))
            } else {
                Ok(None)
            }
        }
    }

    pub fn current_value(&self, ctx: &Context) -> Result<Option<V>, String> 
    where V: for<'a> Deserialize<'a> {
        let c_meta = CString::new(self.get_meta_json()).unwrap();
        let c_payload = CString::new("{}").unwrap();
        
        unsafe {
            let ret = getFromBtree(ctx.id, BtreeAction::GetCurrentValue as c_int, c_meta.into_raw(), c_payload.into_raw());
            let err_str = crate::utils::process_go_result(ret.r1);
            if let Some(err) = err_str {
                crate::utils::process_go_result(ret.r0);
                return Err(err);
            }
            let res_str = crate::utils::process_go_result(ret.r0);
            if res_str.is_none() {
                return Ok(None);
            }
            let json_str = res_str.unwrap();
            if json_str.is_empty() {
                return Ok(None);
            }
            let val: V = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
            Ok(Some(val))
        }
    }

    fn manage(&self, ctx: &Context, action: BtreeAction, payload: String) -> Result<bool, String> {
        let c_payload = CString::new(payload).unwrap();
        let c_meta = CString::new(self.get_meta_json()).unwrap();

        unsafe {
            let ptr = manageBtree(ctx.id, action as c_int, c_meta.into_raw(), c_payload.into_raw());
            let res_opt = crate::utils::process_go_result(ptr);
            if res_opt.is_none() {
                if let Some(err) = ctx.error() {
                    return Err(err);
                }
                return Err("Unknown error".to_string());
            }
            let res = res_opt.unwrap();
            
            if res == "true" {
                return Ok(true);
            }
            if res == "false" {
                return Ok(false);
            }
            return Err(res);
        }
    }

    fn navigate(&self, ctx: &Context, action: BtreeAction) -> Result<bool, String> {
        let c_meta = CString::new(self.get_meta_json()).unwrap();
        let c_payload = CString::new("").unwrap();

        unsafe {
            let ptr = navigateBtree(ctx.id, action as c_int, c_meta.into_raw(), c_payload.into_raw());
            let res_opt = crate::utils::process_go_result(ptr);
            if res_opt.is_none() {
                if let Some(err) = ctx.error() {
                    return Err(err);
                }
                return Ok(false);
            }
            let res_str = res_opt.unwrap();
            Ok(res_str == "true")
        }
    }

    fn get_items_internal(&self, ctx: &Context, action: BtreeAction, payload: String) -> Result<Vec<Item<K, V>>, String> 
    where K: for<'a> Deserialize<'a>, V: for<'a> Deserialize<'a> {
        let c_payload = CString::new(payload).unwrap();
        let c_meta = CString::new(self.get_meta_json()).unwrap();

        unsafe {
            let ret = getFromBtree(ctx.id, action as c_int, c_meta.into_raw(), c_payload.into_raw());
            let err_str = crate::utils::process_go_result(ret.r1);
            if let Some(err) = err_str {
                crate::utils::process_go_result(ret.r0);
                return Err(err);
            }
            let res_str = crate::utils::process_go_result(ret.r0);
            if res_str.is_none() {
                return Ok(Vec::new());
            }
            let json_str = res_str.unwrap();
            if json_str.is_empty() {
                return Ok(Vec::new());
            }
            let items: Vec<Item<K, V>> = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
            Ok(items)
        }
    }
}
