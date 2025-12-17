use crate::context::Context;
use crate::ffi::*;
use crate::transaction::Transaction;
use serde::{Serialize, Deserialize};
use std::marker::PhantomData;
use std::ffi::CString;
use libc::c_int;

/// Specifies a field to be included in a composite index.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexFieldSpecification {
    /// The name of the field/property in the key object.
    #[serde(rename = "field_name")]
    pub field_name: String,
    /// If true, sorts in ascending order. False for descending.
    #[serde(rename = "ascending_sort_order")]
    pub ascending_sort_order: bool,
}

/// Defines the structure of a composite index for complex keys.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexSpecification {
    /// List of fields that make up the index.
    #[serde(rename = "index_fields")]
    pub index_fields: Vec<IndexFieldSpecification>,
}

/// Options for configuring a B-Tree.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BtreeOptions {
    /// The name of the B-Tree.
    #[serde(rename = "name")]
    pub name: String,
    /// Whether the B-Tree enforces unique keys.
    #[serde(rename = "is_unique")]
    pub is_unique: bool,
    /// Whether the key is a primitive type.
    #[serde(rename = "is_primitive_key")]
    pub is_primitive_key: bool,
    /// The number of slots per node.
    #[serde(rename = "slot_length")]
    pub slot_length: i32,
    /// A description of the B-Tree.
    #[serde(rename = "description")]
    pub description: String,
    /// Whether value data is stored in the node segment.
    #[serde(rename = "is_value_data_in_node_segment")]
    pub is_value_data_in_node_segment: bool,
    /// Whether value data is actively persisted.
    #[serde(rename = "is_value_data_actively_persisted")]
    pub is_value_data_actively_persisted: bool,
    /// Whether value data is globally cached.
    #[serde(rename = "is_value_data_globally_cached")]
    pub is_value_data_globally_cached: bool,
    /// Whether to enable leaf load balancing.
    #[serde(rename = "leaf_load_balancing")]
    pub leaf_load_balancing: bool,
    /// The index specification.
    #[serde(rename = "index_specification", skip_serializing_if = "Option::is_none")]
    pub index_specification: Option<String>,
    /// The transaction ID associated with these options.
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

impl BtreeOptions {
    /// Sets the index specification using a strongly-typed object.
    pub fn set_index_specification(&mut self, spec: IndexSpecification) {
        self.index_specification = Some(serde_json::to_string(&spec).unwrap());
    }
}

/// Represents a key-value pair in the B-Tree.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Item<K, V> {
    /// The key.
    #[serde(rename = "key")]
    pub key: K,
    /// The value.
    #[serde(rename = "value")]
    pub value: Option<V>,
    /// The ID of the item.
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl<K, V> Item<K, V> {
    /// Creates a new item with the given key and value.
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

/// Pagination information for queries.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PagingInfo {
    /// The page size.
    #[serde(rename = "page_size")]
    pub page_size: i32,
    /// The page offset.
    #[serde(rename = "page_offset")]
    pub page_offset: i32,
}

/// A B-Tree wrapper.
#[derive(Clone)]
pub struct Btree<K, V> {
    /// The ID of the B-Tree.
    pub id: String,
    /// The transaction ID associated with the B-Tree.
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

    /// Creates a new B-Tree.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `name` - The name of the B-Tree.
    /// * `trans` - The transaction.
    /// * `options` - The B-Tree options.
    ///
    /// # Returns
    ///
    /// A result containing the created B-Tree or an error message.
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

    /// Opens an existing B-Tree.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `name` - The name of the B-Tree.
    /// * `trans` - The transaction.
    /// * `options` - The B-Tree options.
    ///
    /// # Returns
    ///
    /// A result containing the opened B-Tree or an error message.
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

    /// Adds a key-value pair to the B-Tree.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `key` - The key to add.
    /// * `value` - The value to add.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn add(&self, ctx: &Context, key: K, value: V) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let item = Item::new(key, value);
        self.add_batch(ctx, vec![item])
    }

    /// Adds a batch of items to the B-Tree.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `items` - The list of items to add.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn add_batch(&self, ctx: &Context, items: Vec<Item<K, V>>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::Add, json_payload)? {
            true => Ok(()),
            false => Err("Add operation returned false".to_string()),
        }
    }

    /// Adds a key-value pair to the B-Tree if it does not already exist.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `key` - The key to add.
    /// * `value` - The value to add.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn add_if_not_exist(&self, ctx: &Context, key: K, value: V) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let item = Item::new(key, value);
        self.add_if_not_exist_batch(ctx, vec![item])
    }

    /// Adds a batch of items to the B-Tree if they do not already exist.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `items` - The list of items to add.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn add_if_not_exist_batch(&self, ctx: &Context, items: Vec<Item<K, V>>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::AddIfNotExist, json_payload)? {
            true => Ok(()),
            false => Ok(()),
        }
    }

    /// Inserts or updates a key-value pair in the B-Tree.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `key` - The key to upsert.
    /// * `value` - The value to upsert.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn upsert(&self, ctx: &Context, key: K, value: V) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let item = Item::new(key, value);
        self.upsert_batch(ctx, vec![item])
    }

    /// Inserts or updates a batch of items in the B-Tree.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `items` - The list of items to upsert.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn upsert_batch(&self, ctx: &Context, items: Vec<Item<K, V>>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::Upsert, json_payload)? {
            true => Ok(()),
            false => Err("Upsert operation returned false".to_string()),
        }
    }

    /// Updates a key-value pair in the B-Tree.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `key` - The key to update.
    /// * `value` - The new value.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn update(&self, ctx: &Context, key: K, value: V) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let item = Item::new(key, value);
        self.update_batch(ctx, vec![item])
    }

    /// Updates a batch of items in the B-Tree.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `items` - The list of items to update.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn update_batch(&self, ctx: &Context, items: Vec<Item<K, V>>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::Update, json_payload)? {
            true => Ok(()),
            false => Err("Update operation returned false".to_string()),
        }
    }

    /// Updates the key of an item in the B-Tree.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `item` - The item with the new key.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn update_key(&self, ctx: &Context, item: Item<K, V>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        self.update_keys(ctx, vec![item])
    }

    /// Updates the keys of a batch of items in the B-Tree.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `items` - The list of items with new keys.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn update_keys(&self, ctx: &Context, items: Vec<Item<K, V>>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items, paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::UpdateKey, json_payload)? {
            true => Ok(()),
            false => Err("UpdateKey operation returned false".to_string()),
        }
    }

    /// Updates the current key with a new value.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `item` - The item containing the new value.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn update_current_key(&self, ctx: &Context, item: Item<K, V>) -> Result<(), String> 
    where K: Serialize, V: Serialize {
        let payload = ManageBtreePayload { items: vec![item], paging_info: None };
        let json_payload = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::UpdateCurrentKey, json_payload)? {
            true => Ok(()),
            false => Err("UpdateCurrentKey operation returned false".to_string()),
        }
    }

    /// Removes an item from the B-Tree by its key.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `key` - The key of the item to remove.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn remove(&self, ctx: &Context, key: K) -> Result<(), String> 
    where K: Serialize {
        self.remove_batch(ctx, vec![key])
    }

    /// Removes a batch of items from the B-Tree by their keys.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `keys` - The list of keys of the items to remove.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub fn remove_batch(&self, ctx: &Context, keys: Vec<K>) -> Result<(), String> 
    where K: Serialize {
        let json_payload = serde_json::to_string(&keys).map_err(|e| e.to_string())?;
        match self.manage(ctx, BtreeAction::Remove, json_payload)? {
            true => Ok(()),
            false => Err("Remove operation returned false".to_string()),
        }
    }

    /// Finds an item in the B-Tree by its key.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `key` - The key to search for.
    ///
    /// # Returns
    ///
    /// A result indicating whether the item was found.
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

    /// Gets the value associated with a key.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `key` - The key to search for.
    ///
    /// # Returns
    ///
    /// A result containing the item if found, or None.
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

    /// Gets the values associated with a list of keys.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context.
    /// * `keys` - The list of keys to search for.
    ///
    /// # Returns
    ///
    /// A result containing a list of items found.
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
