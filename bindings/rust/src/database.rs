use crate::context::Context;
use crate::transaction::Transaction;
use crate::btree::{Btree, BtreeOptions};
use crate::vector_store::VectorStore;
use crate::model_store::ModelStore;
use crate::search::Search;
use crate::utils::manage_database_safe;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy)]
pub enum DatabaseType {
    Standalone = 0,
    Clustered = 1,
}

impl Serialize for DatabaseType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> Deserialize<'de> for DatabaseType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = i32::deserialize(deserializer)?;
        match v {
            0 => Ok(DatabaseType::Standalone),
            1 => Ok(DatabaseType::Clustered),
            _ => Err(serde::de::Error::custom("invalid DatabaseType")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum L2CacheType {
    NoCache = 0,
    InMemory = 1,
    Redis = 2,
}

impl Serialize for L2CacheType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> Deserialize<'de> for L2CacheType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = i32::deserialize(deserializer)?;
        match v {
            0 => Ok(L2CacheType::NoCache),
            1 => Ok(L2CacheType::InMemory),
            2 => Ok(L2CacheType::Redis),
            _ => Err(serde::de::Error::custom("invalid L2CacheType")),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatabaseOptions {
    #[serde(rename = "stores_folders", skip_serializing_if = "Option::is_none")]
    pub stores_folders: Option<Vec<String>>,
    #[serde(rename = "keyspace", skip_serializing_if = "Option::is_none")]
    pub keyspace: Option<String>,
    #[serde(rename = "cache_type")]
    pub cache_type: L2CacheType,
    #[serde(rename = "type")]
    pub db_type: DatabaseType,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            stores_folders: None,
            keyspace: None,
            cache_type: L2CacheType::InMemory,
            db_type: DatabaseType::Standalone,
        }
    }
}

enum DatabaseAction {
    NewDatabase = 1,
    BeginTransaction = 2,
    OpenModelStore = 5,
    OpenVectorStore = 6,
    OpenSearch = 7,
    RemoveBtree = 8,
}

#[derive(Clone)]
pub struct Database {
    pub id: String,
}

impl Database {
    pub fn new(ctx: &Context, options: DatabaseOptions) -> Result<Self, String> {
        let payload = serde_json::to_string(&options).map_err(|e| e.to_string())?;
        
        let processed = manage_database_safe(ctx.id, DatabaseAction::NewDatabase as i32, "".to_string(), payload)?;
        
        if let Some(id) = processed {
            Ok(Database { id })
        } else {
            Err("Failed to create database: no ID returned".to_string())
        }
    }

    pub fn begin_transaction(&self, ctx: &Context) -> Result<Transaction, String> {
        let processed = manage_database_safe(ctx.id, DatabaseAction::BeginTransaction as i32, self.id.clone(), "".to_string())?;
        
        if let Some(id) = processed {
            Ok(Transaction::new(id, self.id.clone()))
        } else {
            Err("Failed to begin transaction: no ID returned".to_string())
        }
    }
    
    pub fn new_btree<K, V>(&self, ctx: &Context, name: &str, trans: &Transaction, options: Option<BtreeOptions>) -> Result<Btree<K, V>, String> {
        Btree::create(ctx, name, trans, options)
    }

    pub fn open_btree<K, V>(&self, ctx: &Context, name: &str, trans: &Transaction, options: Option<BtreeOptions>) -> Result<Btree<K, V>, String> {
        Btree::open(ctx, name, trans, options)
    }

    pub fn remove_btree(&self, ctx: &Context, name: &str) -> Result<(), String> {
        #[derive(Serialize)]
        struct StoreParams {
            name: String,
        }
        
        let params = StoreParams {
            name: name.to_string(),
        };
        
        let payload = serde_json::to_string(&params).map_err(|e| e.to_string())?;
        
        manage_database_safe(ctx.id, DatabaseAction::RemoveBtree as i32, self.id.clone(), payload)?;
        Ok(())
    }

    pub fn open_vector_store(&self, ctx: &Context, name: &str, trans: &Transaction) -> Result<VectorStore, String> {
        #[derive(Serialize)]
        struct StoreParams {
            name: String,
            transaction_id: String,
        }
        
        let params = StoreParams {
            name: name.to_string(),
            transaction_id: trans.id.clone(),
        };
        
        let payload = serde_json::to_string(&params).map_err(|e| e.to_string())?;
        
        let processed = manage_database_safe(ctx.id, DatabaseAction::OpenVectorStore as i32, self.id.clone(), payload)?;
        
        if let Some(id) = processed {
            Ok(VectorStore { 
                id,
                transaction_id: trans.id.clone(),
            })
        } else {
            Err("Failed to open vector store: no ID returned".to_string())
        }
    }

    pub fn open_search(&self, ctx: &Context, name: &str, trans: &Transaction) -> Result<Search, String> {
        #[derive(Serialize)]
        struct StoreParams {
            name: String,
            transaction_id: String,
        }
        
        let params = StoreParams {
            name: name.to_string(),
            transaction_id: trans.id.clone(),
        };
        
        let payload = serde_json::to_string(&params).map_err(|e| e.to_string())?;
        
        let processed = manage_database_safe(ctx.id, DatabaseAction::OpenSearch as i32, self.id.clone(), payload)?;
        
        if let Some(id) = processed {
            Ok(Search { id })
        } else {
            Err("Failed to open search: no ID returned".to_string())
        }
    }

    pub fn open_model_store(&self, ctx: &Context, name: &str, trans: &Transaction) -> Result<ModelStore, String> {
        #[derive(Serialize)]
        struct StoreParams {
            name: String,
            transaction_id: String,
        }
        
        let params = StoreParams {
            name: name.to_string(),
            transaction_id: trans.id.clone(),
        };
        
        let payload = serde_json::to_string(&params).map_err(|e| e.to_string())?;
        
        let processed = manage_database_safe(ctx.id, DatabaseAction::OpenModelStore as i32, self.id.clone(), payload)?;
        
        if let Some(id) = processed {
            Ok(ModelStore { 
                id,
                transaction_id: trans.id.clone(),
            })
        } else {
            Err("Failed to open model store: no ID returned".to_string())
        }
    }}
