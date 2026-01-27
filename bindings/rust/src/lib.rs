//! # SOP Rust Bindings
//!
//! This crate provides Rust bindings for the SOP (Store Object Persistence) library.
//! It allows you to interact with SOP databases, B-Trees, vector stores, model stores, and search functionality.
//!
//! ## Modules
//!
//! * `context` - Manages the context for SOP operations.
//! * `transaction` - Handles transactions.
//! * `database` - Manages databases.
//! * `btree` - Provides B-Tree functionality.
//! * `vector_store` - Manages vector stores.
//! * `model_store` - Manages model stores.
//! * `search` - Provides search functionality.
//! * `logger` - Configures logging.
//! * `cassandra` - Manages Cassandra connections.
//! * `redis` - Manages Redis connections.

mod ffi;
mod utils;
mod context;
mod transaction;
mod database;
mod btree;
mod vector_store;
mod model_store;
mod search;
mod logger;
mod cassandra;
mod redis;

pub use context::Context;
pub use transaction::Transaction;
pub use database::{
    Database, DatabaseOptions, DatabaseType, L2CacheType, RedisConfig, ErasureCodingConfig
};
pub use btree::{Btree, BtreeOptions, Item, PagingInfo, IndexSpecification, IndexFieldSpecification};
pub use vector_store::{VectorStore, VectorItem, VectorQueryOptions, VectorSearchResult};
pub use model_store::ModelStore;
pub use search::{Search, SearchResult};
pub use logger::{manage_logging, LogLevel};
pub use cassandra::{open_cassandra_connection, close_cassandra_connection, CassandraConfig, CassandraAuthenticator};
pub use redis::{open_redis_connection, close_redis_connection};

pub type SopContext = Context;
