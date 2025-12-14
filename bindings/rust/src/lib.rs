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
pub use database::{Database, DatabaseOptions, DatabaseType, L2CacheType};
pub use btree::{Btree, BtreeOptions, Item, PagingInfo};
pub use vector_store::{VectorStore, VectorItem, VectorQueryOptions, VectorSearchResult};
pub use model_store::ModelStore;
pub use search::{Search, SearchResult};
pub use logger::{manage_logging, LogLevel};
pub use cassandra::{open_cassandra_connection, close_cassandra_connection, CassandraConfig, CassandraAuthenticator};
pub use redis::{open_redis_connection, close_redis_connection};

pub type SopContext = Context;
