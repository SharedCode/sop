use crate::ffi::*;
use serde::{Serialize, Deserialize};
use std::ffi::CString;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CassandraAuthenticator {
    #[serde(rename = "username")]
    pub username: String,
    #[serde(rename = "password")]
    pub password: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CassandraConfig {
    #[serde(rename = "cluster_hosts")]
    pub cluster_hosts: Vec<String>,
    #[serde(rename = "consistency")]
    pub consistency: i32,
    #[serde(rename = "connection_timeout")]
    pub connection_timeout: i32,
    #[serde(rename = "replication_clause")]
    pub replication_clause: String,
    #[serde(rename = "authenticator")]
    pub authenticator: CassandraAuthenticator,
}

pub fn open_cassandra_connection(config: CassandraConfig) -> Result<(), String> {
    let payload = serde_json::to_string(&config).map_err(|e| e.to_string())?;
    let c_payload = CString::new(payload).unwrap();
    unsafe {
        let ptr = openCassandraConnection(c_payload.into_raw());
        let res = crate::utils::process_go_result(ptr);
        if let Some(err_str) = res {
            if err_str.is_empty() {
                Ok(())
            } else {
                Err(err_str)
            }
        } else {
            Ok(())
        }
    }
}

pub fn close_cassandra_connection() -> Result<(), String> {
    unsafe {
        let ptr = closeCassandraConnection();
        let res = crate::utils::process_go_result(ptr);
        if let Some(err_str) = res {
            if err_str.is_empty() {
                Ok(())
            } else {
                Err(err_str)
            }
        } else {
            Ok(())
        }
    }
}
