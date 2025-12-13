package com.sharedcode.sop;

import java.util.List;
import java.util.Map;
import java.util.Collections;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DatabaseOptions {
    @JsonProperty("stores_folders")
    public List<String> stores_folders = Collections.singletonList(".");
    
    @JsonProperty("type")
    public DatabaseType type = DatabaseType.Standalone;
    
    @JsonProperty("keyspace")
    public String keyspace;
    
    @JsonProperty("erasure_config")
    public Map<String, ErasureCodingConfig> erasure_config;
    
    @JsonProperty("cache_type")
    public int cache_type;

    public static class ErasureCodingConfig {
        @JsonProperty("data_shards")
        public int data_shards;
        
        @JsonProperty("parity_shards")
        public int parity_shards;
    }
}
