package com.sharedcode.sop;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class CassandraConfig {
    @JsonProperty("cluster_hosts")
    public List<String> clusterHosts;

    @JsonProperty("consistency")
    public int consistency;

    @JsonProperty("connection_timeout")
    public int connectionTimeout;

    @JsonProperty("replication_clause")
    public String replicationClause;

    @JsonProperty("authenticator")
    public CassandraAuthenticator authenticator;

    public static class CassandraAuthenticator {
        @JsonProperty("username")
        public String username;

        @JsonProperty("password")
        public String password;
    }
}
