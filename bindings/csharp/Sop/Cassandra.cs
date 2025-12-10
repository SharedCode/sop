using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Sop;

public class CassandraAuthenticator
{
    [JsonPropertyName("username")]
    public string Username { get; set; }

    [JsonPropertyName("password")]
    public string Password { get; set; }
}

public class CassandraConfig
{
    [JsonPropertyName("cluster_hosts")]
    public List<string> ClusterHosts { get; set; }

    [JsonPropertyName("consistency")]
    public int Consistency { get; set; }

    [JsonPropertyName("connection_timeout")]
    public int ConnectionTimeout { get; set; }

    [JsonPropertyName("replication_clause")]
    public string ReplicationClause { get; set; }

    [JsonPropertyName("authenticator")]
    public CassandraAuthenticator Authenticator { get; set; }
}

public static class Cassandra
{
    /// <summary>
    /// Initializes the global shared Cassandra connection.
    /// </summary>
    public static void Initialize(CassandraConfig config)
    {
        var json = System.Text.Json.JsonSerializer.Serialize(config);
        var resPtr = NativeMethods.OpenCassandraConnection(Interop.ToBytes(json));
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    /// <summary>
    /// Closes the global shared Cassandra connection.
    /// </summary>
    public static void Close()
    {
        var resPtr = NativeMethods.CloseCassandraConnection();
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }
}
