using System;
using System.Collections.Generic;
using Xunit;

namespace Sop.Tests;

public class CassandraTests
{
    [Fact]
    public void TestCassandraInitialization()
    {
        var config = new CassandraConfig
        {
            ClusterHosts = new List<string> { "localhost" },
            Keyspace = "sop_test",
            Consistency = 1, // LocalQuorum
            ConnectionTimeout = 5000,
            ReplicationClause = "{'class':'SimpleStrategy', 'replication_factor':1}",
            Authenticator = new CassandraAuthenticator
            {
                Username = "",
                Password = ""
            }
        };

        try
        {
            Cassandra.Initialize(config);
        }
        catch (SopException e)
        {
            // Check if it's a connection error (expected) vs a binding error (unexpected)
            if (!e.Message.Contains("connection refused") && 
                !e.Message.Contains("dial tcp") && 
                !e.Message.Contains("failed to create cassandra session"))
            {
                throw;
            }
        }
        finally
        {
            try
            {
                Cassandra.Close();
            }
            catch
            {
                // Ignore close errors
            }
        }
    }
}
