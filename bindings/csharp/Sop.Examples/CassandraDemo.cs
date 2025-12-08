using System;
using System.Collections.Generic;

namespace Sop.Examples
{
    public static class CassandraDemo
    {
        public static void Run()
        {
            Console.WriteLine("--- Cassandra Demo ---");
            Console.WriteLine("Note: This demo requires a running Cassandra instance on localhost.");

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
                Console.WriteLine("Initializing Cassandra connection...");
                Cassandra.Initialize(config);
                Console.WriteLine("Cassandra initialized successfully.");

                // If successful, we could try to create a Clustered database
                // But for this demo, just initialization is enough to show the API.
            }
            catch (SopException e)
            {
                Console.WriteLine($"Cassandra initialization failed (expected if not running): {e.Message}");
            }
            finally
            {
                try
                {
                    Console.WriteLine("Closing Cassandra connection...");
                    Cassandra.Close();
                }
                catch { }
            }

            Console.WriteLine("--- End of Cassandra Demo ---");
        }
    }
}
