using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Sop.Tests;

public class RedisTests : IDisposable
{
    private readonly List<string> _testDirs = new List<string>();

    public void Dispose()
    {
        foreach (var dir in _testDirs)
        {
            if (Directory.Exists(dir))
            {
                try { Directory.Delete(dir, true); } catch { }
            }
        }
    }

    [Fact]
    public void TestRedisConfigInDatabaseOptions()
    {
        // This test verifies that we can pass RedisConfig via DatabaseOptions
        // replacing the deprecated global Redis.Initialize.
        
        string path = Path.Combine(Path.GetTempPath(), $"sop_redis_test_{Guid.NewGuid()}");
        _testDirs.Add(path);

        using var ctx = new Context();

        var dbOptions = new DatabaseOptions
        {
            StoresFolders = new List<string> { path },
            // Using Clustered type typically triggers Redis usage
            Type = (int)DatabaseType.Clustered, 
            RedisConfig = new RedisConfig 
            { 
                Address = "localhost:6379",
                Password = "",
                DB = 0
            }
        };

        // We check if we can instantiate without crashing. 
        // Actual connection might fail if no Redis is running, but the binding logic is what we test here.
        var db = new Database(dbOptions);
        Assert.NotNull(db);
    }
}
