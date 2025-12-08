using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Sop.Tests;

public class ClusteredDatabaseTests : IDisposable
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

    private string CreateTempDir(string suffix)
    {
        string path = Path.Combine(Path.GetTempPath(), $"sop_clustered_test_{suffix}_{Guid.NewGuid()}");
        _testDirs.Add(path);
        return path;
    }

    [Fact]
    public void TestClusteredDatabase()
    {
        // Note: This test might fail if Redis is not available, as Clustered mode usually requires Redis.
        // However, we can wrap it in a try-catch to ignore connection errors if that's the only issue,
        // verifying that the C# binding correctly attempts to use Clustered mode.
        
        string path = CreateTempDir("clustered_db");
        using var ctx = new Context();

        var dbOptions = new DatabaseOptions
        {
            StoresFolders = new List<string> { path },
            Type = (int)DatabaseType.Clustered
        };
        var db = new Database(dbOptions);

        try
        {
            // Try to begin a transaction. This will trigger database creation/opening.
            // In Clustered mode, this might try to connect to Redis.
            using var t = db.BeginTransaction(ctx);
            
            // If we get here, it means either Redis is running or not required for this step.
            var b3 = db.NewBtree<string, string>(ctx, "clustered_btree", t);
            b3.Add(ctx, new Item<string, string> { Key = "k", Value = "v" });
            
            t.Commit();
        }
        catch (SopException e)
        {
            // If it fails because of Redis connection, that's expected in this environment if Redis isn't running.
            // We just want to ensure it's not a binding error (like invalid enum value).
            if (!e.Message.Contains("connection refused") && 
                !e.Message.Contains("connect: connection refused") &&
                !e.Message.Contains("redis"))
            {
                throw;
            }
        }
    }
}
