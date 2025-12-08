using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Sop.Tests;

public class BtreeBatchTests : IDisposable
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
        string path = Path.Combine(Path.GetTempPath(), $"sop_cud_test_{suffix}_{Guid.NewGuid()}");
        _testDirs.Add(path);
        return path;
    }

    [Fact]
    public void TestUserBtreeCudBatch()
    {
        string path = CreateTempDir("user_btree");
        using var ctx = new Context();

        var dbOptions = new DatabaseOptions
        {
            StoresFolders = new List<string> { path },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOptions);

        // 1. Create (Insert)
        using (var t = db.BeginTransaction(ctx))
        {
            var bo = new BtreeOptions("users")
            {
                IsUnique = true
            };
            
            var b3 = db.NewBtree<string, string>(ctx, "users", t, bo);
            
            var items = new List<Item<string, string>>();
            for (int i = 0; i < 100; i++)
            {
                items.Add(new Item<string, string> { Key = $"user_{i}", Value = $"User Name {i}" });
            }
            
            b3.Add(ctx, items);
            t.Commit();
        }

        // Verify Insert
        using (var t = db.BeginTransaction(ctx))
        {
            var b3 = db.OpenBtree<string, string>(ctx, "users", t);
            long count = b3.Count();
            Assert.Equal(100, count);
            
            bool found = b3.Find(ctx, "user_50");
            Assert.True(found);
            
            var items = b3.GetValues(ctx, new List<Item<string, string>> { new Item<string, string> { Key = "user_50" } });
            Assert.Single(items);
            Assert.Equal("User Name 50", items[0].Value);
            t.Commit();
        }

        // 2. Update
        using (var t = db.BeginTransaction(ctx))
        {
            var b3 = db.OpenBtree<string, string>(ctx, "users", t);
            
            var items = new List<Item<string, string>>();
            for (int i = 0; i < 100; i++)
            {
                items.Add(new Item<string, string> { Key = $"user_{i}", Value = $"Updated User {i}" });
            }
            
            b3.Update(ctx, items);
            t.Commit();
        }

        // Verify Update
        using (var t = db.BeginTransaction(ctx))
        {
            var b3 = db.OpenBtree<string, string>(ctx, "users", t);
            var items = b3.GetValues(ctx, new List<Item<string, string>> { new Item<string, string> { Key = "user_50" } });
            Assert.Single(items);
            Assert.Equal("Updated User 50", items[0].Value);
            t.Commit();
        }

        // 3. Delete
        using (var t = db.BeginTransaction(ctx))
        {
            var b3 = db.OpenBtree<string, string>(ctx, "users", t);
            
            var keys = new List<string>();
            for (int i = 0; i < 100; i++)
            {
                keys.Add($"user_{i}");
            }
            
            b3.Remove(ctx, keys);
            t.Commit();
        }

        // Verify Delete
        using (var t = db.BeginTransaction(ctx))
        {
            var b3 = db.OpenBtree<string, string>(ctx, "users", t);
            long count = b3.Count();
            Assert.Equal(0, count);
            t.Commit();
        }
    }
}
