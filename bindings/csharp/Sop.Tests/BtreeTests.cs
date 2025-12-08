using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json.Serialization;
using Xunit;
using Sop;

namespace Sop.Tests;

public class BtreeTests : IDisposable
{
    private readonly string _tempDir;

    public BtreeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sop_test_" + Guid.NewGuid());
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, true);
        }
    }

    [Fact]
    public void TestUserBtreeCudBatch()
    {
        using var ctx = new Context();
        var dbOpts = new DatabaseOptions 
        { 
            StoresFolders = new List<string> { _tempDir },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOpts);

        // 1. Create (Insert)
        using (var t = db.BeginTransaction(ctx))
        {
            var btree = db.NewBtree<string, string>(ctx, "users", t);
            var items = new List<Item<string, string>>();
            for (int i = 0; i < 100; i++)
            {
                items.Add(new Item<string, string>($"user_{i}", $"User Name {i}"));
            }
            btree.Add(ctx, items);
            t.Commit();
        }

        // Verify Insert
        using (var t = db.BeginTransaction(ctx))
        {
            var btree = db.NewBtree<string, string>(ctx, "users", t);
            
            Assert.Equal(100, btree.Count());
            
            Assert.True(btree.Find(ctx, "user_50"));
            var items = btree.GetValues(ctx, "user_50");
            Assert.Single(items);
            Assert.Equal("User Name 50", items[0].Value);
        }

        // 2. Update
        using (var t = db.BeginTransaction(ctx))
        {
            var btree = db.NewBtree<string, string>(ctx, "users", t);
            var items = new List<Item<string, string>>();
            for (int i = 0; i < 100; i++)
            {
                items.Add(new Item<string, string>($"user_{i}", $"Updated User {i}"));
            }
            btree.Update(ctx, items);
            t.Commit();
        }

        // Verify Update
        using (var t = db.BeginTransaction(ctx))
        {
            var btree = db.NewBtree<string, string>(ctx, "users", t);
            var items = btree.GetValues(ctx, "user_50");
            Assert.Single(items);
            Assert.Equal("Updated User 50", items[0].Value);
        }

        // 3. Delete
        using (var t = db.BeginTransaction(ctx))
        {
            var btree = db.NewBtree<string, string>(ctx, "users", t);
            var keys = new List<string>();
            for (int i = 0; i < 100; i++)
            {
                keys.Add($"user_{i}");
            }
            btree.Remove(ctx, keys);
            t.Commit();
        }

        // Verify Delete
        using (var t = db.BeginTransaction(ctx))
        {
            var btree = db.NewBtree<string, string>(ctx, "users", t);
            Assert.Equal(0, btree.Count());
        }
    }

    [Fact]
    public void TestComplexKey()
    {
        using var ctx = new Context();
        var dbOpts = new DatabaseOptions 
        { 
            StoresFolders = new List<string> { _tempDir },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOpts);

        var indexSpec = new IndexSpecification
        {
            IndexFields = new List<IndexFieldSpecification>
            {
                new IndexFieldSpecification { FieldName = "Region", AscendingSortOrder = true },
                new IndexFieldSpecification { FieldName = "Id", AscendingSortOrder = true }
            }
        };

        using (var t = db.BeginTransaction(ctx))
        {
            var opts = new BtreeOptions("complex") { IndexSpec = indexSpec };
            var btree = db.NewBtree<ComplexKey, string>(ctx, "complex", t, opts);

            btree.Add(ctx, new Item<ComplexKey, string>(new ComplexKey { Region = "US", Id = 1 }, "Val1"));
            btree.Add(ctx, new Item<ComplexKey, string>(new ComplexKey { Region = "EU", Id = 2 }, "Val2"));
            
            t.Commit();
        }

        using (var t = db.BeginTransaction(ctx))
        {
            var btree = db.OpenBtree<ComplexKey, string>(ctx, "complex", t);
            
            bool found1 = btree.Find(ctx, new ComplexKey { Region = "US", Id = 1 });
            Console.WriteLine($"Find US/1: {found1}");
            Assert.True(found1);

            bool found2 = btree.Find(ctx, new ComplexKey { Region = "US", Id = 2 });
            Console.WriteLine($"Find US/2: {found2}");
            if (found2)
            {
                var k = btree.GetCurrentKey(ctx);
                Console.WriteLine($"Found Key: {k.Key.Region}/{k.Key.Id}");
            }
            Assert.False(found2);
        }
    }

    public class ComplexKey
    {
        public string Region { get; set; }
        public int Id { get; set; }
    }
}
