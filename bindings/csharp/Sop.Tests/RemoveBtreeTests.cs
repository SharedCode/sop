using System;
using System.Collections.Generic;
using System.IO;
using Xunit;
using Sop;

namespace Sop.Tests;

public class RemoveBtreeTests : IDisposable
{
    private readonly string _tempDir;

    public RemoveBtreeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sop_test_remove_" + Guid.NewGuid());
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
    public void TestRemoveBtree()
    {
        using var ctx = new Context();
        var dbOpts = new DatabaseOptions 
        { 
            StoresFolders = new List<string> { _tempDir },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOpts);

        // 2. Create a store to delete
        using (var t = db.BeginTransaction(ctx))
        {
            var store = db.NewBtree<string, string>(ctx, "temp_store", t);
            store.Add(ctx, new Item<string, string>("foo", "bar"));
            t.Commit();
        }

        // 3. Remove the store
        db.RemoveBtree(ctx, "temp_store");

        // 4. Verify removal
        using (var t = db.BeginTransaction(ctx))
        {
            // Should throw exception because store not found
            Assert.Throws<SopException>(() => db.OpenBtree<string, string>(ctx, "temp_store", t));
        }
    }
}
