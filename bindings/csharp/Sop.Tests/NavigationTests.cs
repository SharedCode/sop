using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Sop.Tests;

public class NavigationTests : IDisposable
{
    private const string DataDir = "data/nav_test";

    public NavigationTests()
    {
        if (Directory.Exists(DataDir))
        {
            Directory.Delete(DataDir, true);
        }
    }

    public void Dispose()
    {
        if (Directory.Exists(DataDir))
        {
            Directory.Delete(DataDir, true);
        }
    }

    [Fact]
    public void TestNavigation()
    {
        using var ctx = new Context();
        var db = new Database(new DatabaseOptions
        {
            StoresFolders = new List<string> { DataDir },
            Type = (int)DatabaseType.Standalone
        });

        var t = db.BeginTransaction(ctx);
        var b3 = db.NewBtree<int, string>(ctx, "nav_btree", t);

        // Add 100 items
        var items = new List<Item<int, string>>();
        for (int i = 0; i < 100; i++)
        {
            items.Add(new Item<int, string> { Key = i, Value = $"val{i}" });
        }
        b3.Add(ctx, items);
        t.Commit();

        // Test Forward Navigation
        t = db.BeginTransaction(ctx);
        b3 = db.OpenBtree<int, string>(ctx, "nav_btree", t);

        Assert.True(b3.First(ctx));

        // Fetch 10 items starting from offset 0
        var keys = b3.GetKeys(ctx, new PagingInfo
        {
            PageOffset = 0,
            PageSize = 10,
            FetchCount = 10,
            Direction = (int)PagingDirection.Forward
        });

        Assert.Equal(10, keys.Count);
        Assert.Equal(0, keys[0].Key);
        Assert.Equal(9, keys[9].Key);

        // Get Values for these keys
        var values = b3.GetValues(ctx, keys);
        Assert.Equal(10, values.Count);
        Assert.Equal("val0", values[0].Value);
        Assert.Equal("val9", values[9].Value);

        t.Commit();

        // Test Backward Navigation
        t = db.BeginTransaction(ctx);
        b3 = db.OpenBtree<int, string>(ctx, "nav_btree", t);

        Assert.True(b3.Last(ctx));

        // Fetch 10 items backwards
        keys = b3.GetKeys(ctx, new PagingInfo
        {
            PageOffset = 0,
            PageSize = 10,
            FetchCount = 10,
            Direction = (int)PagingDirection.Backward
        });

        Assert.Equal(10, keys.Count);
        // Backward: 99, 98, ... 90
        Assert.Equal(99, keys[0].Key);
        Assert.Equal(90, keys[9].Key);

        values = b3.GetValues(ctx, keys);
        Assert.Equal(10, values.Count);
        Assert.Equal("val99", values[0].Value);
        Assert.Equal("val90", values[9].Value);

        t.Commit();
    }
}
