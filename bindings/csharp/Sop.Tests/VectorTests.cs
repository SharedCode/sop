using System;
using System.IO;
using System.Collections.Generic;
using System.Text.Json;
using Xunit;

namespace Sop.Tests;

public class VectorTests : IDisposable
{
    private const string DataDir = "data/vector_test";

    public VectorTests()
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
    public void TestVectorDbStandalone()
    {
        using var ctx = new Context();
        var db = new Database(new DatabaseOptions
        {
            StoresFolders = new List<string> { DataDir },
            Type = (int)DatabaseType.Standalone
        });

        // Transaction 1: Upsert
        var t1 = db.BeginTransaction(ctx);
        var store = db.OpenVectorStore(ctx, "products", t1);

        var item = new VectorItem
        {
            Id = "p1",
            Vector = new float[] { 0.1f, 0.1f, 0.1f },
            Payload = new Dictionary<string, object>() // Empty payload
        };
        store.Upsert(item);
        
        Assert.Equal(1, store.Count());
        
        // Query first to get ID
        var hits1 = store.Query(new float[] { 0.1f, 0.1f, 0.1f }, 1);
        Assert.Single(hits1);
        var id = hits1[0].Id;
        Assert.Equal("p1", id);

        // Now Get
        // FIXME: Get fails with "item not found" even though Query works and Count is 1.
        // This might be due to an issue in the Go binding or C# interop for Get.
        Assert.NotNull(store.Get(id));

        t1.Commit();

        // Transaction 2: Read
        var t2 = db.BeginTransaction(ctx);
        store = db.OpenVectorStore(ctx, "products", t2);

        Assert.Equal(1, store.Count());

        // Get
        Assert.NotNull(store.Get("p1")); // Comment out Get

        // Handle JsonElement
        // var cat = (JsonElement)fetched.Payload["cat"];
        // Assert.Equal("A", cat.GetString());

        // Query
        var hits = store.Query(new float[] { 0.1f, 0.1f, 0.1f }, 1);
        Assert.Single(hits);
        Assert.Equal("p1", hits[0].Id);
        
        // Delete
        store.Delete("p1");
        // Count includes soft-deleted items
        Assert.Equal(1, store.Count());
        Assert.Null(store.Get("p1"));

        // Query should not return deleted items
        hits = store.Query(new float[] { 0.1f, 0.1f, 0.1f }, 1);
        Assert.Empty(hits);

        t2.Commit();
    }

    [Fact]
    public void TestVectorDbBatch()
    {
        using var ctx = new Context();
        var db = new Database(new DatabaseOptions
        {
            StoresFolders = new List<string> { DataDir },
            Type = (int)DatabaseType.Standalone
        });

        var t = db.BeginTransaction(ctx);
        var store = db.OpenVectorStore(ctx, "products_batch", t);

        var items = new List<VectorItem>
        {
            new VectorItem { Id = "p1", Vector = new float[] { 0.1f, 0.1f, 0.1f }, Payload = new Dictionary<string, object>() },
            new VectorItem { Id = "p2", Vector = new float[] { 0.2f, 0.2f, 0.2f }, Payload = new Dictionary<string, object>() }
        };
        store.UpsertBatch(items);

        Assert.Equal(2, store.Count());

        t.Commit();
    }
}
