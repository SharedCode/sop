using System;
using System.IO;
using System.Collections.Generic;
using Xunit;

namespace Sop.Tests;

public class SearchTests : IDisposable
{
    private const string DataDir = "data/search_test";

    public SearchTests()
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
    public void TestSearchBasic()
    {
        using var ctx = new Context();
        var db = new Database(new DatabaseOptions
        {
            StoresFolders = new List<string> { DataDir },
            Type = (int)DatabaseType.Standalone
        });

        var t = db.BeginTransaction(ctx);
        var idx = db.OpenSearch(ctx, "my_index", t);
        
        idx.Add("doc1", "hello world");
        idx.Add("doc2", "hello python");
        
        t.Commit();

        // Search in new transaction
        t = db.BeginTransaction(ctx);
        idx = db.OpenSearch(ctx, "my_index", t);

        var results = idx.SearchQuery("hello");
        Assert.Equal(2, results.Count);

        results = idx.SearchQuery("python");
        Assert.Single(results);
        Assert.Equal("doc2", results[0].DocID);

        t.Commit();
    }
}
