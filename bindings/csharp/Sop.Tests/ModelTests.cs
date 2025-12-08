using System;
using System.IO;
using System.Collections.Generic;
using System.Text.Json;
using Xunit;

namespace Sop.Tests;

public class ModelTests : IDisposable
{
    private const string DataDir = "data/model_test";

    public ModelTests()
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
    public void TestModelStore()
    {
        using var ctx = new Context();
        var db = new Database(new DatabaseOptions
        {
            StoresFolders = new List<string> { DataDir },
            Type = (int)DatabaseType.Standalone
        });

        var t = db.BeginTransaction(ctx);
        var ms = db.OpenModelStore(ctx, "finance", t);

        var modelData = new Dictionary<string, object>
        {
            { "type", "linear_regression" },
            { "weights", new double[] { 1.0, 2.0 } }
        };

        ms.Save("regression", "model1", modelData);
        
        t.Commit();

        // Read in new transaction
        var t2 = db.BeginTransaction(ctx);
        ms = db.OpenModelStore(ctx, "finance", t2);

        var loadedModel = ms.Load<Dictionary<string, object>>("regression", "model1");
        Assert.NotNull(loadedModel);
        
        // Handle JsonElement
        var type = (JsonElement)loadedModel["type"];
        Assert.Equal("linear_regression", type.GetString());

        var models = ms.List("regression");
        Assert.Contains("model1", models);

        t2.Commit();
    }
}
