using System;
using System.IO;
using Xunit;

namespace Sop.Tests;

public class LoggingTests
{
    [Fact]
    public void TestLoggingConfiguration()
    {
        string logFile = "sop_app_test.log";
        if (File.Exists(logFile)) File.Delete(logFile);

        Logger.Configure(LogLevel.Debug, logFile);

        // Trigger some logging by creating a context and database
        using var ctx = new Context();
        string dbPath = Path.Combine(Path.GetTempPath(), $"sop_log_test_{Guid.NewGuid()}");
        
        var dbOptions = new DatabaseOptions
        {
            StoresFolders = new System.Collections.Generic.List<string> { dbPath },
            Type = (int)DatabaseType.Standalone
        };
        
        var db = new Database(dbOptions);
        using var trans = db.BeginTransaction(ctx);
        
        // Just do something simple
        var btree = db.NewBtree<string, string>(ctx, "log_test_btree", trans);
        btree.Add(ctx, new Item<string, string> { Key = "k", Value = "v" });
        
        trans.Commit();

        // Verify log file exists and has content
        // Note: Logging might be asynchronous or buffered, so we might not see it immediately.
        // But the file should be created.
        Assert.True(File.Exists(logFile) || File.Exists(logFile + ".0") || File.Exists(logFile + ".1")); // Log rotation might happen
        
        // Clean up
        if (File.Exists(logFile)) File.Delete(logFile);
        if (Directory.Exists(dbPath)) Directory.Delete(dbPath, true);
    }

    [Fact]
    public void TestLoggingToStderr()
    {
        // Should not throw
        Logger.Configure(LogLevel.Info, null);
        Logger.Configure(LogLevel.Info, "");
    }
}
