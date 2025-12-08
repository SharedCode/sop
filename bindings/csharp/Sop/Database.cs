using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sop;

public enum DatabaseType
{
    Standalone = 0,
    Clustered = 1
}

public class ErasureCodingConfig
{
    [JsonPropertyName("data_shards")]
    public int DataShards { get; set; }

    [JsonPropertyName("parity_shards")]
    public int ParityShards { get; set; }
}

public class DatabaseOptions
{
    [JsonPropertyName("stores_folders")]
    public List<string> StoresFolders { get; set; }

    [JsonPropertyName("type")]
    public int Type { get; set; }

    [JsonPropertyName("keyspace")]
    public string Keyspace { get; set; }

    [JsonPropertyName("erasure_config")]
    public Dictionary<string, ErasureCodingConfig> ErasureConfig { get; set; }
}

internal enum DatabaseAction
{
    NewDatabase = 1,
    BeginTransaction = 2,
    NewBtree = 3,
    OpenBtree = 4,
    OpenModelStore = 5,
    OpenVectorStore = 6,
    OpenSearch = 7,
    RemoveBtree = 8
}

public class Database
{
    public Guid Id { get; private set; }
    private readonly DatabaseOptions _options;

    public Database(DatabaseOptions options)
    {
        _options = options;
    }

    public Transaction BeginTransaction(Context ctx, int mode = 1, int maxTime = 15)
    {
        EnsureDatabaseCreated(ctx);

        var opts = new { mode = mode, max_time = maxTime };
        var payload = JsonSerializer.SerializeToUtf8Bytes(opts);

        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)DatabaseAction.BeginTransaction, Interop.ToBytes(Id.ToString()), payload);
        var res = Interop.FromPtr(resPtr);

        if (Guid.TryParse(res, out Guid tid))
        {
            return new Transaction(ctx, tid, true, Id);
        }
        throw new SopException(res);
    }

    private void EnsureDatabaseCreated(Context ctx)
    {
        if (Id != Guid.Empty) return;

        var payload = JsonSerializer.SerializeToUtf8Bytes(_options);
        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)DatabaseAction.NewDatabase, null, payload);
        var res = Interop.FromPtr(resPtr);

        if (Guid.TryParse(res, out Guid dbId))
        {
            Id = dbId;
        }
        else
        {
            throw new SopException(res);
        }
    }

    public void RemoveBtree(Context ctx, string name)
    {
        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)DatabaseAction.RemoveBtree, Interop.ToBytes(Id.ToString()), Interop.ToBytes(name));
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    public Btree<TK, TV> NewBtree<TK, TV>(Context ctx, string name, Transaction trans, BtreeOptions options = null)
    {
        EnsureDatabaseCreated(ctx);
        return Btree<TK, TV>.New(ctx, name, trans, options);
    }

    public Btree<TK, TV> OpenBtree<TK, TV>(Context ctx, string name, Transaction trans)
    {
        var opts = new { transaction_id = trans.Id.ToString(), name = name };
        return OpenStore(ctx, DatabaseAction.OpenBtree, opts, id => 
        {
            bool isPrimitive = typeof(TK).IsPrimitive || typeof(TK) == typeof(string);
            return new Btree<TK, TV>(id, trans.Id, isPrimitive);
        });
    }

    public Search OpenSearch(Context ctx, string name, Transaction trans)
    {
        var opts = new { transaction_id = trans.Id.ToString(), name = name };
        return OpenStore(ctx, DatabaseAction.OpenSearch, opts, id => new Search(ctx, trans, id));
    }

    public VectorStore OpenVectorStore(Context ctx, string name, Transaction trans, VectorStoreConfig config = null)
    {
        if (config == null) config = new VectorStoreConfig();
        
        var opts = new 
        { 
            transaction_id = trans.Id.ToString(), 
            name = name,
            config = config
        };
        return OpenStore(ctx, DatabaseAction.OpenVectorStore, opts, id => new VectorStore(ctx, trans, id));
    }

    public ModelStore OpenModelStore(Context ctx, string path, Transaction trans)
    {
        var opts = new { transaction_id = trans.Id.ToString(), path = path };
        return OpenStore(ctx, DatabaseAction.OpenModelStore, opts, id => new ModelStore(ctx, trans, id));
    }

    private T OpenStore<T>(Context ctx, DatabaseAction action, object options, Func<Guid, T> factory)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(options);
        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)action, Interop.ToBytes(Id.ToString()), payload);
        var res = Interop.FromPtr(resPtr);

        if (Guid.TryParse(res, out Guid storeId))
        {
            return factory(storeId);
        }
        throw new SopException(res);
    }
}
