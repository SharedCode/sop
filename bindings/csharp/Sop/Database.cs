using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sop;

/// <summary>
/// Specifies the type of database deployment.
/// </summary>
public enum DatabaseType
{
    /// <summary>
    /// Single node database.
    /// </summary>
    Standalone = 0,
    /// <summary>
    /// Distributed database cluster.
    /// </summary>
    Clustered = 1
}

/// <summary>
/// Configuration for erasure coding (redundancy).
/// </summary>
public class ErasureCodingConfig
{
    /// <summary>
    /// Number of data shards.
    /// </summary>
    [JsonPropertyName("data_shards_count")]
    public int DataShards { get; set; }

    /// <summary>
    /// Number of parity shards.
    /// </summary>
    [JsonPropertyName("parity_shards_count")]
    public int ParityShards { get; set; }

	// BaseFolderPathsAcrossDrives lists the drive base paths where data and parity shard files are stored.
    [JsonPropertyName("base_folder_paths_across_drives")]
	public string[] BaseFolderPathsAcrossDrives{ get; set; }

	// RepairCorruptedShards indicates whether to attempt automatic repair when corrupted shards are detected.
	// Auto-repair can be expensive; applications can disable it(default) to prioritize throughput and handle drive
	// failures via external workflows.
    [JsonPropertyName("repair_corrupted_shards")]
	public bool RepairCorruptedShards { get; set; }
}

/// <summary>
/// Configuration options for creating a database.
/// </summary>
public class DatabaseOptions
{
    /// <summary>
    /// List of folders where data stores are located.
    /// </summary>
    [JsonPropertyName("stores_folders")]
    public List<string> StoresFolders { get; set; }

    /// <summary>
    /// The type of database (Standalone/Clustered).
    /// </summary>
    [JsonPropertyName("type")]
    public int Type { get; set; }

    /// <summary>
    /// The keyspace name (for clustered setups).
    /// </summary>
    [JsonPropertyName("keyspace")]
    public string Keyspace { get; set; }

    /// <summary>
    /// Redis configuration for clustered mode.
    /// </summary>
    [JsonPropertyName("redis_config")]
    public RedisConfig RedisConfig { get; set; }

    /// <summary>
    /// Erasure coding configuration per store.
    /// </summary>
    [JsonPropertyName("erasure_config")]
    public Dictionary<string, ErasureCodingConfig> ErasureConfig { get; set; }
}

/// <summary>
/// Configuration for Redis connection.
/// </summary>
public class RedisConfig
{
    /// <summary>
    /// The address of the Redis server (host:port).
    /// </summary>
    [JsonPropertyName("address")]
    public string Address { get; set; }

    /// <summary>
    /// The password for Redis authentication.
    /// </summary>
    [JsonPropertyName("password")]
    public string Password { get; set; }

    /// <summary>
    /// The database index to use.
    /// </summary>
    [JsonPropertyName("db")]
    public int DB { get; set; }

    /// <summary>
    /// The full connection URL (overrides other fields).
    /// </summary>
    [JsonPropertyName("url")]
    public string URL { get; set; }
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
    RemoveBtree = 8,
    RemoveModelStore = 9,
    RemoveVectorStore = 10,
    RemoveSearch = 11,
    SetupDatabase = 12
}

/// <summary>
/// Represents a database instance.
/// </summary>
public class Database
{
    /// <summary>
    /// The unique identifier of the database.
    /// </summary>
    public Guid Id { get; private set; }
    private readonly DatabaseOptions _options;

    /// <summary>
    /// Initializes a new instance of the Database class with the specified options.
    /// </summary>
    /// <param name="options">Configuration options.</param>
    public Database(DatabaseOptions options)
    {
        _options = options;
    }

    /// <summary>
    /// Setup persists the database options to the stores folders.
    /// This is a one-time setup operation for the database.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="options">Configuration options.</param>
    /// <exception cref="SopException">Thrown if setup fails.</exception>
    public static void Setup(Context ctx, DatabaseOptions options)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(options);
        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)DatabaseAction.SetupDatabase, null, payload);
        var res = Interop.FromPtr(resPtr);
        if (res != null)
        {
            throw new SopException(res);
        }
    }

    /// <summary>
    /// Begins a new transaction.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="mode">The transaction mode (Read/Write).</param>
    /// <param name="maxTime">Maximum duration of the transaction in seconds.</param>
    /// <returns>A new Transaction object.</returns>
    /// <exception cref="SopException">Thrown if transaction creation fails.</exception>
    public Transaction BeginTransaction(Context ctx, TransactionMode mode = TransactionMode.ForWriting, int maxTime = 15)
    {
        EnsureDatabaseCreated(ctx);

        var opts = new { mode = (int)mode, max_time = maxTime };
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

    /// <summary>
    /// Removes a B-Tree store from the database.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="name">The name of the store to remove.</param>
    public void RemoveBtree(Context ctx, string name)
    {
        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)DatabaseAction.RemoveBtree, Interop.ToBytes(Id.ToString()), Interop.ToBytes(name));
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    public void RemoveModelStore(Context ctx, string name)
    {
        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)DatabaseAction.RemoveModelStore, Interop.ToBytes(Id.ToString()), Interop.ToBytes(name));
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    public void RemoveVectorStore(Context ctx, string name)
    {
        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)DatabaseAction.RemoveVectorStore, Interop.ToBytes(Id.ToString()), Interop.ToBytes(name));
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    public void RemoveSearch(Context ctx, string name)
    {
        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)DatabaseAction.RemoveSearch, Interop.ToBytes(Id.ToString()), Interop.ToBytes(name));
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
