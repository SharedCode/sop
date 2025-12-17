using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sop;

/// <summary>
/// Configuration options for creating or opening a B-Tree store.
/// </summary>
public class BtreeOptions
{
    /// <summary>
    /// The name of the B-Tree store.
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    /// <summary>
    /// If true, the B-Tree will enforce unique keys.
    /// </summary>
    [JsonPropertyName("is_unique")]
    public bool IsUnique { get; set; } = false;

    /// <summary>
    /// Set to true if the key is a primitive type (int, string, etc.). 
    /// Set to false for complex objects/structs.
    /// </summary>
    [JsonPropertyName("is_primitive_key")]
    public bool IsPrimitiveKey { get; set; } = true;

    /// <summary>
    /// The number of items per node. Higher values mean flatter trees but larger nodes.
    /// Default is 500.
    /// </summary>
    [JsonPropertyName("slot_length")]
    public int SlotLength { get; set; } = 500;

    /// <summary>
    /// Optional description of the store.
    /// </summary>
    [JsonPropertyName("description")]
    public string Description { get; set; } = "";

    /// <summary>
    /// If true, values are stored directly in the B-Tree node. 
    /// Best for small values.
    /// </summary>
    [JsonPropertyName("is_value_data_in_node_segment")]
    public bool IsValueDataInNodeSegment { get; set; } = true;

    /// <summary>
    /// If true, values are stored in a separate segment and persisted immediately.
    /// Best for large values (BLOBs).
    /// </summary>
    [JsonPropertyName("is_value_data_actively_persisted")]
    public bool IsValueDataActivelyPersisted { get; set; } = false;

    /// <summary>
    /// If true, values are cached globally across transactions.
    /// </summary>
    [JsonPropertyName("is_value_data_globally_cached")]
    public bool IsValueDataGloballyCached { get; set; } = false;

    /// <summary>
    /// If true, enables load balancing for leaf nodes during updates.
    /// </summary>
    [JsonPropertyName("leaf_load_balancing")]
    public bool LeafLoadBalancing { get; set; } = false;

    /// <summary>
    /// Advanced caching configuration for this store.
    /// </summary>
    [JsonPropertyName("cache_config")]
    public CacheConfig CacheConfig { get; set; }

    /// <summary>
    /// Strongly typed Index Specification.
    /// </summary>
    [JsonIgnore]
    public IndexSpecification IndexSpecification { get; set; }

    /// <summary>
    /// JSON string defining the index specification for complex keys.
    /// Used for serialization to the underlying storage engine.
    /// </summary>
    [JsonPropertyName("index_specification")]
    public string IndexSpecificationJson 
    { 
        get => IndexSpecification != null ? JsonSerializer.Serialize(IndexSpecification) : null;
        set => IndexSpecification = value != null ? JsonSerializer.Deserialize<IndexSpecification>(value) : null;
    }

    [JsonPropertyName("transaction_id")]
    public string TransactionId { get; set; }
    
    public BtreeOptions(string name)
    {
        Name = name;
    }

    /// <summary>
    /// Helper method to configure storage flags based on expected value size.
    /// </summary>
    /// <param name="s">The expected size category of the values.</param>
    public void SetValueDataSize(ValueDataSize s)
    {
        if (s == ValueDataSize.Medium)
        {
            IsValueDataActivelyPersisted = false;
            IsValueDataGloballyCached = true;
            IsValueDataInNodeSegment = false;
        }
        else if (s == ValueDataSize.Big)
        {
            IsValueDataActivelyPersisted = true;
            IsValueDataGloballyCached = false;
            IsValueDataInNodeSegment = false;
        }
    }
}

/// <summary>
/// Hints for optimizing storage based on value size.
/// </summary>
public enum ValueDataSize
{
    /// <summary>
    /// Small values (e.g. integers, short strings). Stored in-node.
    /// </summary>
    Small = 0,
    /// <summary>
    /// Medium values. Stored separately but cached.
    /// </summary>
    Medium = 1,
    /// <summary>
    /// Large values (e.g. images, documents). Stored separately, actively persisted.
    /// </summary>
    Big = 2
}

/// <summary>
/// Configuration for the store's cache behavior.
/// </summary>
public class CacheConfig
{
    [JsonPropertyName("registry_cache_duration")]
    public int RegistryCacheDuration { get; set; } = 10;

    /// <summary>
    /// If true, the registry cache duration is treated as a Time-To-Live (TTL).
    /// </summary>
    [JsonPropertyName("is_registry_cache_ttl")]
    public bool IsRegistryCacheTtl { get; set; } = false;

    /// <summary>
    /// Duration (in minutes) to cache B-Tree nodes.
    /// </summary>
    [JsonPropertyName("node_cache_duration")]
    public int NodeCacheDuration { get; set; } = 5;

    /// <summary>
    /// If true, the node cache duration is treated as a TTL.
    /// </summary>
    [JsonPropertyName("is_node_cache_ttl")]
    public bool IsNodeCacheTtl { get; set; } = false;

    /// <summary>
    /// Duration (in minutes) to cache store information.
    /// </summary>
    [JsonPropertyName("store_info_cache_duration")]
    public int StoreInfoCacheDuration { get; set; } = 5;

    /// <summary>
    /// If true, the store info cache duration is treated as a TTL.
    /// </summary>
    [JsonPropertyName("is_store_info_cache_ttl")]
    public bool IsStoreInfoCacheTtl { get; set; } = false;

    /// <summary>
    /// Duration (in minutes) to cache value data (if globally cached).
    /// </summary>
    [JsonPropertyName("value_data_cache_duration")]
    public int ValueDataCacheDuration { get; set; } = 0;

    /// <summary>
    /// If true, the value data cache duration is treated as a TTL.
    /// </summary>
    [JsonPropertyName("is_value_data_cache_ttl")]
    public bool IsValueDataCacheTtl { get; set; } = false;
}

/// <summary>
/// Specifies a field to be included in a composite index.
/// </summary>
public class IndexFieldSpecification
{
    /// <summary>
    /// The name of the field/property in the key object.
    /// </summary>
    [JsonPropertyName("field_name")]
    public string FieldName { get; set; }

    /// <summary>
    /// If true, sorts in ascending order. False for descending.
    /// </summary>
    [JsonPropertyName("ascending_sort_order")]
    public bool AscendingSortOrder { get; set; } = true;
}

/// <summary>
/// Defines the structure of a composite index for complex keys.
/// </summary>
public class IndexSpecification
{
    /// <summary>
    /// List of fields that make up the index.
    /// </summary>
    [JsonPropertyName("index_fields")]
    public List<IndexFieldSpecification> IndexFields { get; set; } = new List<IndexFieldSpecification>();
}

internal class ManageBtreeMetaData
{
    [JsonPropertyName("is_primitive_key")]
    public bool IsPrimitiveKey { get; set; }

    [JsonPropertyName("transaction_id")]
    public string TransactionId { get; set; }

    [JsonPropertyName("btree_id")]
    public string BtreeId { get; set; }
}

internal class ManageBtreePayload<TK, TV>
{
    [JsonPropertyName("items")]
    public List<Item<TK, TV>> Items { get; set; }

    [JsonPropertyName("paging_info")]
    public PagingInfo PagingInfo { get; set; }
}

/// <summary>
/// Direction of pagination.
/// </summary>
public enum PagingDirection
{
    /// <summary>
    /// Move forward through the result set.
    /// </summary>
    Forward = 0,
    /// <summary>
    /// Move backward through the result set.
    /// </summary>
    Backward = 1
}

internal enum BtreeAction
{
    Add = 1,
    AddIfNotExist = 2,
    Update = 3,
    Upsert = 4,
    Remove = 5,
    Find = 6,
    FindWithID = 7,
    GetItems = 8,
    GetValues = 9,
    GetKeys = 10,
    First = 11,
    Last = 12,
    IsUnique = 13,
    Count = 14,
    GetStoreInfo = 15,
    UpdateKey = 16,
    UpdateCurrentKey = 17,
    GetCurrentKey = 18,
    Next = 19,
    Previous = 20,
    GetCurrentValue = 21
}

/// <summary>
/// Controls pagination for queries returning multiple items.
/// </summary>
public class PagingInfo
{
    /// <summary>
    /// The number of items to skip (offset).
    /// </summary>
    [JsonPropertyName("page_offset")]
    public int PageOffset { get; set; }

    /// <summary>
    /// The maximum number of items to return per page. Default is 20.
    /// </summary>
    [JsonPropertyName("page_size")]
    public int PageSize { get; set; } = 20;

    /// <summary>
    /// The actual number of items fetched in the current page.
    /// </summary>
    [JsonPropertyName("fetch_count")]
    public int FetchCount { get; set; }

    /// <summary>
    /// The direction of pagination (Forward/Backward).
    /// </summary>
    [JsonPropertyName("direction")]
    public int Direction { get; set; }
}

/// <summary>
/// Represents a B-Tree store for key-value pairs.
/// </summary>
/// <typeparam name="TK">The type of the key.</typeparam>
/// <typeparam name="TV">The type of the value.</typeparam>
public class Btree<TK, TV>
{
    /// <summary>
    /// The unique identifier of the B-Tree store.
    /// </summary>
    public Guid Id { get; }

    /// <summary>
    /// The ID of the transaction this store belongs to.
    /// </summary>
    public Guid TransactionId { get; }
    private readonly bool _isPrimitiveKey;

    internal Btree(Guid id, Guid tid, bool isPrimitiveKey)
    {
        Id = id;
        TransactionId = tid;
        _isPrimitiveKey = isPrimitiveKey;
    }

    /// <summary>
    /// Creates a new B-Tree store.
    /// </summary>
    /// <param name="ctx">The context (server connection).</param>
    /// <param name="name">The name of the store.</param>
    /// <param name="trans">The transaction to create the store in.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <returns>A new Btree instance.</returns>
    /// <exception cref="SopException">Thrown if creation fails.</exception>
    public static Btree<TK, TV> New(Context ctx, string name, Transaction trans, BtreeOptions options = null)
    {
        if (options == null) options = new BtreeOptions(name);
        options.TransactionId = trans.Id.ToString();
        
        bool isPrimitive = typeof(TK).IsPrimitive || typeof(TK) == typeof(string);
        options.IsPrimitiveKey = isPrimitive;

        var payload = JsonSerializer.SerializeToUtf8Bytes(options);
        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)DatabaseAction.NewBtree, Interop.ToBytes(trans.DatabaseId.ToString()), payload);
        var res = Interop.FromPtr(resPtr);

        if (Guid.TryParse(res, out Guid b3id))
        {
            return new Btree<TK, TV>(b3id, trans.Id, isPrimitive);
        }
        throw new SopException(res);
    }

    /// <summary>
    /// Opens an existing B-Tree store.
    /// </summary>
    /// <param name="ctx">The context (server connection).</param>
    /// <param name="name">The name of the store.</param>
    /// <param name="trans">The transaction to open the store in.</param>
    /// <returns>An existing Btree instance.</returns>
    /// <exception cref="SopException">Thrown if opening fails.</exception>
    public static Btree<TK, TV> Open(Context ctx, string name, Transaction trans)
    {
        var options = new BtreeOptions(name);
        options.TransactionId = trans.Id.ToString();
        
        bool isPrimitive = typeof(TK).IsPrimitive || typeof(TK) == typeof(string);
        options.IsPrimitiveKey = isPrimitive;

        var payload = JsonSerializer.SerializeToUtf8Bytes(options);
        var resPtr = NativeMethods.ManageDatabase(ctx.Id, (int)DatabaseAction.OpenBtree, Interop.ToBytes(trans.DatabaseId.ToString()), payload);
        var res = Interop.FromPtr(resPtr);

        if (Guid.TryParse(res, out Guid b3id))
        {
            return new Btree<TK, TV>(b3id, trans.Id, isPrimitive);
        }
        throw new SopException(res);
    }

    /// <summary>
    /// Adds a key-value pair to the store.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="key">The key to add.</param>
    /// <param name="value">The value to add.</param>
    /// <returns>True if successful.</returns>
    public bool Add(Context ctx, TK key, TV value)
    {
        return Manage(ctx, (int)BtreeAction.Add, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { new Item<TK, TV>(key, value) } });
    }

    /// <summary>
    /// Adds an item to the store.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="item">The item to add.</param>
    /// <returns>True if successful.</returns>
    public bool Add(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.Add, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { item } });
    }

    /// <summary>
    /// Adds a list of items to the store.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="items">The list of items to add.</param>
    /// <returns>True if successful.</returns>
    public bool Add(Context ctx, List<Item<TK, TV>> items)
    {
        return Manage(ctx, (int)BtreeAction.Add, new ManageBtreePayload<TK, TV> { Items = items });
    }

    /// <summary>
    /// Adds a key-value pair only if the key does not already exist.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="key">The key to add.</param>
    /// <param name="value">The value to add.</param>
    /// <returns>True if added, false if key exists.</returns>
    public bool AddIfNotExist(Context ctx, TK key, TV value)
    {
        return Manage(ctx, (int)BtreeAction.AddIfNotExist, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { new Item<TK, TV>(key, value) } });
    }

    /// <summary>
    /// Adds an item only if the key does not already exist.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="item">The item to add.</param>
    /// <returns>True if added, false if key exists.</returns>
    public bool AddIfNotExist(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.AddIfNotExist, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { item } });
    }

    /// <summary>
    /// Adds a list of items only if their keys do not already exist.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="items">The list of items to add.</param>
    /// <returns>True if successful.</returns>
    public bool AddIfNotExist(Context ctx, List<Item<TK, TV>> items)
    {
        return Manage(ctx, (int)BtreeAction.AddIfNotExist, new ManageBtreePayload<TK, TV> { Items = items });
    }

    /// <summary>
    /// Adds or updates a key-value pair.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="key">The key to upsert.</param>
    /// <param name="value">The value to upsert.</param>
    /// <returns>True if successful.</returns>
    public bool Upsert(Context ctx, TK key, TV value)
    {
        return Manage(ctx, (int)BtreeAction.Upsert, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { new Item<TK, TV>(key, value) } });
    }

    /// <summary>
    /// Adds or updates an item.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="item">The item to upsert.</param>
    /// <returns>True if successful.</returns>
    public bool Upsert(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.Upsert, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { item } });
    }

    /// <summary>
    /// Adds or updates a list of items.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="items">The list of items to upsert.</param>
    /// <returns>True if successful.</returns>
    public bool Upsert(Context ctx, List<Item<TK, TV>> items)
    {
        return Manage(ctx, (int)BtreeAction.Upsert, new ManageBtreePayload<TK, TV> { Items = items });
    }

    /// <summary>
    /// Updates an existing item.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="item">The item to update.</param>
    /// <returns>True if found and updated.</returns>
    public bool Update(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.Update, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { item } });
    }

    /// <summary>
    /// Updates a list of existing items.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="items">The list of items to update.</param>
    /// <returns>True if successful.</returns>
    public bool Update(Context ctx, List<Item<TK, TV>> items)
    {
        return Manage(ctx, (int)BtreeAction.Update, new ManageBtreePayload<TK, TV> { Items = items });
    }

    /// <summary>
    /// Updates the key of an existing item.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="item">The item with the new key.</param>
    /// <returns>True if successful.</returns>
    public bool UpdateKey(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.UpdateKey, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK,TV>> { item } });
    }

    /// <summary>
    /// Updates the keys of a list of existing items.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="items">The list of items with new keys.</param>
    /// <returns>True if successful.</returns>
    public bool UpdateKey(Context ctx, List<Item<TK, TV>> items)
    {
        return Manage(ctx, (int)BtreeAction.UpdateKey, new ManageBtreePayload<TK, TV> { Items = items });
    }

    /// <summary>
    /// Updates the key of the current item at the cursor.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="item">The item with the new key.</param>
    /// <returns>True if successful.</returns>
    public bool UpdateCurrentKey(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.UpdateCurrentKey, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { item } });
    }

    /// <summary>
    /// Removes an item by key.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="key">The key of the item to remove.</param>
    /// <returns>True if removed.</returns>
    public bool Remove(Context ctx, TK key)
    {
        return Manage(ctx, (int)BtreeAction.Remove, new List<TK>{ key });
    }

    /// <summary>
    /// Removes a list of items by key.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="keys">The list of keys to remove.</param>
    /// <returns>True if successful.</returns>
    public bool Remove(Context ctx, List<TK> keys)
    {
        return Manage(ctx, (int)BtreeAction.Remove, keys);
    }

    /// <summary>
    /// Returns the total number of items in the store.
    /// </summary>
    /// <returns>The count of items.</returns>
    public long Count()
    {
        var metadata = new ManageBtreeMetaData
        {
            IsPrimitiveKey = _isPrimitiveKey,
            BtreeId = Id.ToString(),
            TransactionId = TransactionId.ToString()
        };
        
        NativeMethods.GetBtreeItemCount(
            JsonSerializer.SerializeToUtf8Bytes(metadata),
            out long count,
            out IntPtr errorPtr
        );

        var errorMsg = Interop.FromPtr(errorPtr);
        if (errorMsg != null) throw new SopException(errorMsg);

        return count;
    }

    /// <summary>
    /// Moves the cursor to the specified key.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="key">The key to find.</param>
    /// <returns>True if found.</returns>
    public bool Find(Context ctx, TK key)
    {
        var metadata = new ManageBtreeMetaData
        {
            IsPrimitiveKey = _isPrimitiveKey,
            BtreeId = Id.ToString(),
            TransactionId = TransactionId.ToString()
        };
        
        var payload = new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { new Item<TK, TV> { Key = key } } };
        
        var resPtr = NativeMethods.NavigateBtree(
            ctx.Id, 
            (int)BtreeAction.Find,
            JsonSerializer.SerializeToUtf8Bytes(metadata),
            JsonSerializer.SerializeToUtf8Bytes(payload)
        );
        
        var res = Interop.FromPtr(resPtr);
        return bool.Parse(res);
    }

    /// <summary>
    /// Moves the cursor to the specified key with a specific ID (for duplicates).
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="key">The key to find.</param>
    /// <param name="id">The ID of the specific item.</param>
    /// <returns>True if found.</returns>
    public bool FindWithId(Context ctx, TK key, Guid id)
    {
        var metadata = new ManageBtreeMetaData
        {
            IsPrimitiveKey = _isPrimitiveKey,
            BtreeId = Id.ToString(),
            TransactionId = TransactionId.ToString()
        };
        
        var payload = new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { new Item<TK, TV> { Key = key, Id = id.ToString() } } };
        
        var resPtr = NativeMethods.NavigateBtree(
            ctx.Id, 
            (int)BtreeAction.FindWithID,
            JsonSerializer.SerializeToUtf8Bytes(metadata),
            JsonSerializer.SerializeToUtf8Bytes(payload)
        );
        
        var res = Interop.FromPtr(resPtr);
        return bool.Parse(res);
    }

    /// <summary>
    /// Moves the cursor to the first item.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <returns>True if successful.</returns>
    public bool First(Context ctx)
    {
        return Navigate(ctx, (int)BtreeAction.First);
    }

    /// <summary>
    /// Moves the cursor to the next item.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <returns>True if successful.</returns>
    public bool Next(Context ctx)
    {
        return Navigate(ctx, (int)BtreeAction.Next);
    }

    /// <summary>
    /// Moves the cursor to the previous item.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <returns>True if successful.</returns>
    public bool Previous(Context ctx)
    {
        return Navigate(ctx, (int)BtreeAction.Previous);
    }

    /// <summary>
    /// Moves the cursor to the last item.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <returns>True if successful.</returns>
    public bool Last(Context ctx)
    {
        return Navigate(ctx, (int)BtreeAction.Last);
    }

    /// <summary>
    /// Gets the key at the current cursor position.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <returns>The current item (key only).</returns>
    public Item<TK, TV> GetCurrentKey(Context ctx)
    {
        var metadata = new ManageBtreeMetaData
        {
            IsPrimitiveKey = _isPrimitiveKey,
            BtreeId = Id.ToString(),
            TransactionId = TransactionId.ToString()
        };

        // Payload is not needed for GetCurrentKey but we need to pass something valid or empty
        var payload = new PagingInfo();

        NativeMethods.GetFromBtree(
            ctx.Id, 
            (int)BtreeAction.GetCurrentKey,
            JsonSerializer.SerializeToUtf8Bytes(metadata),
            JsonSerializer.SerializeToUtf8Bytes(payload),
            out IntPtr resultPtr,
            out IntPtr errorPtr
        );

        var errorMsg = Interop.FromPtr(errorPtr);
        if (errorMsg != null) throw new SopException(errorMsg);

        var resultJson = Interop.FromPtr(resultPtr);
        if (resultJson == null) return null;

        var items = JsonSerializer.Deserialize<List<Item<TK, TV>>>(resultJson);
        if (items != null && items.Count > 0)
        {
            return items[0];
        }
        return null;
    }

    /// <summary>
    /// Gets the value at the current cursor position.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <returns>The current item (value only).</returns>
    /// <summary>
    /// Gets the value at the current cursor position.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <returns>The current item (value only).</returns>
    public Item<TK, TV> GetCurrentValue(Context ctx)
    {
        var metadata = new ManageBtreeMetaData
        {
            IsPrimitiveKey = _isPrimitiveKey,
            BtreeId = Id.ToString(),
            TransactionId = TransactionId.ToString()
        };

        // Payload is not needed for GetCurrentValue but we need to pass something valid or empty
        var payload = new PagingInfo();

        NativeMethods.GetFromBtree(
            ctx.Id, 
            (int)BtreeAction.GetCurrentValue,
            JsonSerializer.SerializeToUtf8Bytes(metadata),
            JsonSerializer.SerializeToUtf8Bytes(payload),
            out IntPtr resultPtr,
            out IntPtr errorPtr
        );

        var errorMsg = Interop.FromPtr(errorPtr);
        if (errorMsg != null) throw new SopException(errorMsg);

        var resultJson = Interop.FromPtr(resultPtr);
        if (resultJson == null) return null;

        var items = JsonSerializer.Deserialize<List<Item<TK, TV>>>(resultJson);
        if (items != null && items.Count > 0)
        {
            return items[0];
        }
        return null;
    }

    /// <summary>
    /// Gets a page of keys.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="pagingInfo">Pagination details.</param>
    /// <returns>A list of items (keys only).</returns>
    public List<Item<TK, TV>> GetKeys(Context ctx, PagingInfo pagingInfo)
    {
        var metadata = new ManageBtreeMetaData
        {
            IsPrimitiveKey = _isPrimitiveKey,
            BtreeId = Id.ToString(),
            TransactionId = TransactionId.ToString()
        };

        NativeMethods.GetFromBtree(
            ctx.Id, 
            (int)BtreeAction.GetKeys,
            JsonSerializer.SerializeToUtf8Bytes(metadata),
            JsonSerializer.SerializeToUtf8Bytes(pagingInfo),
            out IntPtr resultPtr,
            out IntPtr errorPtr
        );

        var errorMsg = Interop.FromPtr(errorPtr);
        if (errorMsg != null) throw new SopException(errorMsg);

        var resultJson = Interop.FromPtr(resultPtr);
        if (resultJson == null) return null;

        return JsonSerializer.Deserialize<List<Item<TK, TV>>>(resultJson);
    }

    private bool Navigate(Context ctx, int action)
    {
        var metadata = new ManageBtreeMetaData
        {
            IsPrimitiveKey = _isPrimitiveKey,
            BtreeId = Id.ToString(),
            TransactionId = TransactionId.ToString()
        };

        var resPtr = NativeMethods.NavigateBtree(
            ctx.Id,
            action,
            JsonSerializer.SerializeToUtf8Bytes(metadata),
            null
        );

        var res = Interop.FromPtr(resPtr);
        return bool.Parse(res);
    }

    /// <summary>
    /// Gets the value for a specific key.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="key">The key to retrieve.</param>
    /// <returns>A list containing the item (value only).</returns>
    public List<Item<TK, TV>> GetValues(Context ctx, TK key)
    {
        return GetValues(ctx, new List<Item<TK, TV>> { new Item<TK, TV> { Key = key } });
    }

    /// <summary>
    /// Gets values for a list of keys.
    /// </summary>
    /// <param name="ctx">The context.</param>
    /// <param name="keys">The list of items (keys) to retrieve values for.</param>
    /// <returns>A list of items (values only).</returns>
    public List<Item<TK, TV>> GetValues(Context ctx, List<Item<TK, TV>> keys)
    {
        var metadata = new ManageBtreeMetaData
        {
            IsPrimitiveKey = _isPrimitiveKey,
            BtreeId = Id.ToString(),
            TransactionId = TransactionId.ToString()
        };

        var payload = new ManageBtreePayload<TK, TV> { Items = keys };

        NativeMethods.GetFromBtree(
            ctx.Id, 
            (int)BtreeAction.GetValues,
            JsonSerializer.SerializeToUtf8Bytes(metadata),
            JsonSerializer.SerializeToUtf8Bytes(payload),
            out IntPtr resultPtr,
            out IntPtr errorPtr
        );

        var errorMsg = Interop.FromPtr(errorPtr);
        if (errorMsg != null) throw new SopException(errorMsg);

        var resultJson = Interop.FromPtr(resultPtr);
        if (resultJson == null) return null;

        return JsonSerializer.Deserialize<List<Item<TK, TV>>>(resultJson);
    }

    private bool Manage(Context ctx, int action, object payloadObj)
    {
        var metadata = new ManageBtreeMetaData
        {
            IsPrimitiveKey = _isPrimitiveKey,
            BtreeId = Id.ToString(),
            TransactionId = TransactionId.ToString()
        };

        var resPtr = NativeMethods.ManageBtree(
            ctx.Id,
            action,
            JsonSerializer.SerializeToUtf8Bytes(metadata),
            JsonSerializer.SerializeToUtf8Bytes(payloadObj)
        );

        var res = Interop.FromPtr(resPtr);
        return bool.Parse(res);
    }
}
