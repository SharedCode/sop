using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sop;

public class BtreeOptions
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("is_unique")]
    public bool IsUnique { get; set; } = false;

    [JsonPropertyName("is_primitive_key")]
    public bool IsPrimitiveKey { get; set; } = true;

    [JsonPropertyName("slot_length")]
    public int SlotLength { get; set; } = 500;

    [JsonPropertyName("description")]
    public string Description { get; set; } = "";

    [JsonPropertyName("is_value_data_in_node_segment")]
    public bool IsValueDataInNodeSegment { get; set; } = true;

    [JsonPropertyName("is_value_data_actively_persisted")]
    public bool IsValueDataActivelyPersisted { get; set; } = false;

    [JsonPropertyName("is_value_data_globally_cached")]
    public bool IsValueDataGloballyCached { get; set; } = false;

    [JsonPropertyName("leaf_load_balancing")]
    public bool LeafLoadBalancing { get; set; } = false;

    [JsonPropertyName("cache_config")]
    public CacheConfig CacheConfig { get; set; }

    [JsonPropertyName("index_specification")]
    public string IndexSpecification { get; set; }

    [JsonIgnore]
    public IndexSpecification IndexSpec 
    { 
        set 
        { 
            IndexSpecification = value != null ? JsonSerializer.Serialize(value) : null; 
        } 
    }

    [JsonPropertyName("transaction_id")]
    public string TransactionId { get; set; }
    
    public BtreeOptions(string name)
    {
        Name = name;
    }

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

public enum ValueDataSize
{
    Small = 0,
    Medium = 1,
    Big = 2
}

public class CacheConfig
{
    [JsonPropertyName("registry_cache_duration")]
    public int RegistryCacheDuration { get; set; } = 10;

    [JsonPropertyName("is_registry_cache_ttl")]
    public bool IsRegistryCacheTtl { get; set; } = false;

    [JsonPropertyName("node_cache_duration")]
    public int NodeCacheDuration { get; set; } = 5;

    [JsonPropertyName("is_node_cache_ttl")]
    public bool IsNodeCacheTtl { get; set; } = false;

    [JsonPropertyName("store_info_cache_duration")]
    public int StoreInfoCacheDuration { get; set; } = 5;

    [JsonPropertyName("is_store_info_cache_ttl")]
    public bool IsStoreInfoCacheTtl { get; set; } = false;

    [JsonPropertyName("value_data_cache_duration")]
    public int ValueDataCacheDuration { get; set; } = 0;

    [JsonPropertyName("is_value_data_cache_ttl")]
    public bool IsValueDataCacheTtl { get; set; } = false;
}

public class IndexFieldSpecification
{
    [JsonPropertyName("field_name")]
    public string FieldName { get; set; }

    [JsonPropertyName("ascending_sort_order")]
    public bool AscendingSortOrder { get; set; } = true;
}

public class IndexSpecification
{
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

public enum PagingDirection
{
    Forward = 0,
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

public class PagingInfo
{
    [JsonPropertyName("page_offset")]
    public int PageOffset { get; set; }

    [JsonPropertyName("page_size")]
    public int PageSize { get; set; } = 20;

    [JsonPropertyName("fetch_count")]
    public int FetchCount { get; set; }

    [JsonPropertyName("direction")]
    public int Direction { get; set; }
}

public class Btree<TK, TV>
{
    public Guid Id { get; }
    public Guid TransactionId { get; }
    private readonly bool _isPrimitiveKey;

    internal Btree(Guid id, Guid tid, bool isPrimitiveKey)
    {
        Id = id;
        TransactionId = tid;
        _isPrimitiveKey = isPrimitiveKey;
    }

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

    public bool Add(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.Add, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { item } });
    }

    public bool Add(Context ctx, List<Item<TK, TV>> items)
    {
        return Manage(ctx, (int)BtreeAction.Add, new ManageBtreePayload<TK, TV> { Items = items });
    }

    public bool AddIfNotExist(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.AddIfNotExist, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { item } });
    }

    public bool AddIfNotExist(Context ctx, List<Item<TK, TV>> items)
    {
        return Manage(ctx, (int)BtreeAction.AddIfNotExist, new ManageBtreePayload<TK, TV> { Items = items });
    }

    public bool Upsert(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.Upsert, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { item } });
    }

    public bool Upsert(Context ctx, List<Item<TK, TV>> items)
    {
        return Manage(ctx, (int)BtreeAction.Upsert, new ManageBtreePayload<TK, TV> { Items = items });
    }

    public bool Update(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.Update, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { item } });
    }

    public bool Update(Context ctx, List<Item<TK, TV>> items)
    {
        return Manage(ctx, (int)BtreeAction.Update, new ManageBtreePayload<TK, TV> { Items = items });
    }

    public bool UpdateKey(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.UpdateKey, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK,TV>> { item } });
    }

    public bool UpdateKey(Context ctx, List<Item<TK, TV>> items)
    {
        return Manage(ctx, (int)BtreeAction.UpdateKey, new ManageBtreePayload<TK, TV> { Items = items });
    }

    public bool UpdateCurrentKey(Context ctx, Item<TK, TV> item)
    {
        return Manage(ctx, (int)BtreeAction.UpdateCurrentKey, new ManageBtreePayload<TK, TV> { Items = new List<Item<TK, TV>> { item } });
    }

    public bool Remove(Context ctx, TK key)
    {
        return Manage(ctx, (int)BtreeAction.Remove, new List<TK>{ key });
    }

    public bool Remove(Context ctx, List<TK> keys)
    {
        return Manage(ctx, (int)BtreeAction.Remove, keys);
    }

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

    public bool First(Context ctx)
    {
        return Navigate(ctx, (int)BtreeAction.First);
    }

    public bool Next(Context ctx)
    {
        return Navigate(ctx, (int)BtreeAction.Next);
    }

    public bool Previous(Context ctx)
    {
        return Navigate(ctx, (int)BtreeAction.Previous);
    }

    public bool Last(Context ctx)
    {
        return Navigate(ctx, (int)BtreeAction.Last);
    }

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

    public List<Item<TK, TV>> GetValues(Context ctx, TK key)
    {
        return GetValues(ctx, new List<Item<TK, TV>> { new Item<TK, TV> { Key = key } });
    }

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
