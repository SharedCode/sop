using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sop;

public class VectorItem
{
    [JsonPropertyName("id")]
    public string Id { get; set; }

    [JsonPropertyName("vector")]
    public float[] Vector { get; set; }

    [JsonPropertyName("payload")]
    public Dictionary<string, object> Payload { get; set; }
}

public class VectorQueryResult
{
    [JsonPropertyName("id")]
    public string Id { get; set; }

    [JsonPropertyName("vector")]
    public float[] Vector { get; set; }

    [JsonPropertyName("payload")]
    public Dictionary<string, object> Payload { get; set; }

    [JsonPropertyName("score")]
    public double Score { get; set; }
}

public class VectorStoreConfig
{
    [JsonPropertyName("usage_mode")]
    public int UsageMode { get; set; } = 0;

    [JsonPropertyName("content_size")]
    public int ContentSize { get; set; } = 0;
}

internal enum VectorAction
{
    Upsert = 1,
    UpsertBatch = 2,
    Get = 3,
    Delete = 4,
    Query = 5,
    Count = 6,
    Optimize = 7
}

public class VectorStore
{
    private readonly Context _ctx;
    private readonly Transaction _trans;
    private readonly Guid _storeId;

    internal VectorStore(Context ctx, Transaction trans, Guid storeId)
    {
        _ctx = ctx;
        _trans = trans;
        _storeId = storeId;
    }

    public void Upsert(VectorItem item)
    {
        var jsonPayload = JsonSerializer.SerializeToUtf8Bytes(item);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageVectorDB(_ctx.Id, (int)VectorAction.Upsert, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    public void UpsertBatch(List<VectorItem> items)
    {
        var jsonPayload = JsonSerializer.SerializeToUtf8Bytes(items);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageVectorDB(_ctx.Id, (int)VectorAction.UpsertBatch, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    public VectorItem Get(string id)
    {
        var jsonPayload = System.Text.Encoding.UTF8.GetBytes(id);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageVectorDB(_ctx.Id, (int)VectorAction.Get, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);

        if (res == null) return null;
        if (res.Contains("item not found")) return null;
        if (!res.Trim().StartsWith("{")) throw new SopException(res);

        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
        return JsonSerializer.Deserialize<VectorItem>(res, options);
    }

    public void Delete(string id)
    {
        var jsonPayload = System.Text.Encoding.UTF8.GetBytes(id);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageVectorDB(_ctx.Id, (int)VectorAction.Delete, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    public List<VectorQueryResult> Query(float[] vector, int k = 10, Dictionary<string, object> filter = null)
    {
        var payload = new { vector = vector, k = k, filter = filter };
        var jsonPayload = JsonSerializer.SerializeToUtf8Bytes(payload);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageVectorDB(_ctx.Id, (int)VectorAction.Query, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);

        if (res == null) return new List<VectorQueryResult>();
        if (!res.Trim().StartsWith("[")) throw new SopException(res);

        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
        return JsonSerializer.Deserialize<List<VectorQueryResult>>(res, options);
    }

    public long Count()
    {
        var targetId = GetTargetId();
        var resPtr = NativeMethods.ManageVectorDB(_ctx.Id, (int)VectorAction.Count, targetId, null);
        var res = Interop.FromPtr(resPtr);

        if (long.TryParse(res, out long count))
        {
            return count;
        }
        throw new SopException(res);
    }

    private byte[] GetTargetId()
    {
        var meta = new Dictionary<string, string>
        {
            { "transaction_id", _trans.Id.ToString() },
            { "id", _storeId.ToString() }
        };
        return JsonSerializer.SerializeToUtf8Bytes(meta);
    }
}
