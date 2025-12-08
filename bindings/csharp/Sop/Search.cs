using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sop;

public class TextSearchResult
{
    [JsonPropertyName("DocID")]
    public string DocID { get; set; }

    [JsonPropertyName("Score")]
    public double Score { get; set; }
}

internal enum SearchAction
{
    Add = 1,
    Search = 2
}

public class Search
{
    private readonly Context _ctx;
    private readonly Transaction _trans;
    private readonly Guid _storeId;

    internal Search(Context ctx, Transaction trans, Guid storeId)
    {
        _ctx = ctx;
        _trans = trans;
        _storeId = storeId;
    }

    public void Add(string docId, string text)
    {
        var payload = new { doc_id = docId, text = text };
        var jsonPayload = JsonSerializer.SerializeToUtf8Bytes(payload);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageSearch(_ctx.Id, (int)SearchAction.Add, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    public List<TextSearchResult> SearchQuery(string query)
    {
        var payload = new { query = query };
        var jsonPayload = JsonSerializer.SerializeToUtf8Bytes(payload);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageSearch(_ctx.Id, (int)SearchAction.Search, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);

        if (res == null) return new List<TextSearchResult>();
        
        // Check if result is an error message (not JSON array)
        if (!res.Trim().StartsWith("["))
        {
                throw new SopException(res);
        }

        return JsonSerializer.Deserialize<List<TextSearchResult>>(res);
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
