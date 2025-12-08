using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sop;

internal enum ModelAction
{
    Save = 1,
    Load = 2,
    List = 3,
    Delete = 4
}

public class ModelStore
{
    private readonly Context _ctx;
    private readonly Transaction _trans;
    private readonly Guid _storeId;

    internal ModelStore(Context ctx, Transaction trans, Guid storeId)
    {
        _ctx = ctx;
        _trans = trans;
        _storeId = storeId;
    }

    public void Save(string category, string name, object model)
    {
        var payload = new { category = category, name = name, model = model };
        var jsonPayload = JsonSerializer.SerializeToUtf8Bytes(payload);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageModelStore(_ctx.Id, (int)ModelAction.Save, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    public T Load<T>(string category, string name)
    {
        var payload = new { category = category, name = name };
        var jsonPayload = JsonSerializer.SerializeToUtf8Bytes(payload);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageModelStore(_ctx.Id, (int)ModelAction.Load, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);

        if (res == null) return default;
        
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
        return JsonSerializer.Deserialize<T>(res, options);
    }

    public List<string> List(string category)
    {
        // Payload is just the category string
        var jsonPayload = System.Text.Encoding.UTF8.GetBytes(category);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageModelStore(_ctx.Id, (int)ModelAction.List, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);

        if (res == null) return new List<string>();
        if (!res.Trim().StartsWith("[")) throw new SopException(res);

        return JsonSerializer.Deserialize<List<string>>(res);
    }

    public void Delete(string category, string name)
    {
        var payload = new { category = category, name = name };
        var jsonPayload = JsonSerializer.SerializeToUtf8Bytes(payload);
        var targetId = GetTargetId();

        var resPtr = NativeMethods.ManageModelStore(_ctx.Id, (int)ModelAction.Delete, targetId, jsonPayload);
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
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
