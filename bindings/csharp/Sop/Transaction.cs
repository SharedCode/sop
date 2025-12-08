using System;
using System.Text.Json;

namespace Sop;

internal enum TransactionAction
{
    NewTransaction = 1,
    Begin = 2,
    Commit = 3,
    Rollback = 4
}

public class Transaction : IDisposable
{
    internal Context Context { get; }
    public Guid Id { get; }
    public Guid DatabaseId { get; }
    private bool _begun;

    internal Transaction(Context ctx, Guid id, bool begun, Guid databaseId)
    {
        Context = ctx;
        Id = id;
        _begun = begun;
        DatabaseId = databaseId;
    }

    public void Commit()
    {
        if (!_begun) return;
        
        var resPtr = NativeMethods.ManageTransaction(Context.Id, (int)TransactionAction.Commit, Interop.ToBytes(Id.ToString()));
        var res = Interop.FromPtr(resPtr);
        
        if (res != null) throw new SopException(res);
        _begun = false;
    }

    public void Rollback()
    {
        if (!_begun) return;

        var resPtr = NativeMethods.ManageTransaction(Context.Id, (int)TransactionAction.Rollback, Interop.ToBytes(Id.ToString()));
        var res = Interop.FromPtr(resPtr);

        if (res != null) throw new SopException(res);
        _begun = false;
    }

    public void Dispose()
    {
        if (_begun)
        {
            Rollback();
        }
    }
}
