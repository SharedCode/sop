using System;
using System.Text.Json;

namespace Sop;

/// <summary>
/// Specifies the mode of the transaction.
/// </summary>
public enum TransactionMode
{
    /// <summary>
    /// No specific check.
    /// </summary>
    NoCheck = 0,
    /// <summary>
    /// Transaction intended for writing.
    /// </summary>
    ForWriting = 1,
    /// <summary>
    /// Transaction intended for reading.
    /// </summary>
    ForReading = 2
}

internal enum TransactionAction
{
    NewTransaction = 1,
    Begin = 2,
    Commit = 3,
    Rollback = 4
}

/// <summary>
/// Represents a database transaction.
/// </summary>
public class Transaction : IDisposable
{
    internal Context Context { get; }
    
    /// <summary>
    /// The unique identifier of the transaction.
    /// </summary>
    public Guid Id { get; }

    /// <summary>
    /// The ID of the database this transaction belongs to.
    /// </summary>
    public Guid DatabaseId { get; }
    private bool _begun;

    internal Transaction(Context ctx, Guid id, bool begun, Guid databaseId)
    {
        Context = ctx;
        Id = id;
        _begun = begun;
        DatabaseId = databaseId;
    }

    /// <summary>
    /// Commits the transaction, persisting all changes.
    /// </summary>
    /// <exception cref="SopException">Thrown if the commit fails.</exception>
    public void Commit()
    {
        if (!_begun) return;
        
        var resPtr = NativeMethods.ManageTransaction(Context.Id, (int)TransactionAction.Commit, Interop.ToBytes(Id.ToString()));
        var res = Interop.FromPtr(resPtr);
        
        if (res != null) throw new SopException(res);
        _begun = false;
    }

    /// <summary>
    /// Rolls back the transaction, discarding all changes.
    /// </summary>
    /// <exception cref="SopException">Thrown if the rollback fails.</exception>
    public void Rollback()
    {
        if (!_begun) return;

        var resPtr = NativeMethods.ManageTransaction(Context.Id, (int)TransactionAction.Rollback, Interop.ToBytes(Id.ToString()));
        var res = Interop.FromPtr(resPtr);

        if (res != null) throw new SopException(res);
        _begun = false;
    }

    /// <summary>
    /// Disposes the transaction. Rolls back if not already committed.
    /// </summary>
    public void Dispose()
    {
        if (_begun)
        {
            Rollback();
        }
    }
}
