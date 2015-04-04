namespace Sop.Collections.BTree
{
    /// <summary>
    /// Thread Synchronization interface.
    /// </summary>
    public interface ISynchronizer
    {
        bool IsLocked { get; }
        void Lock(OperationType requestedOperation = OperationType.Write);
        void Unlock(OperationType requestedOperation = OperationType.Write);
    }
}