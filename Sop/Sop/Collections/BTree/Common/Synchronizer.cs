using System;
using System.Threading;

namespace Sop.Collections.BTree
{
    /// <summary>
    /// Synchronizer wraps thread synchronization on Collection code.
    /// Instances of this class can serve as SyncRoot for any collection type classes
    /// </summary>
    public class Synchronizer : ISynchronizer
    {
        /// <summary>
        /// Lock Synchronizer.
        /// </summary>
        /// <param name="requestedOperation">Lock resource for Read, Write or Search</param>
        public void Lock(OperationType requestedOperation = OperationType.Write)
        {
            Interlocked.Increment(ref _lockCount);
            if (requestedOperation == OperationType.Read)
                _locker.EnterReadLock();
            else
                _locker.EnterWriteLock();
        }
        /// <summary>
        /// Unlock Synchronizer.
        /// </summary>
        public void Unlock(OperationType requestedOperation = OperationType.Write)
        {
            Interlocked.Decrement(ref _lockCount);
            if (requestedOperation == OperationType.Read)
                _locker.ExitReadLock();
            else
                _locker.ExitWriteLock();
        }

        /// <summary>
        /// true if there is at least a single lock onto this object, false otherwise.
        /// </summary>
        public bool IsLocked
        {
            get { return Interlocked.Read(ref _lockCount) > 0; }
        }
        private long _lockCount;
        private ReaderWriterLockSlim _locker = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
    }
}
