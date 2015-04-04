using System;
using System.Threading;

namespace Sop.Collections.BTree
{
    //NOTE: this synchronizer predates back .Net 4's Slim Reader Writer class by almost a decade. This may get rewritten sometime when we support .net 4...

    /// <summary>
    /// Synchronizer wraps thread synchronization on Collection code.
    /// Instances of this class can serve as SyncRoot for any collection type classes
    /// </summary>
    public class LegacySynchronizer : ISynchronizer
    {
        //		Logic table:
        //Current Operation	: Requested Operation	: Result
        //Idle				: Read/Search or Write	: Allowed
        //Read/Search		: Read/Search			: Allowed
        //Read/Search		: Write					: Wait until Read/Search is done
        //Write				: Read/Search or Write	: Wait until Write is done
        //

        /// <summary>
        /// Lock Synchronizer.
        /// </summary>
        /// <param name="requestedOperation">Lock resource for Read, Write or Search</param>
        public void Lock(OperationType requestedOperation = OperationType.Write)
        {
            Interlocked.Increment(ref _lockCount);
            _locker.Lock();

            //if (requestedOperation == OperationType.Read)
            //{
            //    if (Interlocked.Increment(ref _readCount) == 1)
            //    {
            //        _locker.Lock();
            //    }
            //}
            //else
            //{
            //    _locker.Lock();
            //}
        }
        /// <summary>
        /// Unlock Synchronizer.
        /// </summary>
        public void Unlock(OperationType requestedOperation = OperationType.Write)
        {
            Interlocked.Decrement(ref _lockCount);
            _locker.Unlock();

            //if (requestedOperation == OperationType.Read)
            //{
            //    if (Interlocked.Decrement(ref _readCount) == 0)
            //    {
            //        _locker.Unlock();
            //    }
            //}
            //else
            //{
            //    _locker.Unlock();
            //}
        }

        /// <summary>
        /// true if there is at least a single lock onto this object, false otherwise.
        /// </summary>
        public bool IsLocked
        {
            get { return Interlocked.Read(ref _lockCount) > 0; }
        }
        private long _lockCount;

        /// <summary>
        /// count of threads in read operation.
        /// </summary>
        private long _readCount;

        private readonly SingleThreadAccess _locker = new SingleThreadAccess();
    }
}
