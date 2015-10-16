using System;
using System.Collections.Generic;
using System.Threading;

namespace Sop.Synchronization
{
    /// <summary>
    /// Synchronizer with a secondary signaling mechanism mostly adept for
    /// (global) Transaction Commit that orchestrate locks of all Stores in the App.
    /// </summary>
    public class Synchronizer<T> : ISynchronizer where T : ISynchronizer, new()
    {
        class CommitLockEvent
        {
            public void Wait()
            {
                _commitLocker.Lock();
                _commitLocker.Unlock();
            }
            public void Set()
            {
                _commitLocker.Unlock();
            }
            public void Reset()
            {
                _commitLocker.Lock();
            }
            private SynchronizerMultiReaderBase _commitLocker = new SynchronizerMultiReaderBase();
        }
        /// <summary>
        /// During Commit, the Server will call this method to signal that a Commit
        /// will be done. The signal will cause any succeeding granted Lock request
        /// to relinquish the Lock so Commit can proceed.
        /// </summary>
        public void CommitLockRequest(bool lockFlag = true)
        {
            if (lockFlag)
            {
                _commitLockEvent.Reset();
                _commitLocked = true;
            }
            else
            {
                _commitLocked = false;
                _trapForCommitLock = false;
                _commitLockEvent.Set();
                Locker.Unlock();
            }
        }
        /// <summary>
        /// Does a spin wait until a commit lock/unlock is detected.
        /// </summary>
        public void WaitForCommitLock(bool lockFlag = true)
        {
            if (lockFlag)
            {
                while (Locker.LockCount > 0)
                {
                    Thread.Sleep(50);
                }
                Locker.Lock();
                _commitLocked = false;
                _trapForCommitLock = false;
            }
        }
        /// <summary>
        /// Lock Synchronizer.
        /// </summary>
        /// <param name="requestedOperation">Lock resource for Read, Write or Search</param>
        public int Lock(OperationType requestedOperation = OperationType.Write)
        {
            while (true)
            {
                if (_trapForCommitLock)
                    _commitLockEvent.Wait();
                var r = Locker.Lock(requestedOperation);
                if (_trapForCommitLock || (_commitLocked && Locker.LockCount == 1))
                {
                    _trapForCommitLock = true;
                    // allow Commit to proceed...
                    r = Locker.Unlock(requestedOperation);
                    // detect when commit is done...
                    _commitLockEvent.Wait();
                    continue;
                }
                return r;
            }
        }

        public int Unlock(OperationType requestedOperation = OperationType.Write)
        {
            if (!_trapForCommitLock && _commitLocked && Locker.LockCount == 1)
            {
                _trapForCommitLock = true;
            }
            return Locker.Unlock(requestedOperation);
        }

        public void Invoke(VoidFunc function, OperationType requestedOperation = OperationType.Write)
        {
            Locker.Invoke(function, requestedOperation);
        }

        public void Invoke<T1, T2>(VoidFunc<T1, T2> function, T1 arg1, T2 arg2, OperationType requestedOperation = OperationType.Write)
        {
            Locker.Invoke(function, arg1, arg2, requestedOperation);
        }

        public TResult Invoke<TResult>(Func<TResult> function, OperationType requestedOperation = OperationType.Write)
        {
            return Locker.Invoke(function, requestedOperation);
        }

        public TResult Invoke<T1, TResult>(Func<T1, TResult> function, T1 arg, OperationType requestedOperation = OperationType.Write)
        {
            return Locker.Invoke(function, arg, requestedOperation);
        }

        public TResult Invoke<T1, T2, TResult>(Func<T1, T2, TResult> function, T1 arg, T2 arg2, OperationType requestedOperation = OperationType.Write)
        {
            return Locker.Invoke(function, arg, arg2, requestedOperation);
        }

        public int LockCount
        {
            get
            {
                return Locker.LockCount;
            }
        }

        public bool IsLocked
        {
            get
            {
                return Locker.IsLocked;
            }
        }

        public bool TransactionRollback
        {
            get
            {
                return Locker.TransactionRollback;
            }
            set
            {
                Locker.TransactionRollback = value;
            }
        }

        private volatile bool _trapForCommitLock;
        private volatile bool _commitLocked;
        private CommitLockEvent _commitLockEvent = new CommitLockEvent();
        private T Locker = new T();
    }
}
