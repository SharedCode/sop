using System;
using System.Collections.Generic;
using System.Threading;

namespace Sop.Synchronization
{
    /// <summary>
    /// Synchronizer with a secondary signaling mechanism mostly adept for
    /// (global) Transaction Commit that orchestrate locks of all Stores in the App.
    /// </summary>
    public class Synchronizer : SynchronizerMultiReaderBase
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
        public override void CommitLockRequest(bool lockFlag = true)
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
                base.Unlock();
            }
        }
        /// <summary>
        /// Does a spin wait until a commit lock/unlock is detected.
        /// </summary>
        public override void WaitForCommitLock(bool lockFlag = true)
        {
            if (lockFlag)
            {
                while (LockCount > 0)
                {
                    Thread.Sleep(50);
                }
                base.Lock();
                _commitLocked = false;
                _trapForCommitLock = false;
            }
        }
        /// <summary>
        /// Lock Synchronizer.
        /// </summary>
        /// <param name="requestedOperation">Lock resource for Read, Write or Search</param>
        public override int Lock(OperationType requestedOperation = OperationType.Write)
        {
            while (true)
            {
                if (_trapForCommitLock)
                    _commitLockEvent.Wait();
                var r = base.Lock(requestedOperation);
                if (_trapForCommitLock || (_commitLocked && LockCount == 1))
                {
                    _trapForCommitLock = true;
                    // allow Commit to proceed...
                    r = base.Unlock(requestedOperation);
                    // detect when commit is done...
                    _commitLockEvent.Wait();
                    continue;
                }
                return r;
            }
        }

        public override int Unlock(OperationType requestedOperation = OperationType.Write)
        {
            if (!_trapForCommitLock && _commitLocked && LockCount == 1)
            {
                _trapForCommitLock = true;
            }
            return base.Unlock(requestedOperation);
        }
        private volatile bool _trapForCommitLock;
        private volatile bool _commitLocked;
        private CommitLockEvent _commitLockEvent = new CommitLockEvent();
    }
}
