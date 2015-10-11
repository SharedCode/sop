using System;
using System.Threading;

namespace Sop.Synchronization
{
    /// <summary>
    /// Synchronizer wraps thread synchronization on Store code.
    /// Instances of this class can serve as SyncRoot for any collection type classes.
    /// 
    /// NOTE: this implementation forces lock requests to the Write operation type.
    /// </summary>
    public class SynchronizerBase : ISynchronizer
    {
        /// <summary>
        /// CommitLockRequest is not implemened in this Synchronizer.
        /// It does nothing, 'simply returns.
        /// </summary>
        virtual public void CommitLockRequest(bool lockFlag = true) { }
        /// <summary>
        /// Does a spin wait until a commit lock is detected.
        /// </summary>
        virtual public void WaitForCommitLock(bool lockFlag = true) { }

        /// <summary>
        /// Lock Synchronizer.
        /// </summary>
        /// <param name="requestedOperation">Lock resource for Read, Write or Search</param>
        virtual public int Lock(OperationType requestedOperation = OperationType.Write)
        {
            if (TransactionRollback)
                RaiseRollbackException();
            Monitor.Enter(locker);
            lockCount++;
            if (TransactionRollback)
            {
                try
                {
                    RaiseRollbackException();
                }
                finally
                {
                    Unlock();
                }
            }
            return lockCount;
        }
        /// <summary>
        /// Unlock Synchronizer.
        /// </summary>
        virtual public int Unlock(OperationType requestedOperation = OperationType.Write)
        {
            lockCount--;
            var l = lockCount;
            Monitor.Exit(locker);
            return l;
        }
        private void RaiseRollbackException()
        {
            throw new Transaction.TransactionRolledbackException("Transaction was rolled back while attempting to get a Lock.");
        }
        #region Invoke
        /// <summary>
        /// Thread safe Invoke wraps in Lock/Unlock calls a call to a lambda expression.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="function"></param>
        /// <param name="arg"></param>
        /// <returns></returns>
        public TResult Invoke<T1, TResult>(Func<T1, TResult> function, T1 arg)
        {
            Lock();
            try
            {
                return function(arg);
            }
            finally
            {
                Unlock();
            }
        }
        public TResult Invoke<T1, T2, TResult>(Func<T1, T2, TResult> function, T1 arg, T2 arg2)
        {
            Lock();
            try
            {
                return function(arg, arg2);
            }
            finally
            {
                Unlock();
            }
        }

        public TResult Invoke<TResult>(Func<TResult> function)
        {
            Lock();
            try
            {
                return function();
            }
            finally
            {
                Unlock();
            }
        }
        public void Invoke(VoidFunc function)
        {
            Lock();
            try
            {
                function();
            }
            finally
            {
                Unlock();
            }
        }
        public void Invoke<T1, T2>(VoidFunc<T1, T2> function, T1 arg1, T2 arg2)
        {
            Lock();
            try
            {
                function(arg1, arg2);
            }
            finally
            {
                Unlock();
            }
        }
        #endregion

        /// <summary>
        /// true if there is at least a single lock onto this object, false otherwise.
        /// </summary>
        public bool IsLocked
        {
            get
            {
                return lockCount > 0;
            }
        }
        /// <summary>
        /// Returns true if Locker detected a Transaction Rollback event,
        /// false otherwise.
        /// </summary>
        public bool TransactionRollback { get; internal set; }
        protected object locker = new object();
        internal protected int lockCount;
    }
}
