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
    public class SynchronizerSingleReaderWriterBase : ISynchronizer
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
            var result = ++lockCount;
            if (TransactionRollback)
            {
                try
                {
                    RaiseRollbackException();
                }
                finally
                {
                    Unlock(requestedOperation);
                }
            }
            return result;
        }
        /// <summary>
        /// Unlock Synchronizer.
        /// </summary>
        virtual public int Unlock(OperationType requestedOperation = OperationType.Write)
        {
            var l = --lockCount;
            if (Monitor.IsEntered(locker))
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
        public TResult Invoke<T1, TResult>(Func<T1, TResult> function, T1 arg, OperationType requestedOperation = OperationType.Write)
        {
            Lock(requestedOperation);
            try
            {
                return function(arg);
            }
            finally
            {
                Unlock(requestedOperation);
            }
        }
        public TResult Invoke<T1, T2, TResult>(Func<T1, T2, TResult> function, T1 arg, T2 arg2, OperationType requestedOperation = OperationType.Write)
        {
            Lock(requestedOperation);
            try
            {
                return function(arg, arg2);
            }
            finally
            {
                Unlock(requestedOperation);
            }
        }

        public TResult Invoke<TResult>(Func<TResult> function, OperationType requestedOperation = OperationType.Write)
        {
            Lock(requestedOperation);
            try
            {
                return function();
            }
            finally
            {
                Unlock(requestedOperation);
            }
        }
        public void Invoke(VoidFunc function, OperationType requestedOperation = OperationType.Write)
        {
            Lock(requestedOperation);
            try
            {
                function();
            }
            finally
            {
                Unlock(requestedOperation);
            }
        }
        public void Invoke<T1, T2>(VoidFunc<T1, T2> function, T1 arg1, T2 arg2, OperationType requestedOperation = OperationType.Write)
        {
            Lock(requestedOperation);
            try
            {
                function(arg1, arg2);
            }
            finally
            {
                Unlock(requestedOperation);
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
        public bool TransactionRollback
        {
            get
            {
                return transactionRollback;
            }
            internal set
            {
                transactionRollback = value;
            }
        }
        private volatile bool transactionRollback;
        private object locker = new object();
        internal protected int LockCount
        {
            get
            {
                return lockCount;
            }
            set
            {
                lockCount = value;
            }
        }
        private volatile int lockCount;
    }
}
