using System;
using System.Threading;

namespace Sop
{
    public delegate void VoidFunc();
    public delegate void VoidFunc<in T1>(T1 arg1);
    public delegate void VoidFunc<in T1, in T2>(T1 arg1, T2 arg2);
    public delegate void VoidFunc<in T1, in T2, in T3>(T1 arg1, T2 arg2, T3 arg3);

    namespace Collections
    {
        /// <summary>
        /// Thread Synchronization interface.
        /// </summary>
        public interface ISynchronizer
        {
            /// <summary>
            ///  Returns true if this resource is locked, false otherwise.
            /// </summary>
            bool IsLocked { get; }
            /// <summary>
            /// CommitRequestLock is used before and after the Server commits or cycles
            /// the Transaction. It needs to lock all the live Stores before it proceed with
            /// the commit process and unlock them afterwards.
            /// </summary>
            /// <param name="lockFlag">true will lock, false will unlock.</param>
            void CommitLockRequest(bool lockFlag = true);
            /// <summary>
            /// Does a spin wait until a commit lock is detected.
            /// </summary>
            void WaitForCommitLock(bool lockFlag = true);
            /// <summary>
            /// Lock this resource.
            /// </summary>
            /// <param name="requestedOperation"></param>
            int Lock(OperationType requestedOperation = OperationType.Write);
            /// <summary>
            /// Unlock this resource. If code that calls this is within an Invoke
            /// call, this will unlock resource right away and Invoke's 'finally' will 
            /// skip the Unlock call.
            /// </summary>
            /// <param name="requestedOperation"></param>
            int Unlock(OperationType requestedOperation = OperationType.Write);
            /// <summary>
            /// Thread-safe call a lambda expression. Call will be wrapped inside
            /// Lock/Unlock calls.
            /// </summary>
            /// <param name="function"></param>
            void Invoke(VoidFunc function);
            /// <summary>
            /// Thread-safe call a lambda expression. Call will be wrapped inside
            /// Lock/Unlock calls.
            /// </summary>
            /// <typeparam name="T1"></typeparam>
            /// <typeparam name="T2"></typeparam>
            /// <param name="function"></param>
            /// <param name="arg1"></param>
            /// <param name="arg2"></param>
            void Invoke<T1, T2>(VoidFunc<T1, T2> function, T1 arg1, T2 arg2);
            /// <summary>
            /// Thread-safe call a lambda expression. Call will be wrapped inside
            /// Lock/Unlock calls.
            /// </summary>
            /// <typeparam name="TResult"></typeparam>
            /// <param name="function"></param>
            /// <returns></returns>
            TResult Invoke<TResult>(Func<TResult> function);
            /// <summary>
            /// Thread-safe call a lambda expression. Call will be wrapped inside
            /// Lock/Unlock calls.
            /// </summary>
            /// <typeparam name="T1"></typeparam>
            /// <typeparam name="TResult"></typeparam>
            /// <param name="function"></param>
            /// <param name="arg"></param>
            /// <returns></returns>
            TResult Invoke<T1, TResult>(Func<T1, TResult> function, T1 arg);
            /// <summary>
            /// true signifies current transaction was rolled back.
            /// </summary>
            bool TransactionRollback { get; }
        }
    }
}
