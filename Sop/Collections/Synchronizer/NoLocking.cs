#if (SINGLE_THREADED)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sop.Collections
{
    public class Synchronizer : ISynchronizer
    {
        public bool IsLocked
        {
            get { return false; }
        }

        public void CommitLockRequest(bool lockFlag = true)
        {
        }

        public void WaitForCommitLock(bool lockFlag = true)
        {
        }

        public void Lock(OperationType requestedOperation = OperationType.Write)
        {
        }

        public void Unlock(OperationType requestedOperation = OperationType.Write)
        {
        }

        public void Invoke(VoidFunc function)
        {
            function();
        }

        public void Invoke<T1, T2>(VoidFunc<T1, T2> function, T1 arg1, T2 arg2)
        {
            function(arg1, arg2);
        }

        public TResult Invoke<TResult>(Func<TResult> function)
        {
            return function();
        }

        public TResult Invoke<T1, TResult>(Func<T1, TResult> function, T1 arg)
        {
            return function(arg);
        }

        public bool TransactionRollback
        {
            get;
            internal set;
        }
    }
}
#endif
