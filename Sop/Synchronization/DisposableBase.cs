using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sop.Synchronization
{
    /// <summary>
    /// Disposable base class contains consumer code and SOP internal code facing
    /// dispose related methods.
    /// </summary>
    public abstract class DisposableBase : IDisposable
    {
        #region IDisposable pattern
        // Note to developers: Disposable pattern was modified to support SOP's "delayed" disposal of Locker 
        //  or SyncRoot object (this). Support for multi-threaded usage of the Store requires this thread-safe
        //  SyncRoot (this) modified dispose pattern.


        /// <summary>
        /// true means Dispose was invoked and thus, this object (member resources) needs
        /// to get freed up whenever possible.
        /// </summary>
        volatile bool disposeSignalled;

        protected bool DisposeSignalled { get { return disposeSignalled; } }

        /// <summary>
        /// true means this object was disposed.
        /// </summary>
        protected bool Disposed { get; set; }

        // Public implementation of Dispose pattern callable by consumers.
        public void Dispose()
        {
            Dispose(true);
        }
        // Protected implementation of Dispose pattern.
        internal protected virtual void Dispose(bool disposing)
        {
            if (disposeSignalled || !disposing)
                return;
            disposeSignalled = true;
        }
        /// <summary>
        /// Dispose for call by SOP's internal disposable object management.
        /// </summary>
        /// <returns>true if this object just got disposed with this call.
        /// false if this object is not ready to be disposed or if it is already 
        /// disposed previous to this call.</returns>
        internal protected virtual bool InternalDispose()
        {
            if (Disposed)
                return false;
            if (disposeSignalled)
            {
                Disposed = true;
                GC.SuppressFinalize(this);
                return true;
            }
            return false;
        }
        ~DisposableBase()
        {
            Dispose(false);
        }
        #endregion
    }
}
