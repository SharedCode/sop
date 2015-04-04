
#region History Log

//Log :
// 6/3/2001  .NET migration
// 6/18/1998 Initial version.

#endregion

namespace Sop.Collections.BTree
{
    using System.Threading;

    /// <summary>
    ///    SingleThreadAccess contains api for single thread access to a resource.
    /// </summary>
    internal class SingleThreadAccess // : ISingleThreadAccess
    {
        /// <summary>
        /// Lock this resource for update. This allows only a single thread to get
        /// access to the resource.
        /// </summary>
        public void Lock()
        {
            Monitor.Enter(_locker);
        }

        /// <summary>
        /// Unlock this resource so other thread(s) may acquire a Lock on it.
        /// </summary>
        public void Unlock()
        {
            Monitor.Exit(_locker);
        }

        private readonly object _locker = new object();
    }
}
