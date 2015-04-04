using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace Sop.OnDisk.IO
{
    /// <summary>
    /// SOP IO Exceptions.
    /// </summary>
    public class IOExceptions : SopException
    {
        public IOExceptions(string message) : base(message) { }
        public IOExceptions(string message, Exception innerException) :
            base(message, innerException) { }

        /// <summary>
        /// Set of Exception encountered across other threads of execution.
        /// This exception contains the 1st reporting thread exception,
        /// OtherThreadExceptions contain the rest of threads' reported exceptions.
        /// </summary>
        public ConcurrentBag<Exception> OtherThreadExceptions = new ConcurrentBag<Exception>();
    }
}
