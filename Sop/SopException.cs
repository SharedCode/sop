// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// SOP exception.
    /// </summary>
    public class SopException : Exception
    {
        public SopException(string message) : base(message)
        {
        }
        public SopException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
    /// <summary>
    /// Duplicate Key exception is raised when adding a Key/Value
    /// pair entry to Store failed due to detection of an existing
    /// entry with the same Key.
    /// 
    /// NOTE: Transaction is not rolled back when this exception
    /// is raised.
    /// </summary>
    public class DuplicateKeyException: SopException
    {
        public DuplicateKeyException(string message) : base(message)
        {
        }
        public DuplicateKeyException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
