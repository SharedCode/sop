// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
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
}
