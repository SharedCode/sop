using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sop.Log
{
    public class LogException : Exception
    {
        public LogException(string message) : base(message) { }
        public LogException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
