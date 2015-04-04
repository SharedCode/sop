using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sop.Server
{
    /// <summary>
    /// SOP Server Exception is thrown for errors originating from the Sop.Server module (dll).
    /// </summary>
    public class SopServerException : SopException
    {
        public SopServerException(string message) : base(message)
        {
        }
        public SopServerException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

    }
}
