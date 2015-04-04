using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sop.SystemInterface
{
    /// <summary>
    /// System Adaptor for use on interfacing with Systems such as Windows (Win32), Linux
    /// and other future systems to be supported, where SOP will be ported.
    /// </summary>
    public static class SystemAdaptor
    {
        /// <summary>
        /// Returns a System interface.
        /// </summary>
        public readonly static ISystemInterface SystemInterface = new SystemInterface();
    }
}
