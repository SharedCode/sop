// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections;
using Sop.Mru;

[assembly: CLSCompliant(true)]

namespace Sop.OnDisk
{
    using Mru;

    /// <summary>
    /// InternalPersistent Object Reference interface
    /// </summary>
    internal interface IInternalPersistentRef
    {
        /// <summary>
        /// DataAddress
        /// </summary>
        long DataAddress { get; set; }

        /// <summary>
        /// MRU cache manager
        /// </summary>
        IMruManager MruManager { get; set; }
    }
}