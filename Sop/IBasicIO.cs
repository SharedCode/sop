// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// Basic I/O interface.
    /// </summary>
    public interface IBasicIO : Client.IItemNavigation
    {
        /// <summary>
        /// Open the Object Store/File.
        /// </summary>
        void Open();

        /// <summary>
        /// Flush to disk all modified POCOs in-memory.
        /// </summary>
        void Flush();
    }
}
