// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// Collection operation types.
    /// </summary>
    public enum OperationType
    {
        /// <summary>
        /// In Move, Search or Get current object/key/value operation.
        /// </summary>
        Read,

        /// <summary>
        /// In either Add, Update or Delete operation.
        /// </summary>
        Write
    }
}
