// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// Deleteable interface.
    /// </summary>
    public interface IDeleteable
    {
        /// <summary>
        /// Implement to delete Entity from disk.
        /// </summary>
        void Delete();
    }
}
