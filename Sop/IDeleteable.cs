// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
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
        /// Delete this Entity from disk.
        /// E.g. - for a Store, calling this method will remove this Store
        /// from its Container.
        /// </summary>
        void Delete();
    }
}
