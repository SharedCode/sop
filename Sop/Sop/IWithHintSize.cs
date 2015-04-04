// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// Interface has HintSize
    /// </summary>
    public interface IWithHintSize
    {
        /// <summary>
        /// Implement to return the size on disk(in bytes) of this object,
        /// or return 0 and SOP will ignore it.
        /// </summary>
        int HintSizeOnDisk { get; }
    }
}
