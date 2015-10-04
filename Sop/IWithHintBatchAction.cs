// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// Interface for defining Hints to optimize batch operations
    /// </summary>
    public interface IWithHintBatchAction
    {
        /// <summary>
        /// Hint SOP that next operation is a sequential read.
        /// Hints allow optimization to occur
        /// </summary>
        bool HintSequentialRead { get; set; }

        /// <summary>
        /// Hint that allows code to tell SOP to optimize sequential reading and batch removal of items.
        /// This Hint tells how many items a batch contains.
        /// </summary>
        int HintBatchCount { get; set; }
    }
}