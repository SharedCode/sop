using System;
using System.Collections.Generic;
using System.Text;

namespace Sop.Client
{
    /// <summary>
    /// Item navigation interface.
    /// </summary>
    public interface IItemNavigation
    {
        /// <summary>
        /// Move pointer to first entry in the collection.
        /// </summary>
        /// <returns></returns>
        bool MoveFirst();
        /// <summary>
        /// Advance the pointer to the next entry in the collection.
        /// </summary>
        /// <returns></returns>
        bool MoveNext();
        /// <summary>
        /// Backtracks the pointer to the previous entry in the collection.
        /// </summary>
        /// <returns></returns>
        bool MovePrevious();
        /// <summary>
        /// Move pointer to last entry in the collection.
        /// </summary>
        /// <returns></returns>
        bool MoveLast();
    }
}
