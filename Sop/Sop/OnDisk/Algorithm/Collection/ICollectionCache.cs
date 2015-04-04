// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using Sop.Mru;

namespace Sop.OnDisk.Algorithm.Collection
{
    /// <summary>
    /// Collection Cache interface.
    /// </summary>
    internal interface ICollectionCache
    {
        /// <summary>
        /// Objects MRU cache manager.
        /// </summary>
        IMruManager MruManager { get; set; }
        /// <summary>
        /// Data Blocks cache.
        /// </summary>
        Collections.Generic.ISortedDictionary<long, Sop.DataBlock> Blocks { get; set; }
    }
}
