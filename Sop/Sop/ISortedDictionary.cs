// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;

namespace Sop
{
    /// <summary>
    /// Non-generics Virtual Sorted Dictionary interface declares
    /// the methods necessary for implementation of a virtual Sorted Dictionary,
    /// i.e. - a Sorted Dictionary whose backend data Storage is abstracted
    /// for its Format(can be SOP, RDBMS,...) and Location(local disk or remote).
    /// </summary>
    public interface ISortedDictionary : Client.ISortedDictionary, IProxyObject
    //, Collections.BTree.ISynchronizer
    {
        /// <summary>
        /// Flush to disk all modified objects in-memory.
        /// </summary>
        void Flush();
    }

    /// <summary>
    /// Generic Virtual Sorted Dictionary interface.
    /// NOTE: SOP virtualizes the Sorted Dictionary between memory and disk.
    /// All items of the dictionary are stored on disk and mostly used items
    /// cached in-memory for high speed access.
    /// 
    /// Keys are allowed to have duplicates.
    /// Values can be nested Sorted Dictionaries if required.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface ISortedDictionary<TKey, TValue> : Client.ISortedDictionary<TKey, TValue>, ISortedDictionary
    {
        /// <summary>
        /// Transaction this Sorted Dictionary belongs to.
        /// </summary>
        ITransaction Transaction { get; set; }
    }
}
