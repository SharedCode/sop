// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
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
        /// Returns this Store's URI path.
        /// </summary>
        string Path { get; }
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
    public interface ISortedDictionary<TKey, TValue> : Client.ISortedDictionary<TKey, TValue>, ISortedDictionary, 
        IEnumerable<KeyValuePair<TKey, TValue>>
        // explicitly declare IEnumerable as one of interface to implement to enable LINQ !
    {
        /// <summary>
        /// Transaction this Sorted Dictionary belongs to.
        /// </summary>
        ITransaction Transaction { get; set; }

        /// <summary>
        /// Returns the Store's File.
        /// </summary>
        IFile File { get; }

        /// <summary>
        /// When Data Value is saved in its own segment (IsDataInKeySegment = false),
        /// True will auto flush the Data Value per each Add/Update action,
        /// False otherwise. Defaults to False.
        /// 
        /// Auto flush feature is useful for example, when managing 
        /// big data entities where user will want to save the record
        /// to disk and prevent it from getting buffered, which will
        /// impact server performance due to big memory requirements
        /// of the data.
        /// </summary>
        bool AutoFlush { get; set; }

        /// <summary>
        /// true means Dictionary is hinted to be used for read-only access.
        /// If true, succeeding reader method calls will issue a reader lock.
        /// Management methods (add, remove, update) will actually ignore this hint
        /// and issue a writer lock to protect the Store's data integrity.
        /// </summary>
        bool HintReadOnly { get; set; }
    }
}
