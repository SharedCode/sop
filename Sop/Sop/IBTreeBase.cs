// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// B-Tree Base interface. Both In-Memory and On-Disk
    /// B-Tree versions implement IBTreeBase interface.
    /// </summary>
    public interface IBTreeBase : IDictionary, ICloneable, IBasicIO
    {
        /// <summary>
        /// Returns current sort order. Setting to a different sort order will 
        /// reset BTree. First item according to sort order will be current item.
        /// </summary>
        SortOrderType SortOrder { get; set; }

        /// <summary>
        /// Returns current item's key
        /// </summary>
        object CurrentKey { get; }

        /// <summary>
        /// Returns/sets current item's value
        /// </summary>
        object CurrentValue { get; set; }

        /// <summary>
        /// Returns true if current record pointer is beyond last item in tree.
        /// </summary>
        /// <returns></returns>
        bool EndOfTree();

        /// <summary>
        /// Search BTreeAlgorithm for the entry having its key equal to 'Key'
        /// </summary>
        /// <param name="key">Key of record to search for</param>
        /// <returns>true if successful, false otherwise</returns>
        bool Search(object key);

        /// <summary>
        /// Search btree for the entry having its key equal to 'Key'
        /// </summary>
        /// <param name="key">Key of record to search for</param>
        /// <param name="goToFirstInstance">if true and Key is duplicated, will make first instance of duplicated keys the current record so one can easily get/traverse all records having the same keys using 'MoveNext' function</param>
        /// <returns>true if successful, false otherwise</returns>
        bool Search(object key, bool goToFirstInstance);

        /// <summary>
        /// Returns the number of items in the btree
        /// </summary>
        /// <summary>
        /// Returns the current item (value and key pair) contained in 'DictionaryEntry' object.
        /// </summary>
        DictionaryEntry CurrentEntry { get; }
    }
}
