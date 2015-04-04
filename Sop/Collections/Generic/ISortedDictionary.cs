
#region History Log

/* DESCRIPTION       :                                           *
* This class provides the generic and extendible Collection      *
* class and its item base object. Collection is an extendible    *
* container type of class. It defines the overridable methods    *
* for managing contained objects. Benefit of this is that        *
* applications could easily switch Collection types whenever they*
* want to. For example, initially, an array Collection was used  *
* and later it was found that array searching is so slow and a   *
* need to use a Collection offering faster searches then, a      *
* Btree(look for Gerry's Btree Collection implementation)        *
* Collection might be introduced and could be easily swapped to  *
* the old array Collection. Only the code that instantiates the  *
* Collection object needs to be changed.                         */

// 06/02/2001   Gerardo
// - .NET migration work.

// 04/30/1999	Gerardo
// - Write operation request will no longer wait until all overlapping Read 
//	requests had been served. After serving the last Read operation and the
//	requested operation is Write, will prioritize the Write in that succeeding
//	Read requests while Write is waiting for its turn will wait until the Write
//	operation had been served.

// 04/24/1999	Gerardo
// - Decided to support Key & Data through inheritance. Descendant Collection
//	classes will be introduced to support it.

// 03/23/1999	Gerardo
// - Changed back return types from bool to 'short'. 'short' will
//	allow us to support multiple error return types.
// 06/18/1998	Gerardo
// - Added multi-thread support.

#endregion

using System.Collections.Generic;

namespace Sop.Collections.Generic
{
    /// <summary>
    /// In-Memory Sorted Dictionary interface.
    /// </summary>
    public interface ISortedDictionary<TKey, TValue> : System.Collections.Generic.IDictionary<TKey, TValue>,
                                                       IBaseCollection<TKey>
    {
        /// <summary>
        /// true if end of tree is reached (CurrentItem is null), otherwise false.
        /// </summary>
        /// <returns></returns>
        bool EndOfTree();

        /// <summary>
        /// Implement to copy items from source onto this instance.
        /// </summary>
        /// <param name="source"></param>
        void Copy(ISortedDictionary<TKey, TValue> source);

        /// <summary>
        /// Returns current sort order. Setting to a different sort order will 
        /// reset BTree. First item according to sort order will be current item.
        /// </summary>
        SortOrderType SortOrder { get; set; }

        /// <summary>
        /// Returns the Current entry.
        /// </summary>
        KeyValuePair<TKey, TValue>? CurrentEntry { get; }

        /// <summary>
        /// Returns the Current entry's key.
        /// </summary>
        TKey CurrentKey { get; }

        /// <summary>
        /// Returns the Current entry's Value.
        /// </summary>
        TValue CurrentValue { get; set; }

        /// <summary>
        /// Remove an entry from Sorted Dictionary.
        /// </summary>
        void Remove();

        /// <summary>
        /// Returns the SyncRoot as an ISynchronizer interface.
        /// This is used for managing this entity's locking for thread safety.
        /// </summary>
        ISynchronizer Locker { get; }
    }
}