
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

namespace Sop.Collections.BTree
{
    using System;
    using System.Threading;

    /// <summary>
    /// ICollection defines the BTree "custom" Collection interface
    /// </summary>
    internal interface ICollection
    {
        /// <summary>
        /// Returns the count of items stored in the Collection
        /// </summary>
        int Count { get; }

        /// <summary>
        /// MoveNext makes the next entry the current one
        /// </summary>
        bool MoveNext();

        /// <summary>
        /// MovePrevious makes the previous entry the current one
        /// </summary>
        bool MovePrevious();

        /// <summary>
        /// MoveFirst makes the first entry in the Collection the current one
        /// </summary>
        bool MoveFirst();

        /// <summary>
        /// MoveLast makes the last entry in the Collection the current one
        /// </summary>
        bool MoveLast();

        /// <summary>
        /// Returns the Current entry
        /// </summary>
        object CurrentEntry { get; }

        /// <summary>
        /// Search the Collection for existence of ObjectToSearch
        /// </summary>
        bool Search(object item);

        // return true if found, else false
        /// <summary>
        /// Add 'ObjectToAdd' to the Collection
        /// </summary>
        void Add(object item);

        /// <summary>
        /// Remove ObjectToRemove from the Collection if found, else throws an exception
        /// </summary>
        void Remove(object item);

        // return true if found & removed, else false
        /// <summary>
        /// Clear the Collection of all its items
        /// </summary>
        void Clear();

        /// <summary>
        /// Shallow copy the Collection and returns a duplicate Collection.
        /// </summary>
        object Clone();

        // Do a shallow copy of the Collection
    }
}