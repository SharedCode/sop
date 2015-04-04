using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using Sop.Collections.BTree;
using Sop.Persistence;

namespace Sop.Client
{

    /// <summary>
    /// Non-generics Virtual Sorted Dictionary interface declares
    /// the methods necessary for implementation of a virtual Sorted Dictionary,
    /// i.e. - a Sorted Dictionary whose backend data Storage is abstracted
    /// for its Format(can be SOP, RDBMS,...) and Location(local disk or remote).
    /// </summary>
    public interface ISortedDictionary : IPersistent, IItemNavigation,
                                         IWithHintBatchAction,
                                         IDisposable,
                                         IDeleteable        //, Collections.BTree.ISynchronizer
    {
        /// <summary>
        /// Auto Dispose Item when it gets removed from Cache or when it gets deleted
        /// </summary>
        bool AutoDisposeItem { get; set; }
        /// <summary>
        /// Auto Dispose this Store when it gets removed from the container's cache.
        /// </summary>
        bool AutoDispose { get; set; }

        /// <summary>
        /// true will store data in the Key region, 
        /// otherwise data will be stored in the Data region.
        /// NOTE: if data is small in size, it is recommended to be stored
        /// in Key region, otherwise in Data region.
        /// </summary>
        bool IsDataInKeySegment { get; }
        /// <summary>
        /// Locker object provides monitor type(enter/exit) of access locking to the Store.
        /// </summary>
        ISynchronizer Locker { get; }
    }

    /// <summary>
    /// Generics Virtual Sorted Dictionary interface.
    /// NOTE: SOP virtualizes the Sorted Dictionary between memory and disk.
    /// All items of the dictionary are stored on disk and mostly used items
    /// cached in-memory for high speed access.
    /// 
    /// Keys are allowed to have duplicates.
    /// Values can be nested Sorted Dictionaries if required.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface ISortedDictionary<TKey, TValue> : IDictionary<TKey, TValue>, ISortedDictionary
    {
        /// <summary>
        /// Add a group of items onto the Store.
        /// </summary>
        /// <param name="items"></param>
        void Add(KeyValuePair<TKey, TValue>[] items);

        /// <summary>
        /// Removes all items from the Sorted Dictionary.
        /// NOTE: Clear is synonymous to Delete in this version of SOP.
        /// </summary>
        new void Clear();

        /// <summary>
        /// Copy to a target array the contents of the Sorted Dictionary starting from the 1st item.
        /// When end of dictionary or end of target array is reached, copying will end.
        /// </summary>
        /// <param name="array"></param>
        /// <param name="arrayIndex"></param>
        new void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex);

        /// <summary>
        /// Returns current sort order. Setting to a different sort order will 
        /// reset BTree. First item according to sort order will be current item.
        /// </summary>
        SortOrderType SortOrder { get; set; }

        /// <summary>
        /// Returns true if current record pointer is beyond last item in tree.
        /// </summary>
        /// <returns></returns>
        bool EndOfTree();

        /// <summary>
        /// Returns current item's key
        /// </summary>
        TKey CurrentKey { get; }

        /// <summary>
        /// Returns/sets current item's value
        /// </summary>
        TValue CurrentValue { get; set; }

        /// <summary>
        /// Comparer used to compare Item's Key and provide sorted behavior
        /// </summary>
        IComparer<TKey> Comparer { get; set; }

        /// <summary>
        /// Returns the Current Sequence of the Sorted Dictionary
        /// </summary>
        long CurrentSequence { get; }

        /// <summary>
        /// Returns the name of this Sorted Dictionary
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Increment the Sorted Dictionary's Sequence and return its new value
        /// </summary>
        /// <returns></returns>
        long GetNextSequence();

        /// <summary>
        /// Search for a Key and allows code to position the record pointer to the
        /// 1st occurrence in the case of Key having duplicates.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="goToFirstOccurrence">true will cause Search to position item pointer to 1st occurrence in the case of duplicated Keys</param>
        /// <returns></returns>
        bool Search(TKey key, bool goToFirstOccurrence);

        /// <summary>
        /// Search for a Key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        bool Search(TKey key);

        /// <summary>
        /// Rename this Sorted Dictionary
        /// </summary>
        /// <param name="newName"></param>
        void Rename(string newName);

        /// <summary>
        /// Remove the currently selected Item from the Sorted Dictionary
        /// </summary>
        /// <returns>true if there is a selected item and it got deleted, false otherwise</returns>
        bool Remove();

        /// <summary>
        /// Remove all occurences of Items with the given Key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="removeAllOccurence"></param>
        /// <returns></returns>
        bool Remove(TKey key, bool removeAllOccurence);

        /// <summary>
        /// Remove Items matching the specified query filter expressions.
        /// </summary>
        /// <param name="keyExpressions"></param>
        /// <param name="results"></param>
        /// <returns></returns>
        bool Remove(QueryExpression<TKey>[] keyExpressions, out QueryResult<TKey>[] results);

        /// <summary>
        /// Remove Items matching the specified query filter expressions.
        /// </summary>
        /// <param name="keyExpressions"></param>
        /// <returns></returns>
        bool Remove(QueryExpression<TKey>[] keyExpressions);

        /// <summary>
        /// Remove Items matching the specified query filter expressions.
        /// </summary>
        /// <param name="keyExpressions"></param>
        /// <param name="removeAllOccurence">true will remove all occurences of each Key in Keys</param>
        /// <param name="results"> </param>
        /// <returns></returns>
        bool Remove(QueryExpression<TKey>[] keyExpressions, bool removeAllOccurence, out QueryResult<TKey>[] results);

        /// <summary>
        /// Query the B-Tree for each Keys submitted, returns query result
        /// as array of information containing key, query result, value.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="results"></param>
        /// <returns>true if found at least one key, otherwise false</returns>
        bool Query(QueryExpression<TKey>[] keys, out QueryResult<TKey>[] results);

        /// <summary>
        /// Update Items with given Keys
        /// </summary>
        /// <param name="items"></param>
        /// <returns></returns>
        bool Update(KeyValuePair<TKey, TValue>[] items);
    }
}
