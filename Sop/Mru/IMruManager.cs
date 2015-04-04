using System;
using System.Collections.Generic;
using Sop.Collections.BTree;
using Sop.OnDisk.DataBlock;
using Sop.Persistence;
using ICollection = System.Collections.ICollection;

namespace Sop.Mru
{
    using OnDisk;

    /// <summary>
    /// MRU cache Manager interface
    /// </summary>
    internal interface IMruManager : IDisposable
    {
        /// <summary>
        /// Add a Key/Value pair to the MRU cache
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        void Add(object key, object value);

        /// <summary>
        /// Clear objects from MRU cache
        /// </summary>
        void Clear();

        /// <summary>
        /// true means at least one item in MRU cache is dirty, otherwise all items aren't dirty
        /// </summary>
        bool IsDirty { get; }

        /// <summary>
        /// Check whether object with Key is in cache
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        bool Contains(object key);

        /// <summary>
        /// Return count of Objects in cache
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Maximum MRU Capacity. If count of Objects in cache reached this
        /// number, cache is pruned to the MinCapacity
        /// </summary>
        int MaxCapacity { get; set; }

        /// <summary>
        /// Minimum MRU Capacity.
        /// </summary>
        int MinCapacity { get; set; }
        /// <summary>
        /// Remove item from the tail end with option to move the removed item onto remove list.
        /// </summary>
        /// <param name="moveToRemoveList"></param>
        /// <returns></returns>
        MruItem RemoveInTail(bool moveToRemoveList);
        /// <summary>
        /// Recycle an object.
        /// </summary>
        /// <param name="recycledObject"></param>
        void Recycle(IInternalPersistent recycledObject);
        /// <summary>
        /// Return a recycled object.
        /// </summary>
        /// <returns></returns>
        IInternalPersistent GetRecycledObject();

        /// <summary>
        /// Remove Objects identified in keys.
        /// NOTE: during MaxCapacity, Objects pruning occurs by Remove getting
        /// invoked and removed Objects persisted to Disk/virtual target store.
        /// If RemoveFromCache is true, Objects are only removed from cache
        /// and not persisted to store
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="removeFromCache"></param>
        void Remove(ICollection keys, bool removeFromCache);
        /// <summary>
        /// Remove item with given key with an option to remove item from cache or not.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="removeFromCache"></param>
        /// <returns></returns>
        object Remove(object key, bool removeFromCache);
        /// <summary>
        /// Remove item with given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        object Remove(object key);

        /// <summary>
        /// Save the Objects in cache to target store
        /// </summary>
        void Flush();

        /// <summary>
        /// Binds the MRU Client to the DataDriver. DataDriver is used
        /// for persistence of cached Objects to target store
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="dataDriver"></param>
        void SetDataStores(IMruClient parent, object dataDriver);

        /// <summary>
        /// Returns the Parent Collection.
        /// </summary>
        /// <returns></returns>
        IMruClient GetParent();

        Collections.Generic.SortedDictionary<object, object> RemovedObjects { get; }

        /// <summary>
        /// get/set Object to/from cache
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        object this[object key] { get; set; }

        /// <summary>
        /// Returns Keys of Objects in cache
        /// </summary>
        ICollection<object> Keys { get; }

        /// <summary>
        /// Returns Objects in cache
        /// </summary>
        ICollection<object> Values { get; }

        /// <summary>
        /// Save State
        /// </summary>
        SaveTypes SaveState { get; set; }

        /// <summary>
        /// true will generate Prune events, else will not
        /// </summary>
        bool GeneratePruneEvent { get; set; }
    }
}