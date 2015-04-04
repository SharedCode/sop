using System;

namespace Sop.Mru.Generic
{
    /// <summary>
    /// MRU cache Manager interface
    /// </summary>
    public interface IMruManager<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// Add a Key/Value pair to the MRU cache
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        void Add(TKey key, TValue value);

        /// <summary>
        /// Clear objects from MRU cache
        /// </summary>
        void Clear();

        /// <summary>
        /// Check whether object with Key is in cache
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        bool Contains(TKey key);

        /// <summary>
        /// Return count of Objects in cache
        /// </summary>
        int Count { get; }

        /// <summary>
        /// true if MRU is in MaxCapacity, false otherwise.
        /// </summary>
        bool IsFull { get; }

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
        /// Remove Objects identified in keys.
        /// NOTE: during MaxCapacity, Objects pruning occurs by Remove getting
        /// invoked and removed Objects persisted to Disk/virtual target store.
        /// If RemoveFromCache is true, Objects are only removed from cache
        /// and not persisted to store
        /// </summary>
        /// <param name="keys"></param>
        void Remove(System.Collections.Generic.ICollection<TKey> keys);

        /// <summary>
        /// Remove item from MRU.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        object Remove(TKey key);

        /// <summary>
        /// Save the Objects in cache to target store
        /// </summary>
        void Flush();

        /// <summary>
        /// Binds the MRU Client to the DataDriver. DataDriver is used
        /// for persistence of cached Objects to target store
        /// </summary>
        /// <param name="parent"></param>
        void SetDataStores(IMruClient parent /*, IDataBlockDriver DataDriver*/);

        /// <summary>
        /// get/set Object to/from cache
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        TValue this[TKey key] { get; set; }

        /// <summary>
        /// Returns Keys of Objects in cache
        /// </summary>
        System.Collections.Generic.ICollection<TKey> Keys { get; }

        /// <summary>
        /// Returns Objects in cache
        /// </summary>
        //System.Collections.Generic.ICollection<TValue> Values { get; }
        /// <summary>
        /// Returns Objects in cache
        /// </summary>
        System.Collections.Generic.ICollection<MruItem<TKey, TValue>> MruItems { get; }

        /// <summary>
        /// Save State
        /// </summary>
        Sop.Mru.SaveTypes SaveState { get; set; }

        /// <summary>
        /// true will generate Prune events, else will not
        /// </summary>
        bool GeneratePruneEvent { get; set; }

        /// <summary>
        /// Peek in tail.
        /// </summary>
        /// <returns></returns>
        MruItem<TKey, TValue> PeekInTail();

        /// <summary>
        /// Get and remove item from tail. This is useful during MaxCapacity when code is reducing MRU load.
        /// </summary>
        /// <returns></returns>
        MruItem<TKey, TValue> RemoveInTail();
    }
}