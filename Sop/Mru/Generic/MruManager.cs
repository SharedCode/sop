using System;
using System.Collections.Generic;
using Sop.OnDisk;
using Sop.Persistence;

namespace Sop.Mru.Generic
{
    /// <summary>
    /// MRU algorithm. Services MRU objects management in Collections on disk.
    /// Characteristics are as follows:
    /// 1) Maintains Most Recently Used (MRU) objects in memory
    /// 2) When full, offload objects onto disk using associated CollectionOnDisk (a.k.a. - data store)
    /// 3) If object being accessed is not in memory, automatically loads it from disk and added to MRU pool
    /// 
    /// NOTE: Full is either maximum count of objects had been reached or memory (RAM) is full, necessitating
    /// objects offload into disk
    /// </summary>
    public class MruManager<TKey, TValue> : IMruManager<TKey, TValue>
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public MruManager()
        {
            CacheCollection = new Collections.Generic.SortedDictionary<TKey, MruItem<TKey, TValue>>();
        }

        public MruManager(int minCapacity, int maxCapacity) :
            this(minCapacity, maxCapacity, null)
        {
        }

        /// <summary>
        /// Constructor expecting MinCapacity & MaxCapacity
        /// </summary>
        /// <param name="minCapacity"></param>
        /// <param name="maxCapacity"></param>
        /// <param name="comparer"> </param>
        public MruManager(int minCapacity, int maxCapacity, IComparer<TKey> comparer)
        {
            mruManager = new InternalMruManager<TKey, TValue>(minCapacity, maxCapacity);
            CacheCollection = new Collections.Generic.SortedDictionary<TKey, MruItem<TKey, TValue>>(comparer);
        }

        internal bool? AutoDisposeItem { get; set; }
        public void Dispose()
        {
            foreach (var entry in CacheCollection)
            {
                DisposeItem(entry.Key);
                DisposeItem(entry.Value);
            }
            Clear();
        }
        private void DisposeItem(object item)
        {
            if (item is IDisposable)
            {
                if (AutoDisposeItem != null &&
                    item is SpecializedDataStore.SpecializedStoreBase)
                {
                    ((SpecializedDataStore.SpecializedStoreBase)item).Collection.AutoDisposeItem = AutoDisposeItem.Value;
                }
                ((IDisposable)item).Dispose();
                if (item is SpecializedDataStore.SpecializedStoreBase)
                {
                    ((SpecializedDataStore.SpecializedStoreBase)item).Collection = null;
                }
            }
        }

        /// <summary>
        /// true if MRU is in MaxCapacity, false otherwise.
        /// </summary>
        public bool IsFull
        {
            get { return Count >= MaxCapacity; }
        }

        /// <summary>
        /// Get & remove item from tail. This is useful during MaxCapacity when code is reducing MRU load.
        /// </summary>
        /// <returns></returns>
        public MruItem<TKey, TValue> GetFromTail()
        {
            var r = this.PeekInTail();
            RemoveInTail(false);
            return r;
        }

        /// <summary>
        /// true will use cache entry recycling for more efficient memory management.
        /// otherwise will not
        /// </summary>
        public bool RecycleEnabled = false;

        /// <summary>
        /// Contains the objects for recycling
        /// </summary>
        public List<IInternalPersistent> RecycledObjects = new List<IInternalPersistent>();

        /// <summary>
        /// Add Objects to the RecycledObjects collection
        /// </summary>
        /// <param name="objects"></param>
        public void Recycle(System.Collections.Generic.IEnumerable<IInternalPersistent> objects)
        {
            if (RecycleEnabled)
            {
                foreach (IInternalPersistent o in objects)
                    Recycle(o);
            }
        }

        /// <summary>
        /// Add Object to the RecycledObjects collection
        /// </summary>
        /// <param name="Object"></param>
        public void Recycle(IInternalPersistent Object)
        {
            if (RecycleEnabled)
            {
                if (Object is Recycling.IRecyclable)
                    ((Recycling.IRecyclable) Object).Initialize();
                RecycledObjects.Add(Object);
            }
        }

        /// <summary>
        /// Attempts to actually recycle an Object from recycle bin(RecycledObjects collection)
        /// </summary>
        /// <returns></returns>
        public IInternalPersistent GetRecycledObject()
        {
            if (RecycleEnabled && RecycledObjects.Count > 0)
            {
                IInternalPersistent r = RecycledObjects[0];
                RecycledObjects.RemoveAt(0);
                return r;
            }
            return null;
        }

        /// <summary>
        /// Save Removed Blocks to target store
        /// </summary>
        public void SaveRemovedBlocks()
        {
            mruManager.SaveRemovedBlocks();
        }

        /// <summary>
        /// Save the unpersisted MRU items
        /// </summary>
        public virtual void Flush()
        {
            if ((SaveState & Sop.Mru.SaveTypes.CollectionSave) ==
                Sop.Mru.SaveTypes.CollectionSave)
                return;
            SaveState |= Sop.Mru.SaveTypes.CollectionSave;
            if (CacheCollection.Count > 0)
            {
                const int batchLimit = 90;
                var dirtyNodes = new List<IInternalPersistent>(batchLimit);
                int batchCount = 0;
                foreach (Node<TKey, TValue> n in mruManager)
                {
                    MruItem<TKey, TValue> itm = n.Data;
                    if (itm.Value is IInternalPersistent)
                    {
                        if (((IInternalPersistent) itm.Value).IsDirty)
                        {
                            dirtyNodes.Add((IInternalPersistent) itm.Value);
                            if (++batchCount > batchLimit)
                            {
                                if (mruManager.Collection != null)
                                {
                                    if (dirtyNodes.Count > 0)
                                    {
                                        mruManager.Collection.OnMaxCapacity(dirtyNodes);
                                        mruManager.Collection.OnMaxCapacity();
                                    }
                                }
                                dirtyNodes.Clear();
                                batchCount = 0;
                            }
                        }
                    }
                    else
                        break;
                }
                if (mruManager.Collection != null)
                {
                    if (dirtyNodes.Count > 0)
                    {
                        mruManager.Collection.OnMaxCapacity(dirtyNodes);
                        mruManager.Collection.OnMaxCapacity();
                    }
                }
            }
            SaveState ^= Sop.Mru.SaveTypes.CollectionSave;
        }

        /// <summary>
        /// Binds this MRU Manager to a MRUClient(Parent)
        /// </summary>
        /// <param name="parent"></param>
        public void SetDataStores(IMruClient parent /*, IDataBlockDriver DataDriver*/)
        {
            //mruManager.DataDriver = DataDriver;
            mruManager.Collection = parent;
        }

        /// <summary>
        /// get/set flag for Prune event
        /// </summary>
        public bool GeneratePruneEvent
        {
            get { return mruManager.GeneratePruneEvent; }
            set { mruManager.GeneratePruneEvent = value; }
        }

        /// <summary>
        /// Save State
        /// </summary>
        public Sop.Mru.SaveTypes SaveState
        {
            get { return mruManager.SaveState; }
            set { mruManager.SaveState = value; }
        }

        /// <summary>
        /// Add Object to the cache
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public virtual void Add(TKey key, TValue value)
        {
            if (SaveState != SaveTypes.Default) return;
            MruItem<TKey, TValue> itm = null;
            if (GeneratePruneEvent)
            {
                if (MaxCapacity > 0 && CacheCollection.Count >= MaxCapacity)
                {
                    if (mruManager.RemovedObjects.Count == 0)
                    {
                        bool ogp = GeneratePruneEvent;
                        GeneratePruneEvent = false;
                        itm = mruManager.PeekInTail();
                        // prevent pruning to occur if itm is Specialized Store and is locked by a thread...
                        if (!(itm.Value is SpecializedDataStore.SpecializedStoreBase &&
                              ((SpecializedDataStore.SpecializedStoreBase)(object)itm.Value).Locker.IsLocked))
                        {
                            Log.Logger.Instance.Log(Log.LogLevels.Information, "GenericMruManager.Add: pruning enter.");
                            for (int i = MaxCapacity; i > MinCapacity; i--)
                            {
                                itm = mruManager.RemoveInTail(mruManager.Collection != null);
                                CacheCollection.Remove(itm.Key);
                                if (!(itm.Value is IDisposable)) continue;
                                if (itm.Value is SpecializedDataStore.SpecializedStoreBase)
                                {
                                    if (((SpecializedDataStore.SpecializedStoreBase)(object)itm.Value).Locker.IsLocked)
                                        continue;
                                    ((SpecializedDataStore.SpecializedStoreBase)(object)itm.Value).InvokeFromMru =
                                        true;
                                }
                                if (mruManager.Collection == null)
                                    ((IDisposable) itm.Value).Dispose();
                                if (itm.Value is SpecializedDataStore.SpecializedStoreBase)
                                    ((SpecializedDataStore.SpecializedStoreBase) (object) itm.Value).InvokeFromMru =
                                        false;
                            }
                            Log.Logger.Instance.Log(Log.LogLevels.Information, "GenericMruManager.Add: pruning exit.");
                            mruManager.SaveRemovedBlocks();
                        }
                        GeneratePruneEvent = ogp;
                    }
                }
            }
            itm = (MruItem<TKey, TValue>) Remove(key);
            if (itm == null)
            {
                itm = new MruItem<TKey, TValue>(key, value, Transaction);
                itm.IndexToMruList = mruManager.AddInHead(itm, false);
            }
            else
            {
                itm.Transaction = Transaction;
                itm.Key = key;
                itm.Value = value;
                mruManager.AddInHead(itm.IndexToMruList, true);
            }
            CacheCollection[key] = itm;
        }

        public Transaction.ITransactionLogger Transaction = null;

        /// <summary>
        /// Returns the number of items in the Manager
        /// </summary>
        public virtual int Count
        {
            get { return CacheCollection.Count; }
        }

        /// <summary>
        /// Given a key, will return its value.
        /// If key is not found, will add a new entry having passed 
        /// params key and value.
        /// </summary>
        public TValue this[TKey key]
        {
            get
            {
                MruItem<TKey, TValue> itm = this.CacheCollection[key];
                if (itm == null)
                    return default(TValue);
                if ((SaveState & Sop.Mru.SaveTypes.CollectionSave) !=
                    Sop.Mru.SaveTypes.CollectionSave)
                {
                    if (itm.IndexToMruList != null)
                    {
                        mruManager.RemoveNode(itm.IndexToMruList);
                        mruManager.AddInHead(itm.IndexToMruList, true);
                    }
                }
                return itm.Value;
            }
            set
            {
                if ((SaveState & Sop.Mru.SaveTypes.CollectionSave) ==
                    Sop.Mru.SaveTypes.CollectionSave)
                    return;
                MruItem<TKey, TValue> itm = this.CacheCollection[key];
                if (itm != null)
                {
                    itm.Transaction = ((OnDisk.Algorithm.Collection.ICollectionOnDisk) this.mruManager.Collection).Transaction;
                    mruManager.RemoveNode(itm.IndexToMruList);
                    itm.Value = value;
                    mruManager.AddInHead(itm.IndexToMruList, true);
                }
                else
                {
                    Transaction = ((OnDisk.Algorithm.Collection.ICollectionOnDisk) this.mruManager.Collection).Transaction;
                    this.Add(key, value);
                }
            }
        }

        public MruItem<TKey, TValue> PeekInTail()
        {
            return mruManager.PeekInTail();
        }

        public void AddInTail(TKey key, TValue value)
        {
            var itm = CacheCollection[key];
            if (itm == null)
                itm = new MruItem<TKey, TValue>(key, value, Transaction);
            else
            {
                mruManager.RemoveNode(itm.IndexToMruList);
                itm.Transaction = Transaction;
                itm.Key = key;
                itm.Value = value;
            }
            itm.IndexToMruList = mruManager.AddInTail(itm);
            CacheCollection[key] = itm;
        }

        public MruItem<TKey, TValue> RemoveInTail()
        {
            return RemoveInTail(false);
        }

        internal MruItem<TKey, TValue> RemoveInTail(bool moveToRemoveList)
        {
            if (Count > 0 &&
                (SaveState & Sop.Mru.SaveTypes.CollectionSave) !=
                Sop.Mru.SaveTypes.CollectionSave)
            {
                MruItem<TKey, TValue> itm = mruManager.RemoveInTail(moveToRemoveList);
                if (itm != null && (typeof(TKey).IsValueType || itm.Key != null))
                {
                    CacheCollection.Remove(itm.Key);
                    return itm;
                }
            }
            return null;
        }

        /// <summary>
        /// Remove Objects from cache and persist them to target store
        /// </summary>
        /// <param name="keys"></param>
        public void Remove(ICollection<TKey> keys)
        {
            if ((SaveState & SaveTypes.CollectionSave) == SaveTypes.CollectionSave)
                return;
            foreach (TKey key in keys)
                Remove(key);
        }

        public void Remove(Transaction.ITransactionLogger transaction)
        {
            if (Count == 0)
                return;
            var keys = new List<TKey>();
            foreach (MruItem<TKey, TValue> itm in CacheCollection.Values)
            {
                if (itm.Transaction == transaction)
                    keys.Add(itm.Key);
            }
            foreach (TKey o in keys)
            {
                MruItem<TKey, TValue> m = CacheCollection[o];
                if (m != null && m.IndexToMruList != null)
                    mruManager.RemoveNode(m.IndexToMruList);
                CacheCollection.Remove(o);
            }
        }

        /// <summary>
        /// Removes entry with key.
        /// </summary>
        /// <param name="key">key of entry to delete from collection</param>
        public virtual object Remove(TKey key)
        {
            MruItem<TKey, TValue> itm = CacheCollection[key];
            if (itm != null)
            {
                if ((SaveState & Sop.Mru.SaveTypes.CollectionSave) !=
                    Sop.Mru.SaveTypes.CollectionSave)
                {
                    if (itm.IndexToMruList != null)
                        mruManager.RemoveNode(itm.IndexToMruList);
                    CacheCollection.Remove(key);
                }
            }
            return itm;
        }

        /// <summary>
        /// Returns true if Key is in MRU, otherwise false
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool Contains(TKey key)
        {
            return this.CacheCollection.ContainsKey(key);
        }

        /// <summary>
        /// Set to null all collected items and their internal buffers
        /// </summary>
        public virtual void Clear()
        {
            if ((SaveState & Sop.Mru.SaveTypes.CollectionSave) ==
                Sop.Mru.SaveTypes.CollectionSave)
                return;
            this.CacheCollection.Clear();
            this.mruManager.Clear();
        }

        /// <summary>
        /// MinCapacity of MRU cache
        /// </summary>
        public int MinCapacity
        {
            get { return mruManager.MinCapacity; }
            set { mruManager.MinCapacity = value; }
        }

        /// <summary>
        /// MaxCapacity of MRU cache
        /// </summary>
        public int MaxCapacity
        {
            get { return this.mruManager.MaxCapacity; }
            set { this.mruManager.MaxCapacity = value; }
        }

        /// <summary>
        /// Returns the Objects cached
        /// </summary>
        public System.Collections.Generic.ICollection<TValue> Values
        {
            get
            {
                if (CacheCollection.MoveFirst())
                {
                    var r = new TValue[Count];
                    int i = 0;
                    do
                    {
                        r[i++] = CacheCollection.CurrentValue.Value;
                    } while (CacheCollection.MoveNext());
                    return r;
                }
                return null;
            }
        }

        /// <summary>
        /// Returns the Objects cached
        /// </summary>
        public System.Collections.Generic.ICollection<MruItem<TKey, TValue>> MruItems
        {
            get { return CacheCollection.Values; }
        }

        /// <summary>
        /// Returns the Keys of Objects cached
        /// </summary>
        public virtual System.Collections.Generic.ICollection<TKey> Keys
        {
            get { return CacheCollection.Keys; }
        }

        protected internal Collections.Generic.ISortedDictionary<TKey, MruItem<TKey, TValue>> CacheCollection;
        internal InternalMruManager<TKey, TValue> mruManager;
    }
}