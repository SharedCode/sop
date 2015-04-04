using System;
using System.Collections;
using System.Collections.Generic;
using Sop.Collections.BTree;
using Sop.OnDisk.DataBlock;
using Sop.Persistence;
using Sop.SpecializedDataStore;
using ICollection = System.Collections.ICollection;

namespace Sop.Mru
{
    using OnDisk;

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
    internal class MruManager : IMruManager
    {
        #region CacheManager

        ///// <summary>
        ///// Cache Manager Collection is used across different threads and thus,
        ///// members are inherently thread safe.
        ///// </summary>
        //internal class CacheManagerCollection
        //{
        //    /// <summary>
        //    /// CacheManagers keep track of all MRU cache managers in the SOP system
        //    /// </summary>
        //    internal Dictionary<IMruManager, IMruManager> Managers = new Dictionary<IMruManager, IMruManager>();
        //    public void Add(IMruManager MruManager)
        //    {
        //        lock (Managers)
        //        {
        //            if (!Managers.ContainsKey(MruManager ))
        //                Managers.Add(MruManager, null);
        //        }
        //    }
        //    public void Remove(IMruManager MruManager)
        //    {
        //        lock (Managers)
        //        {
        //            Managers.Remove(MruManager);
        //        }
        //    }
        //    public ICollection<IMruManager>  Get()
        //    {
        //        lock (Managers)
        //        {
        //            return Managers.Keys;
        //        }
        //    }
        //    public IEnumerator<IMruManager> GetEnumerator()
        //    {
        //        lock (Managers)
        //        {
        //            return Managers.Keys.GetEnumerator();
        //        }
        //    }
        //}

        #endregion

        /// <summary>
        /// Default Constructor
        /// </summary>
        public MruManager()
        {
            CacheCollection = new Collections.Generic.SortedDictionary<object, object>();
            //CacheManagers.Add(this);
        }

        public MruManager(int minCapacity, int maxCapacity) : this(minCapacity, maxCapacity, null)
        {
        }

        /// <summary>
        /// Constructor expecting MinCapacity & MaxCapacity
        /// </summary>
        /// <param name="minCapacity"></param>
        /// <param name="maxCapacity"></param>
        /// <param name="comparer"> </param>
        public MruManager(int minCapacity, int maxCapacity, IComparer<object> comparer)
        {
            mruManager = new InternalMruManager(minCapacity, maxCapacity);
            CacheCollection = new Collections.Generic.SortedDictionary<object, object>(comparer);
            //CacheManagers.Add(this);

            //RecycledObjects = new List<IInternalPersistent>(maxCapacity);
        }

        internal bool? AutoDisposeItem { get; set; }
        public void Dispose()
        {
            foreach(var entry in CacheCollection)
            {
                DisposeItem(entry.Key);
                DisposeItem(((MruItem) entry.Value).Value);
            }
            Clear();
        }
        private void DisposeItem(object item)
        {
            if (item is IDisposable)
            {
                if (AutoDisposeItem  != null &&
                    item is SpecializedDataStore.SpecializedStoreBase)
                {
                    ((SpecializedDataStore.SpecializedStoreBase)item).Collection.AutoDisposeItem = AutoDisposeItem.Value;
                }
                ((IDisposable) item).Dispose();
                if (item is SpecializedDataStore.SpecializedStoreBase)
                {
                    ((SpecializedDataStore.SpecializedStoreBase) item).Collection = null;
                }
            }
        }

        /// <summary>
        /// true will use cache entry recycling for more efficient memory management.
        /// otherwise will not
        /// </summary>
        public bool RecycleEnabled
        {
            get
            {
                return RecycledObjects != null;
            }
        }

        /// <summary>
        /// Contains the objects for recycling
        /// </summary>
        public List<IInternalPersistent> RecycledObjects;

        /// <summary>
        /// Add Objects to the RecycledObjects collection
        /// </summary>
        /// <param name="objects"></param>
        public void Recycle(System.Collections.Generic.ICollection<object> objects)
        {
            if (!RecycleEnabled || RecycledObjects.Count >= RecycledObjects.Capacity) return;
            foreach (var o in objects)
            {
                if (RecycledObjects.Count >= RecycledObjects.Capacity) break;
                if (o is IInternalPersistent)
                    Recycle((IInternalPersistent)o);
            }
        }

        /// <summary>
        /// Add Object to the RecycledObjects collection
        /// </summary>
        /// <param name="Object"></param>
        public void Recycle(IInternalPersistent Object)
        {
            if (RecycledObjects.Count >= RecycledObjects.Capacity) return;
            if (Object == null) throw new ArgumentNullException("Object");
            if (!RecycleEnabled) return;
            if (Object is Recycling.IRecyclable)
                ((Recycling.IRecyclable) Object).Initialize();
            RecycledObjects.Add(Object);
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

        public bool IsDirty
        {
            get
            {
                if (Count > 0)
                {
                    foreach (MruItem itm in CacheCollection.Values)
                    {
                        if (itm == null) continue;
                        if (!(itm.Value is IInternalPersistent))
                            return false;
                        if (((IInternalPersistent)itm.Value).IsDirty)
                            return true;
                    }
                }
                return false;
            }
        }


        /*
        /// <summary>
        /// Based on MaxMemoryUsage vs. current usage, this function will generate OnMaxCapacity event
        /// to cause associated CollectionOnDisk (DataStore) to offload objects onto Disk.
        /// </summary>
        protected void MonitorMemory()
        {
            System.Diagnostics.Process p = System.Diagnostics.Process.GetCurrentProcess();
            long MemoryUsage = p.WorkingSet64;
            IntPtr MaxMemoryUsage = p.MaxWorkingSet;
        }
		 */

        /// <summary>
        /// Save Removed Blocks to target store
        /// </summary>
        public void SaveRemovedBlocks()
        {
            mruManager.SaveRemovedBlocks();
            Recycle(RemovedObjects.Values);
            RemovedObjects.Clear();
        }

        /// <summary>
        /// Save the unpersisted MRU items
        /// </summary>
        public virtual void Flush()
        {
            if ((SaveState & SaveTypes.CollectionSave) == SaveTypes.CollectionSave)
                return;
            SaveState |= SaveTypes.CollectionSave;
            if (CacheCollection.Count > 0)
            {
                const int batchLimit = 90;
                List<IInternalPersistent> dirtyNodes = new List<IInternalPersistent>(batchLimit);
                int batchCount = 0;
                foreach (Node n in mruManager)
                {
                    MruItem itm = n.Data;
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
            SaveState ^= SaveTypes.CollectionSave;
        }

        /// <summary>
        /// Binds the DataDriver to the MRUClient(specified by Parent)
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="dataDriver"></param>
        public void SetDataStores(IMruClient parent, object dataDriver)
        {
            mruManager.DataDriver = dataDriver;
            mruManager.Collection = parent;
        }

        public IMruClient GetParent()
        {
            return mruManager.Collection;
        }
        public Collections.Generic.SortedDictionary<object, object> RemovedObjects
        {
            get { return mruManager.RemovedObjects; }
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
        public SaveTypes SaveState
        {
            get { return mruManager.SaveState; }
            set { mruManager.SaveState = value; }
        }

        /// <summary>
        /// Add Object to the cache
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public virtual void Add(object key, object value)
        {
            if (SaveState != SaveTypes.Default) return;
            MruItem itm = null;
            if (GeneratePruneEvent)
            {
                if (MaxCapacity > 0 && CacheCollection.Count >= this.MaxCapacity)
                {
                    if (mruManager.RemovedObjects.Count == 0)
                    {
                        bool ogp = GeneratePruneEvent;
                        GeneratePruneEvent = false;
                        itm = mruManager.PeekInTail();
                        // prevent pruning to occur if itm is Specialized Store and is locked by a thread...
                        if (!(itm.Value is SpecializedStoreBase &&
                              ((SpecializedStoreBase)itm.Value).Locker.IsLocked))
                        {
                            Log.Logger.Instance.Log(Log.LogLevels.Information, "MruManager.Add: pruning enter.");
                            for (int i = MaxCapacity; i > MinCapacity; i--)
                            {
                                itm = mruManager.RemoveInTail(mruManager.Collection != null);
                                CacheCollection.Remove(itm.Key);
                                if (!(itm.Value is IDisposable)) continue;
                                var specializedStoreBase = itm.Value as SpecializedStoreBase;
                                if (specializedStoreBase != null)
                                {
                                    if (specializedStoreBase.Locker.IsLocked) continue;
                                    specializedStoreBase.InvokeFromMru = true;
                                }
                                if (mruManager.Collection == null)
                                    ((IDisposable)itm.Value).Dispose();
                                if (specializedStoreBase != null)
                                    specializedStoreBase.InvokeFromMru = false;
                            }
                            Log.Logger.Instance.Log(Log.LogLevels.Information, "MruManager.Add: pruning exit.");
                            SaveRemovedBlocks();
                        }
                        GeneratePruneEvent = ogp;
                    }
                }
            }
            itm = (MruItem) Remove(key, true);
            if (itm == null)
            {
                itm = new MruItem(key, value, Transaction);
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

        // for use in VirtualStore to access MRU Item key/value pair.
        internal MruItem GetItem(object key)
        {
            // check if item is in removed objects, not likely but in case...
            //if (mruManager.RemovedObjects.Count > 0 &&
            //    mruManager.RemovedObjects.ContainsKey(key))
            //    return mruManager.RemovedObjects.CurrentValue;
            MruItem itm = (MruItem)this.CacheCollection[key];
            if (itm == null)
                return null;
            if ((SaveState & SaveTypes.CollectionSave) != SaveTypes.CollectionSave)
            {
                if (itm.IndexToMruList != null)
                {
                    mruManager.RemoveNode(itm.IndexToMruList, true);
                    mruManager.AddInHead(itm.IndexToMruList, true);
                }
            }
            return itm;
        }

        /// <summary>
        /// Given a key, will return its value.
        /// If key is not found, will add a new entry having passed 
        /// params key and value.
        /// </summary>
        public object this[object key]
        {
            get
            {
                // check if item is in removed objects, not likely but in case...
                if (mruManager.RemovedObjects.Count > 0 &&
                    mruManager.RemovedObjects.ContainsKey(key))
                    return mruManager.RemovedObjects.CurrentValue;

                MruItem itm = (MruItem) this.CacheCollection[key];
                if (itm == null)
                    return null;
                if ((SaveState & SaveTypes.CollectionSave) != SaveTypes.CollectionSave)
                {
                    if (itm.IndexToMruList != null)
                    {
                        mruManager.RemoveNode(itm.IndexToMruList, true);
                        mruManager.AddInHead(itm.IndexToMruList, true);
                    }
                }
                return itm.Value;
            }
            set
            {
                if ((SaveState & SaveTypes.CollectionSave) == SaveTypes.CollectionSave)
                    return;
                MruItem itm = (MruItem) this.CacheCollection[key];
                if (itm != null)
                {
                    itm.Transaction = ((OnDisk.Algorithm.Collection.ICollectionOnDisk) this.mruManager.Collection).Transaction;
                    mruManager.RemoveNode(itm.IndexToMruList, true);
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

        /// <summary>
        /// Remove Object with key from cache
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public object Remove(object key)
        {
            return Remove(key, false);
        }

        internal MruItem PeekInTail()
        {
            return mruManager.PeekInTail();
        }

        public void AddInTail(object key, object value)
        {
            var itm = (MruItem) CacheCollection[key];
            if (itm == null)
                itm = new MruItem(key, value, Transaction);
            else
            {
                mruManager.RemoveNode(itm.IndexToMruList, true);
                itm.Transaction = Transaction;
                itm.Key = key;
                itm.Value = value;
            }
            itm.IndexToMruList = mruManager.AddInTail(itm);
            CacheCollection[key] = itm;
        }

        public MruItem RemoveInTail(bool moveToRemoveList)
        {
            if (Count > 0 &&
                (SaveState & SaveTypes.CollectionSave) != SaveTypes.CollectionSave)
            {
                var itm = mruManager.RemoveInTail(moveToRemoveList);
                if (itm != null && itm.Key != null)
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
        /// <param name="removeFromCache"></param>
        public void Remove(ICollection keys, bool removeFromCache)
        {
            if ((SaveState & SaveTypes.CollectionSave) == SaveTypes.CollectionSave)
                return;
            foreach (object key in keys)
            {
                if (key is Sop.DataBlock)
                    Remove(((DataBlock) key).DataAddress, removeFromCache);
                else
                    Remove(key, removeFromCache);
            }
        }

        public void Remove(Transaction.ITransactionLogger transaction)
        {
            if (Count == 0)
                return;
            var keys = new List<object>();
            foreach (MruItem itm in CacheCollection.Values)
            {
                if (itm.Transaction == transaction)
                    keys.Add(itm.Key);
            }
            foreach (object o in keys)
            {
                var m = (MruItem) CacheCollection[o];
                if (m != null && m.IndexToMruList != null)
                    mruManager.RemoveNode(m.IndexToMruList, true);

                CacheCollection.Remove(o);
            }
        }

        /// <summary>
        /// Removes entry with key.
        /// </summary>
        /// <param name="key">key of entry to delete from collection</param>
        /// <param name="removeFromCache"> </param>
        public virtual object Remove(object key, bool removeFromCache)
        {
            var itm = (MruItem) CacheCollection[key];
            if (itm != null)
            {
                if ((SaveState & SaveTypes.CollectionSave) != SaveTypes.CollectionSave)
                {
                    if (itm.IndexToMruList != null)
                        mruManager.RemoveNode(itm.IndexToMruList, removeFromCache);
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
        public bool Contains(object key)
        {
            return this.CacheCollection.ContainsKey(key);
        }

        /// <summary>
        /// Set to null all collected items and their internal buffers
        /// </summary>
        public virtual void Clear()
        {
            if ((SaveState & SaveTypes.CollectionSave) == SaveTypes.CollectionSave)
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
        /// Returns all the items of this MRU cache.
        /// </summary>
        internal IEnumerator<KeyValuePair<object, object>> GetEnumerator()
        {
            return CacheCollection.GetEnumerator();
        }

        /// <summary>
        /// Returns the Objects cached
        /// </summary>
        public ICollection<object> Values
        {
            get { return CacheCollection.Values; }
        }

        /// <summary>
        /// Returns the Keys of Objects cached
        /// </summary>
        public virtual ICollection<object> Keys
        {
            get { return CacheCollection.Keys; }
        }

        protected internal Collections.Generic.SortedDictionary<object, object> CacheCollection;
        internal InternalMruManager mruManager;
    }
}