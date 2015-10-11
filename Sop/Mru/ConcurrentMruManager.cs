using System.Collections.Generic;
using Sop.Persistence;
using ICollection = System.Collections.ICollection;

namespace Sop.Mru
{
    using Collections.Generic;

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
    internal class ConcurrentMruManager : IMruManager
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public ConcurrentMruManager()
        {
            realMruManager = new MruManager();
        }

        public ConcurrentMruManager(int minCapacity, int maxCapacity) : this(minCapacity, maxCapacity, null) { }

        /// <summary>
        /// Constructor expecting MinCapacity & MaxCapacity
        /// </summary>
        /// <param name="minCapacity"></param>
        /// <param name="maxCapacity"></param>
        /// <param name="comparer"> </param>
        public ConcurrentMruManager(int minCapacity, int maxCapacity, IComparer<object> comparer)
        {
            realMruManager = new MruManager(minCapacity, maxCapacity, comparer);
        }

        public void Dispose()
        {
            if (realMruManager == null) return;
            lock (Locker)
            {
                if (realMruManager == null) return;
                realMruManager.Dispose();
                realMruManager = null;
            }
        }

        public object this[object key]
        {
            get
            {
                lock(Locker)
                {
                    return realMruManager[key];
                }
            }
            set
            {
                lock(Locker)
                {
                    realMruManager[key] = value;
                }
            }
        }

        public int Count
        {
            get
            {
                lock(Locker)
                {
                    return realMruManager.Count;
                }
            }
        }

        public bool GeneratePruneEvent
        {
            get
            {
                lock(Locker)
                {
                    return realMruManager.GeneratePruneEvent;
                }
            }

            set
            {
                lock(Locker)
                {
                    realMruManager.GeneratePruneEvent = value;
                }
            }
        }

        public bool IsDirty
        {
            get
            {
                lock(Locker)
                {
                    return realMruManager.IsDirty;
                }
            }
        }

        public System.Collections.Generic.ICollection<object> Keys
        {
            get
            {
                lock (Locker)
                {
                    return realMruManager.Keys;
                }
            }
        }

        public int MaxCapacity
        {
            get
            {
                lock (Locker)
                {
                    return realMruManager.MaxCapacity;
                }
            }

            set
            {
                lock (Locker)
                {
                    realMruManager.MaxCapacity = value;
                }
            }
        }

        public int MinCapacity
        {
            get
            {
                lock (Locker)
                {
                    return realMruManager.MinCapacity;
                }
            }

            set
            {
                lock (Locker)
                {
                    realMruManager.MinCapacity = value;
                }
            }
        }

        public SortedDictionary<object, object> RemovedObjects
        {
            get
            {
                lock (Locker)
                {
                    return realMruManager.RemovedObjects;
                }
            }
        }

        public SaveTypes SaveState
        {
            get
            {
                lock (Locker)
                {
                    return realMruManager.SaveState;
                }
            }

            set
            {
                lock (Locker)
                {
                    realMruManager.SaveState = value;
                }
            }
        }

        public System.Collections.Generic.ICollection<object> Values
        {
            get
            {
                lock (Locker)
                {
                    return realMruManager.Values;
                }
            }
        }

        public void Add(object key, object value)
        {
            lock (Locker)
            {
                realMruManager.Add(key, value);
            }
        }

        public void Clear()
        {
            lock (Locker)
            {
                realMruManager.Clear();
            }
        }

        public bool Contains(object key)
        {
            lock (Locker)
            {
                return realMruManager.Contains(key);
            }
        }

        public void Flush()
        {
            lock (Locker)
            {
                realMruManager.Flush();
            }
        }

        public IMruClient GetParent()
        {
            lock (Locker)
            {
                return realMruManager.GetParent();
            }
        }

        public IInternalPersistent GetRecycledObject()
        {
            lock (Locker)
            {
                return realMruManager.GetRecycledObject();
            }
        }

        public void Recycle(IInternalPersistent recycledObject)
        {
            lock (Locker)
            {
                realMruManager.Recycle(recycledObject);
            }
        }

        public object Remove(object key)
        {
            lock (Locker)
            {
                return realMruManager.Remove(key);
            }
        }

        public object Remove(object key, bool removeFromCache)
        {
            lock (Locker)
            {
                return realMruManager.Remove(key, removeFromCache);
            }
        }

        public void Remove(ICollection keys, bool removeFromCache)
        {
            lock (Locker)
            {
                realMruManager.Remove(keys, removeFromCache);
            }
        }

        public MruItem RemoveInTail(bool moveToRemoveList)
        {
            lock (Locker)
            {
                return realMruManager.RemoveInTail(moveToRemoveList);
            }
        }

        public void SetDataStores(IMruClient parent, object dataDriver)
        {
            lock (Locker)
            {
                realMruManager.SetDataStores(parent, dataDriver);
            }
        }

        private MruManager realMruManager;
        private object Locker = new object();
    }
}
