using System.Collections.Generic;
using Sop.Persistence;
using ICollection = System.Collections.ICollection;

namespace Sop.Mru
{
    using Collections.Generic;
    using Synchronization;

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
            Locker.Invoke(() =>
            {
                if (realMruManager == null) return;
                realMruManager.Dispose();
                realMruManager = null;
            });
        }

        public object this[object key]
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return realMruManager[key];
                });
            }
            set
            {
                Locker.Invoke(() =>
                {
                    realMruManager[key] = value;
                });
            }
        }

        public int Count
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return realMruManager.Count;
                });
            }
        }

        public bool GeneratePruneEvent
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return realMruManager.GeneratePruneEvent;
                });
            }

            set
            {
                Locker.Invoke(() =>
                {
                    realMruManager.GeneratePruneEvent = value;
                });
            }
        }

        public bool IsDirty
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return realMruManager.IsDirty;
                });
            }
        }

        public System.Collections.Generic.ICollection<object> Keys
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return realMruManager.Keys;
                });
            }
        }

        public int MaxCapacity
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return realMruManager.MaxCapacity;
                });
            }

            set
            {
                Locker.Invoke(() =>
                {
                    realMruManager.MaxCapacity = value;
                });
            }
        }

        public int MinCapacity
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return realMruManager.MinCapacity;
                });
            }

            set
            {
                Locker.Invoke(() =>
                {
                    realMruManager.MinCapacity = value;
                });
            }
        }

        public SortedDictionary<object, object> RemovedObjects
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return realMruManager.RemovedObjects;
                });
            }
        }

        public SaveTypes SaveState
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return realMruManager.SaveState;
                });
            }

            set
            {
                Locker.Invoke(() =>
                {
                    realMruManager.SaveState = value;
                });
            }
        }

        public System.Collections.Generic.ICollection<object> Values
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return realMruManager.Values;
                });
            }
        }

        public void Add(object key, object value)
        {
            Locker.Invoke(() =>
            {
                realMruManager.Add(key, value);
            });
        }

        public void Clear()
        {
            Locker.Invoke(() =>
            {
                realMruManager.Clear();
            });
        }

        public bool Contains(object key)
        {
            return Locker.Invoke(() =>
            {
                return realMruManager.Contains(key);
            });
        }

        public void Flush()
        {
            Locker.Invoke(() =>
            {
                realMruManager.Flush();
            });
        }

        public IMruClient GetParent()
        {
            return Locker.Invoke(() =>
            {
                return realMruManager.GetParent();
            });
        }

        public IInternalPersistent GetRecycledObject()
        {
            return Locker.Invoke(() =>
            {
                return realMruManager.GetRecycledObject();
            });
        }

        public void Recycle(IInternalPersistent recycledObject)
        {
            Locker.Invoke(() =>
            {
                realMruManager.Recycle(recycledObject);
            });
        }

        public object Remove(object key)
        {
            return Locker.Invoke(() =>
            {
                return realMruManager.Remove(key);
            });
        }

        public object Remove(object key, bool removeFromCache)
        {
            return Locker.Invoke(() =>
            {
                return realMruManager.Remove(key, removeFromCache);
            });
        }

        public void Remove(ICollection keys, bool removeFromCache)
        {
            Locker.Invoke(() =>
            {
                realMruManager.Remove(keys, removeFromCache);
            });
        }

        public MruItem RemoveInTail(bool moveToRemoveList)
        {
            return Locker.Invoke(() =>
            {
                return realMruManager.RemoveInTail(moveToRemoveList);
            });
        }

        public void SetDataStores(IMruClient parent, object dataDriver)
        {
            Locker.Invoke(() =>
            {
                realMruManager.SetDataStores(parent, dataDriver);
            });
        }

        private MruManager realMruManager;
        private readonly ISynchronizer Locker = new Synchronizer();
    }
}
