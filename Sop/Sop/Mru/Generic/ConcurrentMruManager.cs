using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Sop.OnDisk;

namespace Sop.Mru.Generic
{
    /// <summary>
    /// Thread Safe MRU Manager.
    /// </summary>
    public class ConcurrentMruManager<TKey, TValue> : IMruManager<TKey, TValue>
    {
        public ConcurrentMruManager(int mruMinCapacity, int mruMaxCapacity) : this(mruMinCapacity, mruMaxCapacity, null){}

        public ConcurrentMruManager(int mruMinCapacity, int mruMaxCapacity, IComparer<TKey> comparer)
        {
            MruManager = new MruManager<TKey, TValue>(mruMinCapacity, mruMaxCapacity, comparer);
        }
        public void Add(TKey key, TValue value)
        {
            lock (Locker)
            {
                MruManager.Add(key, value);
            }
        }

        public void Clear()
        {
            lock(Locker)
            {
                MruManager.Clear();
            }
        }

        public bool Contains(TKey key)
        {
            lock(Locker)
            {
                return MruManager.Contains(key);
            }
        }

        public int Count
        {
            get 
            {
                lock (Locker)
                {
                    return MruManager.Count;
                }
            }
        }

        public int MaxCapacity
        {
            get
            {
                lock (Locker)
                {
                    return MruManager.MaxCapacity;
                }
            }
            set
            {
                lock (Locker)
                {
                    MruManager.MaxCapacity = value;
                }
            }
        }

        public int MinCapacity
        {
            get
            {
                lock (Locker)
                {
                    return MruManager.MinCapacity;
                }
            }
            set
            {
                lock (Locker)
                {
                    MruManager.MinCapacity = value;
                }
            }
        }
        public object Remove(TKey key)
        {
            lock(Locker)
            {
                return MruManager.Remove(key);
            }
        }

        public void Remove(ICollection<TKey> keys)
        {
            lock (Locker)
            {
                MruManager.Remove(keys);
            }
        }

        public void Dispose()
        {
            lock(Locker)
            {
                MruManager.Dispose();
            }
        }

        public void Flush()
        {
            lock (Locker)
            {
                MruManager.Flush();
            }
        }

        /// <summary>
        /// Binds the MRU Client to the DataDriver. DataDriver is used
        /// for persistence of cached Objects to target store
        /// </summary>
        /// <param name="parent"></param>
        public void SetDataStores(IMruClient parent)
        {
            lock (Locker)
            {
                MruManager.SetDataStores(parent);
            }
        }

        /// <summary>
        /// Peek in tail.
        /// </summary>
        /// <returns></returns>
        public MruItem<TKey, TValue> PeekInTail()
        {
            lock(Locker)
            {
                return MruManager.PeekInTail();
            }
        }

        public bool IsFull
        {
            get
            {
                lock (Locker)
                {
                    return MruManager.IsFull;
                }
            }
        }

        /// <summary>
        /// Remove in tail.
        /// </summary>
        /// <returns></returns>
        public MruItem<TKey, TValue> RemoveInTail()
        {
            lock (Locker)
            {
                return MruManager.RemoveInTail();
            }
        }

        public TValue this[TKey key]
        {
            get
            {
                lock (Locker)
                {
                    return MruManager[key];
                }
            }
            set
            {
                lock (Locker)
                {
                    MruManager[key] = value;
                }
            }
        }

        public ICollection<TKey> Keys
        {
            get
            {
                lock (Locker)
                {
                    return MruManager.Keys;
                }
            }
        }

        /// <summary>
        /// Returns Objects in cache
        /// </summary>
        //System.Collections.Generic.ICollection<TValue> Values { get; }
        /// <summary>
        /// Returns Objects in cache
        /// </summary>
        public ICollection<MruItem<TKey, TValue>> MruItems
        {
            get { throw new NotImplementedException(); }
        }

        public ICollection<TValue> Values
        {
            get
            {
                lock (Locker)
                {
                    return MruManager.Values;
                }
            }
        }

        public SaveTypes SaveState
        {
            get
            {
                lock (Locker)
                {
                    return MruManager.SaveState;
                }
            }
            set
            {
                lock (Locker)
                {
                    MruManager.SaveState = value;
                }
            }
        }

        public bool GeneratePruneEvent
        {
            get
            {
                lock (Locker)
                {
                    return MruManager.GeneratePruneEvent;
                }
            }
            set
            {
                lock (Locker)
                {
                    MruManager.GeneratePruneEvent = value;
                }
            }
        }

        internal readonly object Locker = new object();
        internal readonly MruManager<TKey, TValue> MruManager;


    }
}
