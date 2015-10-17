using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Sop.OnDisk;
using Sop.Synchronization;

namespace Sop.Mru.Generic
{
    /// <summary>
    /// Thread Safe MRU Manager.
    /// </summary>
    public class ConcurrentMruManager<TKey, TValue> : IMruManager<TKey, TValue>
    {
        public ConcurrentMruManager(int mruMinCapacity, int mruMaxCapacity) : this(mruMinCapacity, mruMaxCapacity, null) { }

        public ConcurrentMruManager(int mruMinCapacity, int mruMaxCapacity, IComparer<TKey> comparer)
        {
            MruManager = new MruManager<TKey, TValue>(mruMinCapacity, mruMaxCapacity, comparer);
        }
        public void Add(TKey key, TValue value)
        {
            Locker.Invoke(() =>
            {
                MruManager.Add(key, value);
            });
        }

        public void Clear()
        {
            Locker.Invoke(() =>
            {
                MruManager.Clear();
            });
        }

        public bool Contains(TKey key)
        {
            return Locker.Invoke(() =>
            {
                return MruManager.Contains(key);
            });
        }

        public int Count
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return MruManager.Count;
                });
            }
        }

        public int MaxCapacity
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return MruManager.MaxCapacity;
                });
            }
            set
            {
                Locker.Invoke(() =>
                {
                    MruManager.MaxCapacity = value;
                });
            }
        }

        public int MinCapacity
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return MruManager.MinCapacity;
                });
            }
            set
            {
                Locker.Invoke(() =>
                {
                    MruManager.MinCapacity = value;
                });
            }
        }
        public object Remove(TKey key)
        {
            return Locker.Invoke(() =>
            {
                return MruManager.Remove(key);
            });
        }

        public void Remove(ICollection<TKey> keys)
        {
            Locker.Invoke(() =>
            {
                MruManager.Remove(keys);
            });
        }

        public void Dispose()
        {
            Locker.Invoke(() =>
            {
                MruManager.Dispose();
            });
        }

        public void Flush()
        {
            Locker.Invoke(() =>
            {
                MruManager.Flush();
            });
        }

        /// <summary>
        /// Binds the MRU Client to the DataDriver. DataDriver is used
        /// for persistence of cached Objects to target store
        /// </summary>
        /// <param name="parent"></param>
        public void SetDataStores(IMruClient parent)
        {
            Locker.Invoke(() =>
            {
                MruManager.SetDataStores(parent);
            });
        }

        /// <summary>
        /// Peek in tail.
        /// </summary>
        /// <returns></returns>
        public MruItem<TKey, TValue> PeekInTail()
        {
            return Locker.Invoke(() =>
            {
                return MruManager.PeekInTail();
            });
        }

        public bool IsFull
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return MruManager.IsFull;
                });
            }
        }

        /// <summary>
        /// Remove in tail.
        /// </summary>
        /// <returns></returns>
        public MruItem<TKey, TValue> RemoveInTail()
        {
            return Locker.Invoke(() =>
            {
                return MruManager.RemoveInTail();
            });
        }

        public TValue this[TKey key]
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return MruManager[key];
                });
            }
            set
            {
                Locker.Invoke(() =>
                {
                    MruManager[key] = value;
                });
            }
        }

        public ICollection<TKey> Keys
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return MruManager.Keys;
                });
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
                return Locker.Invoke(() =>
                {
                    return MruManager.Values;
                });
            }
        }

        public SaveTypes SaveState
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return MruManager.SaveState;
                });
            }
            set
            {
                Locker.Invoke(() =>
                {
                    MruManager.SaveState = value;
                });
            }
        }

        public bool GeneratePruneEvent
        {
            get
            {
                return Locker.Invoke(() =>
                {
                    return MruManager.GeneratePruneEvent;
                });
            }
            set
            {
                Locker.Invoke(() =>
                {
                    MruManager.GeneratePruneEvent = value;
                });
            }
        }

        internal readonly ISynchronizer Locker = new SynchronizerSingleReaderWriterBase();
        internal readonly MruManager<TKey, TValue> MruManager;


    }
}
