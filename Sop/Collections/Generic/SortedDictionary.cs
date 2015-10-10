
using System.Collections.Generic;

namespace Sop.Collections.Generic
{
    using System;
    using System.Collections;
    using System.ComponentModel;
    using Synchronization;

    /// <summary>
    /// In-Memory, duplicates allowed Sorted Dictionary.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
#if !DEVICE
    [Serializable]
#endif
    public class SortedDictionary<TKey, TValue> : ISortedDictionary<TKey, TValue>
    {
        #region Enumerator classes
        internal class BTreeEnumeratorValue : BTreeEnumerator<TValue>
        {
            /// <summary>
            /// Constructor. Pass the B-Tree instance you want to enumerate its items/elements on.
            /// </summary>
            /// <param name="bTree">BTree instance items will be enumerated</param>
            public BTreeEnumeratorValue(ISortedDictionary<TKey, TValue> bTree) : base(bTree)
            {
            }

            public override TValue Current
            {
                get
                {
                    if (!bWasReset)
                        return BTree.CurrentValue;
                    throw new InvalidOperationException("BTreeEnumeratorValue encountered an invalid Reset status.");
                }
            }
        }

        internal class BTreeEnumeratorKey : BTreeEnumerator<TKey>
        {
            /// <summary>
            /// Constructor. Pass the B-Tree instance you want to enumerate its items/elements on.
            /// </summary>
            /// <param name="bTree">BTree instance items will be enumerated</param>
            public BTreeEnumeratorKey(ISortedDictionary<TKey, TValue> bTree) : base(bTree)
            {
            }

            public override TKey Current
            {
                get
                {
                    if (!bWasReset)
                        return BTree.CurrentKey;
                    throw new InvalidOperationException("BTreeEnumeratorValue encountered an invalid Reset status.");
                }
            }
        }

        internal class BTreeEnumeratorDefault : BTreeEnumerator<System.Collections.Generic.KeyValuePair<TKey, TValue>>
        {
            /// <summary>
            /// Constructor. Pass the B-Tree instance you want to enumerate its items/elements on.
            /// </summary>
            /// <param name="bTree">BTree instance items will be enumerated</param>
            public BTreeEnumeratorDefault(ISortedDictionary<TKey, TValue> bTree) : base(bTree)
            {
            }

            public override KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    if (!bWasReset)
                    {
                        return BTree.CurrentEntry == null ? new KeyValuePair<TKey, TValue>() : BTree.CurrentEntry.Value;
                    }
                    throw new InvalidOperationException("BTreeEnumeratorValue encountered an invalid Reset status.");
                }
            }
        }
        /// <summary>
        /// The B-Tree enumerator
        /// </summary>
        internal abstract class BTreeEnumerator<T> : System.Collections.Generic.IEnumerator<T>
        {
            /// <summary>
            /// Constructor. Pass the B-Tree instance you want to enumerate its items/elements on.
            /// </summary>
            /// <param name="bTree">BTree instance items will be enumerated</param>
            public BTreeEnumerator(ISortedDictionary<TKey, TValue> bTree)
            {
                this.BTree = bTree;
                this.Reset();
            }

            /// <summary>
            /// Returns Current record
            /// </summary>
            /// <exception cref="InvalidOperationException">Throws InvalidOperationException exception if Reset was called without calling MoveNext</exception>
            public abstract T Current { get; }

            public void Dispose()
            {
                BTree = null;
            }

            object IEnumerator.Current
            {
                get { return Current; }
            }

            /// <summary>
            /// Make the next record current
            /// </summary>
            /// <returns>Returns true if successul, false otherwise</returns>
            public bool MoveNext()
            {
                if (!bWasReset)
                    return BTree.MoveNext();
                if (BTree.Count == 0)
                    return false;
                bWasReset = false;
                return true;
            }

            /// <summary>
            /// Reset enumerator. You will need to call MoveNext to get to first record.
            /// </summary>
            public void Reset()
            {
                if (BTree.Count > 0)
                    BTree.MoveFirst();
                bWasReset = true;
            }

            internal ISortedDictionary<TKey, TValue> BTree;
            protected bool bWasReset;
        }
        #endregion

        /// <summary>
        /// Constructor to use if you want to provide your own Comparer object that defines
        /// how your records will be sorted/arranged
        /// </summary>
        /// <param name="comparer">IComparer implementation that defines how records will be sorted</param>
        public SortedDictionary(System.Collections.Generic.IComparer<TKey> comparer)
        {
            Btree = new BTree.BTreeAlgorithm<TKey, TValue>(comparer);
        }

        /// <summary>
        /// Constructor to use if you want to provide the number of slots per node of the tree
        /// </summary>
        /// <param name="slotLen">number of slots per node</param>
        public SortedDictionary(byte slotLen)
        {
            Btree = new Sop.Collections.Generic.BTree.BTreeAlgorithm<TKey, TValue>(slotLen);
        }

        /// <summary>
        /// Constructor to use if you want to use default number of slots per node (12 slots).
        /// </summary>
        public SortedDictionary()
        {
            Btree = new Sop.Collections.Generic.BTree.BTreeAlgorithm<TKey, TValue>();
        }

        /// <summary>
        /// Constructor to use if you want to provide number of slots per node and your comparer object
        /// </summary>
        /// <param name="slotLen">Number of slots per node</param>
        /// <param name="comparer">compare object defining how records will be sorted</param>
        public SortedDictionary(byte slotLen, System.Collections.Generic.IComparer<TKey> comparer)
        {
            Btree = new Sop.Collections.Generic.BTree.BTreeAlgorithm<TKey, TValue>(slotLen, comparer);
        }

        /// <summary>
        /// Copy all items from source onto this instance.
        /// NOTE: it is assumed that this instance's Comparer is compatible with source's.
        /// </summary>
        /// <param name="source"></param>
        public void Copy(ISortedDictionary<TKey, TValue> source)
        {
            Btree.Copy(((SortedDictionary<TKey, TValue>)source).Btree);
        }

        /// <summary>
        /// Returns current sort order. Setting to a different sort order will 
        /// reset BTree. First item according to sort order will be current item.
        /// </summary>
        public SortOrderType SortOrder
        {
            get { return _currentSortOrder; }
            set
            {
                _currentSortOrder = value;
                if (Btree != null && Btree.Count > 0)
                    MoveFirst();
            }
        }

        public ISynchronizer Locker
        {
            get
            {
                if (Btree == null) return null;
                return Btree.Locker;
            }
        }

        public KeyValuePair<TKey, TValue>? CurrentEntry
        {
            get {
                return Btree.CurrentEntry == null
                           ? (KeyValuePair<TKey, TValue>?) null
                           : new KeyValuePair<TKey, TValue>(CurrentKey, CurrentValue);
            }
        }

        public TKey CurrentKey
        {
            get
            {
                if (Btree.CurrentEntry != null)
                    return Btree.CurrentEntry.Key;
                return default(TKey);
            }
        }

        public TValue CurrentValue
        {
            get
            {
                if (Btree.CurrentEntry != null)
                    return Btree.CurrentEntry.Value;
                return default(TValue);
            }
            set
            {
                if (Btree.CurrentEntry != null)
                    Btree.CurrentEntry.Value = value;
                else
                    throw new InvalidOperationException("CurrentEntry is null.");
            }
        }

        public int Count
        {
            get { return Btree.Count; }
        }

        public bool EndOfTree()
        {
            return Btree.CurrentEntry == null;
        }

        public bool MoveNext()
        {
            return this.SortOrder ==
                   SortOrderType.Ascending ? Btree.MoveNext() : Btree.MovePrevious();
        }

        public bool MovePrevious()
        {
            return this.SortOrder ==
                   SortOrderType.Ascending ? Btree.MovePrevious() : Btree.MoveNext();
        }

        public bool MoveFirst()
        {
            return this.SortOrder ==
                   SortOrderType.Ascending ? Btree.MoveFirst() : Btree.MoveLast();
        }

        public bool MoveLast()
        {
            return this.SortOrder ==
                   SortOrderType.Ascending ? Btree.MoveLast() : MoveFirst();
        }

        public bool Search(TKey key)
        {
            return Btree.Search(key);
        }
        public bool Search(TKey key, bool gotoFirstInstance)
        {
            return Btree.Search(key, gotoFirstInstance);
        }

        public void Clear()
        {
            Btree.Clear();
        }

        public object Clone()
        {
            return Btree.Clone();
        }

        /// <summary>
        /// Add adds an entry with the provided key and value into the BTree.
        /// Duplicate keys are allowed in BTree unlike in a Dictionary/HashTable
        /// where key is required to be unique.
        /// </summary>
        /// <param name="key">key of item you want to add to the collection</param>
        /// <param name="value">item you want to add to the collection</param>
        public virtual void Add(TKey key, TValue value)
        {
            Btree.Add(key, value);
        }

        public bool ContainsKey(TKey key)
        {
            return Search(key);
        }

        /// <summary>
        /// Contains determines whether this collection contains an entry with the specified key.
        /// </summary>
        /// <param name="item"> </param>
        /// <returns></returns>
        public bool Contains(System.Collections.Generic.KeyValuePair<TKey, TValue> item)
        {
            return Search(item.Key);
        }

        private System.Collections.Generic.ICollection<TKey> _keys;

        public System.Collections.Generic.ICollection<TKey> Keys
        {
            get
            {
                if (_keys == null)
                    _keys = new BTree.BTreeKeys<TKey, TValue>(this);
                return _keys;
            }
        }

        bool IDictionary<TKey, TValue>.Remove(TKey key)
        {
            return Btree.Remove(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            if (Search(key))
            {
                value = CurrentValue;
                return true;
            }
            value = default(TValue);
            return false;
        }

        private System.Collections.Generic.ICollection<TValue> _values;

        public System.Collections.Generic.ICollection<TValue> Values
        {
            get
            {
                if (_values == null)
                    _values = new BTree.BTreeValues<TKey, TValue>(this);
                return _values;
            }
        }

        /// <summary>
        /// BTree indexer. Given a key, will return its value.
        /// If key is not found, will add a new entry having passed 
        /// params key and value.
        /// </summary>
        public virtual TValue this[TKey key]
        {
            get
            {
                if (Count > 0 && Btree.CurrentEntry != null &&
                    Comparer != null && Comparer.Compare(CurrentKey, key) == 0)
                    return CurrentValue;
                return Search(key) ? CurrentValue : default(TValue);
            }
            set
            {
                if (Count > 0 &&
                    (Btree.CurrentEntry != null &&
                     Comparer != null && Comparer.Compare(this.CurrentKey, key) == 0) ||
                    Search(key))
                    CurrentValue = value;
                else // if not found, add new entry/record. 
                    // NOTE: this is .net compliance feature
                    Add(key, value);
            }
        }

        public System.Collections.Generic.IComparer<TKey> Comparer
        {
            get { return Btree.Comparer; }
            set { Btree.Comparer = value; }
        }

        public void Add(System.Collections.Generic.KeyValuePair<TKey, TValue> item)
        {
            Btree.Add(item.Key, item.Value);
        }

        public void CopyTo(
            System.Collections.Generic.KeyValuePair<TKey, TValue>[] array,
            int arrayIndex)
        {
            if (arrayIndex < 0 || arrayIndex >= array.Length)
                throw new ArgumentOutOfRangeException("arrayIndex");
            if (Count > array.Length - arrayIndex)
                throw new InvalidOperationException("BTreeEnumeratorValue encountered an invalid Reset status.");
            if (!MoveFirst()) return;
            do
            {
                array[arrayIndex++] = new System.Collections.Generic.KeyValuePair<TKey, TValue>(CurrentKey,
                                                                                                CurrentValue);
            } while (MoveNext());
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(System.Collections.Generic.KeyValuePair<TKey, TValue> item)
        {
            return Btree.Remove(item.Key);
        }

        /// <summary>
        /// Removes entry with key.
        /// </summary>
        /// <param name="key">key of entry to delete from collection</param>
        public void Remove(TKey key)
        {
            Btree.Remove(key);
        }

        /// <summary>
        /// Delete currently selected entry of BTree
        /// </summary>
        public void Remove()
        {
            Btree.Remove();
        }

        private System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<TKey, TValue>> _enumerator;

        public System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            if (_enumerator == null || ((BTreeEnumeratorDefault) _enumerator).BTree == null)
            {
                _enumerator = new BTreeEnumeratorDefault(this);
            }
            return _enumerator;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        private SortOrderType _currentSortOrder = SortOrderType.Ascending;
#if !DEVICE
        //private System.Runtime.Serialization.SerializationInfo SerializationInfo;
#endif
        internal BTree.BTreeAlgorithm<TKey, TValue> Btree;
    }
}