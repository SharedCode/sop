using System.Collections.Generic;
using Sop.Collections.BTree;
using Sop.Collections.Generic.BTree;

namespace Sop.Collections.Generic
{
    using System;
    using System.Collections;
    using Synchronization;

    /// <summary>
    /// In-Memory, duplicates allowed Concurrent (thread safe) Sorted Dictionary.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
#if !DEVICE
    [Serializable]
#endif
    internal class ConcurrentSortedDictionary<TKey, TValue> : ISortedDictionary<TKey, TValue>
    {
        public ConcurrentSortedDictionary(IComparer<TKey> comparer, SortOrderType sortOrder = SortOrderType.Ascending) : 
            this(BTreeAlgorithm<TKey, TValue>.DefaultSlotLength, comparer, sortOrder){}

        /// <summary>
        /// Constructor to use if you want to provide number of slots per node and your comparer object
        /// </summary>
        /// <param name="slotLen">Number of slots per node</param>
        /// <param name="comparer">compare object defining how records will be sorted</param>
        /// <param name="sortOrder"> </param>
        public ConcurrentSortedDictionary(byte slotLen = BTreeAlgorithm<TKey, TValue>.DefaultSlotLength,
            IComparer<TKey> comparer = null, SortOrderType sortOrder = SortOrderType.Ascending)
        {
            _btree = new BTreeAlgorithm<TKey, TValue>(slotLen, comparer);
            _currentItem = new BTreeAlgorithm<TKey, TValue>.TreeNode.ItemAddress();
            _sortOrder = sortOrder;
        }
        internal ConcurrentSortedDictionary(ConcurrentSortedDictionary<TKey, TValue> source)
        {
            _btree = (BTreeAlgorithm<TKey, TValue>)source.Btree.Clone();
            _currentItem = source._currentItem;
            _sortOrder = source.SortOrder;
        }
        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            if (_enumerator == null || ((SortedDictionary<TKey, TValue>.BTreeEnumeratorDefault)_enumerator).BTree == null)
            {
                Locker.Lock();
                if (_enumerator == null ||
                    ((SortedDictionary<TKey, TValue>.BTreeEnumeratorDefault) _enumerator).BTree == null)
                {
                    _enumerator = new SortedDictionary<TKey, TValue>.BTreeEnumeratorDefault(this);
                }
                Locker.Unlock();
            }
            return _enumerator;
        }
        private IEnumerator<KeyValuePair<TKey, TValue>> _enumerator;

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Adds an item to the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <param name="item">The object to add to the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        ///                 </param><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.
        ///                 </exception>
        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Locker.Lock();
            Btree.Add(item.Key, item.Value);
            Locker.Unlock();
        }

        // todo: set thread to certain ID, Store in Synchronizer the BTreeReader for a given thread.

        /// <summary>
        /// Removes all items from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only. 
        ///                 </exception>
        public void Clear()
        {
            Locker.Lock();
            Btree.Clear();
            Locker.Unlock();
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.Generic.ICollection`1"/> contains a specific value.
        /// </summary>
        /// <returns>
        /// true if <paramref name="item"/> is found in the <see cref="T:System.Collections.Generic.ICollection`1"/>; otherwise, false.
        /// </returns>
        /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        ///                 </param>
        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return Search(item.Key);
        }

        /// <summary>
        /// Copies the elements of the <see cref="T:System.Collections.Generic.ICollection`1"/> to an <see cref="T:System.Array"/>, starting at a particular <see cref="T:System.Array"/> index.
        /// </summary>
        /// <param name="array">The one-dimensional <see cref="T:System.Array"/> that is the destination of the elements copied from <see cref="T:System.Collections.Generic.ICollection`1"/>. The <see cref="T:System.Array"/> must have zero-based indexing.
        ///                 </param><param name="arrayIndex">The zero-based index in <paramref name="array"/> at which copying begins.
        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="array"/> is null.
        ///                 </exception><exception cref="T:System.ArgumentOutOfRangeException"><paramref name="arrayIndex"/> is less than 0.
        ///                 </exception><exception cref="T:System.ArgumentException"><paramref name="array"/> is multidimensional.
        ///                     -or-
        ///                 <paramref name="arrayIndex"/> is equal to or greater than the length of <paramref name="array"/>.
        ///                     -or-
        ///                     The number of elements in the source <see cref="T:System.Collections.Generic.ICollection`1"/> is greater than the available space from <paramref name="arrayIndex"/> to the end of the destination <paramref name="array"/>.
        ///                     -or-
        ///                     Type <paramref name="T"/> cannot be cast automatically to the type of the destination <paramref name="array"/>.
        ///                 </exception>
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            if (arrayIndex < 0 || arrayIndex >= array.Length)
                throw new ArgumentOutOfRangeException("arrayIndex");
            Locker.Lock();
            if (Count > array.Length - arrayIndex)
                throw new ArgumentException("BTree has more elements than target array.", "array");
            if (!MoveFirst())
            {
                Locker.Unlock();
                return;
            }
            do
            {
                array[arrayIndex++] = new KeyValuePair<TKey, TValue>(CurrentKey,
                                                                                                CurrentValue);
            } while (MoveNext());
            Locker.Unlock();
        }

        /// <summary>
        /// Removes the first occurrence of a specific object from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <returns>
        /// true if <paramref name="item"/> was successfully removed from the <see cref="T:System.Collections.Generic.ICollection`1"/>; otherwise, false. This method also returns false if <paramref name="item"/> is not found in the original <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </returns>
        /// <param name="item">The object to remove from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        ///                 </param><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.
        ///                 </exception>
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }

        /// <summary>
        /// Gets the number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <returns>
        /// The number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </returns>
        public int Count
        {
            get
            {
                Locker.Lock();
                var r = Btree.Count;
                Locker.Unlock();
                return r;
            }
        }

        /// <summary>
        /// Implement to copy items from source onto this instance.
        /// </summary>
        /// <param name="source"></param>
        public void Copy(ISortedDictionary<TKey, TValue> source)
        {
            Locker.Lock();
            Btree.Copy(((SortedDictionary<TKey, TValue>)source).Btree);
            Locker.Unlock();
        }

        /// <summary>
        /// Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.
        /// </summary>
        /// <returns>
        /// true if the <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only; otherwise, false.
        /// </returns>
        public bool IsReadOnly
        {
            get { return false; }
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.Generic.IDictionary`2"/> contains an element with the specified key.
        /// </summary>
        /// <returns>
        /// true if the <see cref="T:System.Collections.Generic.IDictionary`2"/> contains an element with the key; otherwise, false.
        /// </returns>
        /// <param name="key">The key to locate in the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.
        ///                 </exception>
        public bool ContainsKey(TKey key)
        {
            return Search(key);
        }

        /// <summary>
        /// Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </summary>
        /// <param name="key">The object to use as the key of the element to add.
        ///                 </param><param name="value">The object to use as the value of the element to add.
        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.
        ///                 </exception><exception cref="T:System.ArgumentException">An element with the same key already exists in the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        ///                 </exception><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.
        ///                 </exception>
        public void Add(TKey key, TValue value)
        {
            Locker.Lock();
            Btree.Add(key, value);
            Locker.Unlock();
        }

        /// <summary>
        /// Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </summary>
        /// <returns>
        /// true if the element is successfully removed; otherwise, false.  This method also returns false if <paramref name="key"/> was not found in the original <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </returns>
        /// <param name="key">The key of the element to remove.
        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.
        ///                 </exception><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.
        ///                 </exception>
        public bool Remove(TKey key)
        {
            Locker.Lock();
            var r = Btree.Remove(key);
            Locker.Unlock();
            return r;
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <returns>
        /// true if the object that implements <see cref="T:System.Collections.Generic.IDictionary`2"/> contains an element with the specified key; otherwise, false.
        /// </returns>
        /// <param name="key">The key whose value to get.
        ///                 </param><param name="value">When this method returns, the value associated with the specified key, if the key is found; otherwise, the default value for the type of the <paramref name="value"/> parameter. This parameter is passed uninitialized.
        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.
        ///                 </exception>
        public bool TryGetValue(TKey key, out TValue value)
        {
            Locker.Lock();
            var r = false;
            value = default(TValue);
            if (Search(key))
            {
                value = CurrentValue;
                r = true;
            }
            Locker.Unlock();
            return r;
        }

        /// <summary>
        /// Gets or sets the element with the specified key.
        /// </summary>
        /// <returns>
        /// The element with the specified key.
        /// </returns>
        /// <param name="key">The key of the element to get or set.
        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.
        ///                 </exception><exception cref="T:System.Collections.Generic.KeyNotFoundException">The property is retrieved and <paramref name="key"/> is not found.
        ///                 </exception><exception cref="T:System.NotSupportedException">The property is set and the <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.
        ///                 </exception>
        public TValue this[TKey key]
        {
            get
            {
                Locker.Lock();
                TValue r;
                if (Count > 0 && CurrentEntry != null &&
                    Comparer != null && Comparer.Compare(this.CurrentKey, key) == 0)
                    r = CurrentValue;
                else
                    r = Search(key) ? this.CurrentValue : default(TValue);
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                if (Count > 0 &&
                    (CurrentEntry != null &&
                     Comparer != null && Comparer.Compare(this.CurrentKey, key) == 0) ||
                    Search(key))
                    CurrentValue = value;
                else // if not found, add new entry/record. 
                    // NOTE: this is .net compliance feature
                    Add(key, value);
                Locker.Unlock();
            }
        }

        /// <summary>
        /// Gets an <see cref="T:System.Collections.Generic.ICollection`1"/> containing the keys of the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.Generic.ICollection`1"/> containing the keys of the object that implements <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </returns>
        public System.Collections.Generic.ICollection<TKey> Keys
        {
            get
            {
                Locker.Lock();
                if (_keys == null)
                    _keys = new BTree.BTreeKeys<TKey, TValue>(this);
                var r = _keys;
                Locker.Unlock();
                return r;
            }
        }
        private System.Collections.Generic.ICollection<TKey> _keys;

        /// <summary>
        /// Gets an <see cref="T:System.Collections.Generic.ICollection`1"/> containing the values in the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.Generic.ICollection`1"/> containing the values in the object that implements <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </returns>
        public System.Collections.Generic.ICollection<TValue> Values
        {
            get
            {
                Locker.Lock();
                if (_values == null)
                    _values = new BTreeValues<TKey, TValue>(this);
                var r =_values;
                Locker.Unlock();
                return r;
            }
        }
        private System.Collections.Generic.ICollection<TValue> _values;

        /// <summary>
        /// Creates a new object that is a copy of the current instance.
        /// </summary>
        /// <returns>
        /// A new object that is a copy of this instance.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public object Clone()
        {
            Locker.Lock();
            var r = new ConcurrentSortedDictionary<TKey, TValue>(this);
            Locker.Unlock();
            return r;
        }

        /// <summary>
        /// MoveNext makes the next entry current.
        /// </summary>
        public bool MoveNext()
        {
            Locker.Lock();
            var r = this.SortOrder ==
                   SortOrderType.Ascending ? Btree.MoveNext() : Btree.MovePrevious();
            Locker.Unlock();
            return r;
        }

        /// <summary>
        /// MovePrevious makes the previous entry current.
        /// </summary>
        public bool MovePrevious()
        {
            Locker.Lock();
            var r = this.SortOrder ==
                   SortOrderType.Ascending ? Btree.MovePrevious() : Btree.MoveNext();
            Locker.Unlock();
            return r;
        }

        /// <summary>
        /// MoveFirst makes the first entry in the Collection current.
        /// </summary>
        public bool MoveFirst()
        {
            Locker.Lock();
            bool r = this.SortOrder ==
                   SortOrderType.Ascending ? Btree.MoveFirst() : Btree.MoveLast();
            Locker.Unlock();
            return r;
        }

        /// <summary>
        /// MoveLast makes the last entry in the Collection current.
        /// </summary>
        public bool MoveLast()
        {
            Locker.Lock();
            var r = this.SortOrder ==
                   SortOrderType.Ascending ? Btree.MoveLast() : MoveFirst();
            Locker.Unlock();
            return r;
        }

        /// <summary>
        /// Search the Collection for existence of entry with a given key.
        /// </summary>
        /// <param name="key">key to search for.</param>
        /// <returns>true if found, false otherwise.</returns>
        public bool Search(TKey key)
        {
            return Search(key, false);
        }

        /// <summary>
        /// true if end of tree is reached (CurrentItem is null), otherwise false.
        /// </summary>
        /// <returns></returns>
        public bool EndOfTree()
        {
            Locker.Lock();
            var r = Btree.CurrentEntry == null;
            Locker.Unlock();
            return r;
        }

        /// <summary>
        /// Returns current sort order. Setting to a different sort order will 
        /// reset BTree. First item according to sort order will be current item.
        /// </summary>
        public SortOrderType SortOrder
        {
            get
            {
                Locker.Lock();
                var r = _sortOrder;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                _sortOrder = value;
                if (_btree.Count > 0)
                    MoveFirst();
                Locker.Unlock();
            }
        }
        private SortOrderType _sortOrder;

        /// <summary>
        /// Returns the Current entry.
        /// </summary>
        public KeyValuePair<TKey, TValue>? CurrentEntry
        {
            get
            {
                Locker.Lock();
                var o = Btree.CurrentEntry;
                var r = o == null ? null :
                           (KeyValuePair<TKey, TValue>?)new KeyValuePair<TKey, TValue>(o.Key, o.Value);
                Locker.Unlock();
                return r;
            }
        }

        /// <summary>
        /// Returns the Current entry's key.
        /// </summary>
        public TKey CurrentKey
        {
            get
            {
                Locker.Lock();
                TKey r = default(TKey);
                if (Btree.CurrentEntry != null)
                    r = Btree.CurrentEntry.Key;
                Locker.Unlock();
                return r;
            }
        }

        /// <summary>
        /// Returns the Current entry's Value.
        /// </summary>
        public TValue CurrentValue
        {
            get
            {
                TValue r = default(TValue);
                Locker.Lock();
                if (Btree.CurrentEntry != null)
                    r = Btree.CurrentEntry.Value;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                try
                {
                    if (Btree.CurrentEntry != null)
                        Btree.CurrentEntry.Value = value;
                    else
                        throw new InvalidOperationException("CurrentEntry is null.");
                }
                finally
                {
                    Locker.Unlock();
                }
            }
        }

        /// <summary>
        /// Remove an entry from Sorted Dictionary.
        /// </summary>
        public void Remove()
        {
            Locker.Lock();
            Btree.Remove();
            Locker.Unlock();
        }

        public bool Search(TKey key, bool goToFirstInstance)
        {
            Locker.Lock();
            bool r = Btree.Search(key, goToFirstInstance);
            Locker.Unlock();
            return r;
        }

        public BTreeAlgorithm<TKey, TValue>.TreeNode.ItemAddress CurrentItem
        {
            get { return _currentItem; }
        }
        private BTreeAlgorithm<TKey, TValue>.TreeNode.ItemAddress _currentItem;

        public void SetCurrentItemAddress(BTree.BTreeAlgorithm<TKey, TValue>.TreeNode itemNode, byte itemIndex)
        {
            _currentItem.Node = itemNode;
            _currentItem.NodeItemIndex = itemIndex;
        }

        public byte SlotLength
        {
            get { return _btree.SlotLength; }
        }

        public IComparer<BTreeItem<TKey, TValue>> SlotsComparer
        {
            get { return _btree.SlotsComparer; }
        }

        public IComparer<TKey> Comparer
        {
            get { return _btree.Comparer; }
            set { _btree.Comparer = value; }
        }

        public ISynchronizer Locker
        {
            get { return _btree.Locker; }
        }


        internal BTreeAlgorithm<TKey, TValue> Btree
        {
            get
            {
                return _btree;
            }
        }
        private readonly BTreeAlgorithm<TKey, TValue> _btree;
    }
}

#region Under Study...
//using System.Collections.Generic;
//using Sop.Collections.BTree;
//using Sop.Collections.Generic.BTree;
//using Sop.Virtual;

//namespace Sop.Collections.Generic
//{
//    using System;
//    using System.Collections;

//    /// <summary>
//    /// In-Memory, duplicates allowed Concurrent (thread safe) Sorted Dictionary.
//    /// </summary>
//    /// <typeparam name="TKey"></typeparam>
//    /// <typeparam name="TValue"></typeparam>
//#if !DEVICE
//    [Serializable]
//#endif
//    internal struct ConcurrentSortedDictionary<TKey, TValue> : ISortedDictionary<TKey, TValue>, BTree.IBTreeAlgorithm<TKey, TValue>
//    {
//        /// <summary>
//        /// Constructor to use if you want to provide number of slots per node and your comparer object
//        /// </summary>
//        /// <param name="slotLen">Number of slots per node</param>
//        /// <param name="comparer">compare object defining how records will be sorted</param>
//        /// <param name="sortOrder"> </param>
//        public ConcurrentSortedDictionary(byte slotLen = BTreeAlgorithm.DefaultSlots,
//            IComparer<TKey> comparer = null, SortOrderType sortOrder = SortOrderType.Ascending)
//        {
//            _btreeWrite = new BTree.BTreeAlgorithm<TKey, TValue>(slotLen, comparer);
//            _currentItem = new BTreeAlgorithm<TKey, TValue>.TreeNode.ItemAddress();
//            _sortOrder = sortOrder;
//            _enumerator = null;
//        }
//        /// <summary>
//        /// Returns an enumerator that iterates through the collection.
//        /// </summary>
//        /// <returns>
//        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
//        /// </returns>
//        /// <filterpriority>1</filterpriority>
//        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
//        {
//            if (_enumerator == null || ((SortedDictionary<TKey, TValue>.BTreeEnumeratorDefault)_enumerator).BTree == null)
//            {
//                _enumerator = new SortedDictionary<TKey, TValue>.BTreeEnumeratorDefault(this);
//            }
//            return _enumerator;
//        }
//        private IEnumerator<KeyValuePair<TKey, TValue>> _enumerator;

//        /// <summary>
//        /// Returns an enumerator that iterates through a collection.
//        /// </summary>
//        /// <returns>
//        /// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
//        /// </returns>
//        /// <filterpriority>2</filterpriority>
//        IEnumerator IEnumerable.GetEnumerator()
//        {
//            return GetEnumerator();
//        }

//        /// <summary>
//        /// Adds an item to the <see cref="T:System.Collections.Generic.ICollection`1"/>.
//        /// </summary>
//        /// <param name="item">The object to add to the <see cref="T:System.Collections.Generic.ICollection`1"/>.
//        ///                 </param><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.
//        ///                 </exception>
//        public void Add(KeyValuePair<TKey, TValue> item)
//        {
//            SyncRoot.Lock();
//            BtreeWrite.Add(item.Key, item.Value);
//            SyncRoot.Unlock();
//        }

//        // todo: set thread to certain ID, Store in Synchronizer the BTreeReader for a given thread.

//        /// <summary>
//        /// Removes all items from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
//        /// </summary>
//        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only. 
//        ///                 </exception>
//        public void Clear()
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// Determines whether the <see cref="T:System.Collections.Generic.ICollection`1"/> contains a specific value.
//        /// </summary>
//        /// <returns>
//        /// true if <paramref name="item"/> is found in the <see cref="T:System.Collections.Generic.ICollection`1"/>; otherwise, false.
//        /// </returns>
//        /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.ICollection`1"/>.
//        ///                 </param>
//        public bool Contains(KeyValuePair<TKey, TValue> item)
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// Copies the elements of the <see cref="T:System.Collections.Generic.ICollection`1"/> to an <see cref="T:System.Array"/>, starting at a particular <see cref="T:System.Array"/> index.
//        /// </summary>
//        /// <param name="array">The one-dimensional <see cref="T:System.Array"/> that is the destination of the elements copied from <see cref="T:System.Collections.Generic.ICollection`1"/>. The <see cref="T:System.Array"/> must have zero-based indexing.
//        ///                 </param><param name="arrayIndex">The zero-based index in <paramref name="array"/> at which copying begins.
//        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="array"/> is null.
//        ///                 </exception><exception cref="T:System.ArgumentOutOfRangeException"><paramref name="arrayIndex"/> is less than 0.
//        ///                 </exception><exception cref="T:System.ArgumentException"><paramref name="array"/> is multidimensional.
//        ///                     -or-
//        ///                 <paramref name="arrayIndex"/> is equal to or greater than the length of <paramref name="array"/>.
//        ///                     -or-
//        ///                     The number of elements in the source <see cref="T:System.Collections.Generic.ICollection`1"/> is greater than the available space from <paramref name="arrayIndex"/> to the end of the destination <paramref name="array"/>.
//        ///                     -or-
//        ///                     Type <paramref name="T"/> cannot be cast automatically to the type of the destination <paramref name="array"/>.
//        ///                 </exception>
//        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// Removes the first occurrence of a specific object from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
//        /// </summary>
//        /// <returns>
//        /// true if <paramref name="item"/> was successfully removed from the <see cref="T:System.Collections.Generic.ICollection`1"/>; otherwise, false. This method also returns false if <paramref name="item"/> is not found in the original <see cref="T:System.Collections.Generic.ICollection`1"/>.
//        /// </returns>
//        /// <param name="item">The object to remove from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
//        ///                 </param><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.
//        ///                 </exception>
//        public bool Remove(KeyValuePair<TKey, TValue> item)
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// Gets the number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1"/>.
//        /// </summary>
//        /// <returns>
//        /// The number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1"/>.
//        /// </returns>
//        public int Count
//        {
//            get { throw new NotImplementedException(); }
//        }

//        /// <summary>
//        /// Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.
//        /// </summary>
//        /// <returns>
//        /// true if the <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only; otherwise, false.
//        /// </returns>
//        public bool IsReadOnly
//        {
//            get { throw new NotImplementedException(); }
//        }

//        /// <summary>
//        /// Determines whether the <see cref="T:System.Collections.Generic.IDictionary`2"/> contains an element with the specified key.
//        /// </summary>
//        /// <returns>
//        /// true if the <see cref="T:System.Collections.Generic.IDictionary`2"/> contains an element with the key; otherwise, false.
//        /// </returns>
//        /// <param name="key">The key to locate in the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
//        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.
//        ///                 </exception>
//        public bool ContainsKey(TKey key)
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
//        /// </summary>
//        /// <param name="key">The object to use as the key of the element to add.
//        ///                 </param><param name="value">The object to use as the value of the element to add.
//        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.
//        ///                 </exception><exception cref="T:System.ArgumentException">An element with the same key already exists in the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
//        ///                 </exception><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.
//        ///                 </exception>
//        public void Add(TKey key, TValue value)
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
//        /// </summary>
//        /// <returns>
//        /// true if the element is successfully removed; otherwise, false.  This method also returns false if <paramref name="key"/> was not found in the original <see cref="T:System.Collections.Generic.IDictionary`2"/>.
//        /// </returns>
//        /// <param name="key">The key of the element to remove.
//        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.
//        ///                 </exception><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.
//        ///                 </exception>
//        public bool Remove(TKey key)
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// Gets the value associated with the specified key.
//        /// </summary>
//        /// <returns>
//        /// true if the object that implements <see cref="T:System.Collections.Generic.IDictionary`2"/> contains an element with the specified key; otherwise, false.
//        /// </returns>
//        /// <param name="key">The key whose value to get.
//        ///                 </param><param name="value">When this method returns, the value associated with the specified key, if the key is found; otherwise, the default value for the type of the <paramref name="value"/> parameter. This parameter is passed uninitialized.
//        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.
//        ///                 </exception>
//        public bool TryGetValue(TKey key, out TValue value)
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// Gets or sets the element with the specified key.
//        /// </summary>
//        /// <returns>
//        /// The element with the specified key.
//        /// </returns>
//        /// <param name="key">The key of the element to get or set.
//        ///                 </param><exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.
//        ///                 </exception><exception cref="T:System.Collections.Generic.KeyNotFoundException">The property is retrieved and <paramref name="key"/> is not found.
//        ///                 </exception><exception cref="T:System.NotSupportedException">The property is set and the <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.
//        ///                 </exception>
//        public TValue this[TKey key]
//        {
//            get { throw new NotImplementedException(); }
//            set { throw new NotImplementedException(); }
//        }

//        /// <summary>
//        /// Gets an <see cref="T:System.Collections.Generic.ICollection`1"/> containing the keys of the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
//        /// </summary>
//        /// <returns>
//        /// An <see cref="T:System.Collections.Generic.ICollection`1"/> containing the keys of the object that implements <see cref="T:System.Collections.Generic.IDictionary`2"/>.
//        /// </returns>
//        public System.Collections.Generic.ICollection<TKey> Keys
//        {
//            get { throw new NotImplementedException(); }
//        }

//        /// <summary>
//        /// Gets an <see cref="T:System.Collections.Generic.ICollection`1"/> containing the values in the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
//        /// </summary>
//        /// <returns>
//        /// An <see cref="T:System.Collections.Generic.ICollection`1"/> containing the values in the object that implements <see cref="T:System.Collections.Generic.IDictionary`2"/>.
//        /// </returns>
//        public System.Collections.Generic.ICollection<TValue> Values
//        {
//            get { throw new NotImplementedException(); }
//        }

//        /// <summary>
//        /// Creates a new object that is a copy of the current instance.
//        /// </summary>
//        /// <returns>
//        /// A new object that is a copy of this instance.
//        /// </returns>
//        /// <filterpriority>2</filterpriority>
//        public object Clone()
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// MoveNext makes the next entry current.
//        /// </summary>
//        public bool MoveNext()
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// MovePrevious makes the previous entry current.
//        /// </summary>
//        public bool MovePrevious()
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// MoveFirst makes the first entry in the Collection current.
//        /// </summary>
//        public bool MoveFirst()
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// MoveLast makes the last entry in the Collection current.
//        /// </summary>
//        public bool MoveLast()
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// Search the Collection for existence of entry with a given key.
//        /// </summary>
//        /// <param name="key">key to search for.</param>
//        /// <returns>true if found, false otherwise.</returns>
//        public bool Search(TKey key)
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// true if end of tree is reached (CurrentItem is null), otherwise false.
//        /// </summary>
//        /// <returns></returns>
//        public bool EndOfTree()
//        {
//            throw new NotImplementedException();
//        }

//        /// <summary>
//        /// Returns current sort order. Setting to a different sort order will 
//        /// reset BTree. First item according to sort order will be current item.
//        /// </summary>
//        public SortOrderType SortOrder
//        {
//            get { return _sortOrder; }
//            set
//            {
//                _sortOrder = value;
//                if (_btreeWrite.Count > 0)
//                    MoveFirst();
//            }
//        }
//        private SortOrderType _sortOrder;

//        /// <summary>
//        /// Returns the Current entry.
//        /// </summary>
//        public BTreeItem<TKey, TValue> CurrentEntry
//        {
//            get { throw new NotImplementedException(); }
//        }

//        /// <summary>
//        /// Returns the Current entry's key.
//        /// </summary>
//        public TKey CurrentKey
//        {
//            get { throw new NotImplementedException(); }
//        }

//        /// <summary>
//        /// Returns the Current entry's Value.
//        /// </summary>
//        public TValue CurrentValue
//        {
//            get
//            {
//                return 
//            }
//            set { throw new NotImplementedException(); }
//        }

//        /// <summary>
//        /// Remove an entry from Sorted Dictionary.
//        /// </summary>
//        public void Remove()
//        {
//            SyncRoot.Lock();
//            _btreeWrite.Remove();
//            SyncRoot.Unlock();
//        }

//        public bool Search(TKey key, bool goToFirstInstance)
//        {
//            bool r = false;
//            SyncRoot.Lock(OperationType.Read);
//            if (Count > 0)
//            {
//                if (CurrentEntry == null || Comparer.Compare(CurrentEntry.Key, key) != 0 ||
//                    goToFirstInstance)
//                {
//                    var item = new BTreeItem<TKey, TValue>(key, default(TValue));
//                    r = _btreeWrite.Root.Search(this, item, goToFirstInstance);
//                }
//                else
//                    r = true; // current entry's key matches key to search for.
//            }
//            SyncRoot.Unlock(OperationType.Read);
//            return r;
//        }

//        public BTreeAlgorithm<TKey, TValue>.TreeNode.ItemAddress CurrentItem
//        {
//            get { return _currentItem; }
//        }
//        private BTreeAlgorithm<TKey, TValue>.TreeNode.ItemAddress _currentItem;

//        public void SetCurrentItemAddress(BTree.BTreeAlgorithm<TKey, TValue>.TreeNode itemNode, byte itemIndex)
//        {
//            _currentItem.Node = itemNode;
//            _currentItem.NodeItemIndex = itemIndex;
//        }

//        public byte SlotLength
//        {
//            get { return _btreeWrite.SlotLength; }
//        }

//        public IComparer<BTreeItem<TKey, TValue>> SlotsComparer
//        {
//            get { return _btreeWrite.SlotsComparer; }
//        }

//        public IComparer<TKey> Comparer
//        {
//            get { return _btreeWrite.Comparer; }
//            set { _btreeWrite.Comparer = value; }
//        }

//        public Synchronizer SyncRoot
//        {
//            get { return _btreeWrite.SyncRoot; }
//        }


//        private BTreeAlgorithm<TKey, TValue> BtreeWrite
//        {
//            get
//            {
//                if (SyncRoot.LastOperationType != OperationType.Write)
//                    _btreeWrite.SetCurrentItemAddress(CurrentItem.Node, CurrentItem.NodeItemIndex);
//                return _btreeWrite;
//            }
//        }
//        private readonly BTreeAlgorithm<TKey, TValue> _btreeWrite;
//    }
//}

#endregion