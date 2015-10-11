// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using Sop.OnDisk;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.Persistence;

namespace Sop.SpecializedDataStore
{
    /// <summary>
    /// Sorted Dictionary for simple value types that stores items to user selected storage medium.
    /// NOTE: only SOP on Disk is currently supported. Following lists the supported simple types:
    /// string, byte[], float, short, ushort, double, bool, byte, sbyte, char, decimal, int, uint, long, ulong.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <remarks>Thread Safe</remarks>
    public partial class SimpleKeyValue<TKey, TValue> : SpecializedStoreBase, ISortedDictionary<TKey, TValue>
    {
        #region Constructors
        public SimpleKeyValue() { }
        public SimpleKeyValue(object container, string name) : this(container, null, name) { }
        public SimpleKeyValue(object container,
                                 IComparer<TKey> comparer, string name) :
            this(container, comparer, name, DataStoreType.SopOndisk) { }
        internal SimpleKeyValue(object container,
                                   IComparer<TKey> comparer, string name, DataStoreType dataStoreType) :
            this(container, comparer, name, dataStoreType, null, false) { }
        internal SimpleKeyValue(object container,
                                   IComparer<TKey> comparer, string name, DataStoreType dataStoreType,
                                   ISortedDictionaryOnDisk dataStore, bool isDataInKeySegment)
        {
            if (container == null)
                throw new ArgumentNullException("container");
            ISortedDictionaryOnDisk containerDod = null;
            if (container is ISortedDictionaryOnDisk)
                containerDod = (ISortedDictionaryOnDisk)container;
            else if (container is IProxyObject &&
                     ((IProxyObject)container).RealObject is ISortedDictionaryOnDisk)
                containerDod = (ISortedDictionaryOnDisk)((IProxyObject)container).RealObject;
            else
                throw new ArgumentException(
                    "container type isn't supported. Only Sop.ISortedDictionaryOnDisk types are allowed.");
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException("name");
            this.DataStoreType = DataStoreType;
            if (comparer == null)
                comparer = Comparer<TKey>.Default;
            GenericComparer<TKey> _Comparer = null;
            _Comparer = new GenericComparer<TKey>(comparer);
            if (dataStore == null)
                dataStore = GetCollection(containerDod, _Comparer, name, isDataInKeySegment);
            Collection = dataStore;

            if (!IsLongCompatible(typeof(TValue))) return;
            if (!((OnDisk.Algorithm.SortedDictionary.ISortedDictionaryOnDisk)Collection).IsDataLongInt)
                ((OnDisk.Algorithm.SortedDictionary.ISortedDictionaryOnDisk)Collection).IsDataLongInt = true;
        }
        #endregion

        /// <summary>
        /// Store's Data Address on File. i.e. - numeric File Offset where 1st byte of the Store is located.
        /// </summary>
        internal long DataAddress;

        /// <summary>
        /// Store's URI path. This gets populated if Store was retrieved using the StoreNavigator.
        /// </summary>
        public string Path { get; internal set; }

        /// <summary>
        /// Is Object one of basic types of .net such as the Int family, and the float/double family.
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static bool IsLongCompatible(Type t)
        {
            return t == typeof(long);
        }


        /// <summary>
        /// Return the real object proxied by this wrapper object
        /// </summary>
        public object RealObject
        {
            get { return Collection; }
            set
            {
                if (!(value is ISortedDictionaryOnDisk))
                    throw new ArgumentException("value isn't Sop.Collections.OnDisk.SortedDictionaryOnDisk type.");
                Collection = (ISortedDictionaryOnDisk)value;
            }
        }

        /// <summary>
        /// Returns the Current Sequence value
        /// </summary>
        public long CurrentSequence
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't return its CurrentSequence.");
                return Collection.CurrentSequence;
            }
        }

        /// <summary>
        /// Generate a new log sequence and return it
        /// </summary>
        /// <returns></returns>
        public long GetNextSequence()
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't return its NextSequence.");
            try
            {
                return Collection.GetNextSequence();
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.TransactionBase)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "GetNextSequence call failed. Transaction was rolled back to prevent damage to your database.",
                        exc);
                }
                throw;
            }
        }

        /// <summary>
        /// Returns the Store given its Container and Name.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"></param>
        /// <returns></returns>
        protected virtual ISortedDictionaryOnDisk GetCollection(
            ISortedDictionaryOnDisk container,
            GenericComparer<TKey> comparer, string name, bool isDataInKeySegment)
        {
            if (container == null)
                throw new ArgumentNullException("container");
            if (comparer == null)
                throw new ArgumentNullException("comparer");
            try
            {
                var o = (ISortedDictionaryOnDisk)container.GetValue(name, null);
                if (o == null)
                {
                    if (container.Transaction != null)
                        o = ((Transaction.ITransactionLogger)container.Transaction).CreateCollection(
                            container.File, comparer, name, isDataInKeySegment);
                    else
                        o = OnDisk.ObjectServer.CreateDictionaryOnDisk(
                            ((OnDisk.Algorithm.SortedDictionary.ISortedDictionaryOnDisk)container).File, comparer, name,
                            isDataInKeySegment);
                    o.Open();
                    o.Flush();
                    container.Add(o.Name, o);
                }
                else
                {
                    o.Comparer = comparer;
                    o.Open();
                }
                DataAddress = o.DataAddress;
                o.Container = container;
                return o;
            }
            catch (Exception exc)
            {
                if (container.Transaction != null)
                {
                    container.Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "GetCollection call failed. Transaction was rolled back to prevent damage to your database.",
                        exc);
                }
                throw;
            }
        }

        /// <summary>
        /// Override ToString to return the string value of data address on disk.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return DataAddress.ToString();
        }

        /// <summary>
        /// Returns name of the Sorted Dictionary
        /// </summary>
        public string Name
        {
            get
            {
                return Collection != null ? Collection.Name : string.Empty;
            }
        }

        public IFile File
        {
            get { return Collection.File; }
        }

        /// <summary>
        /// Delete from data store this collection and dispose it from memory.
        /// </summary>
        public void Delete()
        {
            if (File.Server.ReadOnly)
                throw new InvalidOperationException("Object Server was opened in read only mode.");
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't delete it.");
            try
            {
                Locker.Invoke(() =>
                {
                    Collection.Delete();
                    Collection = null;
                });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Delete call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        #region Close & Dispose
        /// <summary>
        /// Dispose the Sorted Dictionary
        /// </summary>
        public void Dispose()
        {
            if (Collection == null ||
                ((SortedDictionaryOnDisk)Collection).BTreeAlgorithm == null)
                return;

            Sop.VoidFunc d = () =>
                {
                    if (!InvokeFromMru)
                        AutoDisposeItem = true;
                    Close();
                    ((SortedDictionaryOnDisk)Collection).BTreeAlgorithm.Dispose();
                    ((SortedDictionaryOnDisk)Collection).BTreeAlgorithm = null;
                    Collection.Container = null;
                    IsDisposed = true;
                };

            if (Locker.TransactionRollback)
            {
                d();
                return;
            }
            Locker.Invoke(d);
        }
        private void Close()
        {
            // remove store from StoreFactory's MRU of opened Stores.
            StoreFactory.OpenedStores.Remove(UniqueName);

            _keys = null;
            _values = null;
            try
            {
                if (Collection != null)
                    Collection.Close();
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Close call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }
        private volatile bool _isDisposed;
        /// <summary>
        /// true means this is Disposed, otherwise false
        /// </summary>
        public bool IsDisposed
        {
            get
            {
                return _isDisposed || Collection == null || ((SortedDictionaryOnDisk)Collection).IsDisposed;
            }
            set
            {
                _isDisposed = value;
            }
        }
        #endregion

        /// <summary>
        /// Save changes to the Sorted Dictionary
        /// </summary>
        public void Flush()
        {
            if (File.Server.ReadOnly)
                throw new InvalidOperationException("Object Server was opened in read only mode.");
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't save.");
            try
            {
                Locker.Invoke(() => { Collection.Flush(); });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Flush call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        /// <summary>
        /// Transaction this object belongs to
        /// </summary>
        public ITransaction Transaction
        {
            get
            {
                return Collection != null ? Collection.Transaction : null;
            }
            set
            {
                if (Collection != null)
                    Collection.Transaction = value;
            }
        }

        // only SopOnDisk type is supported, no need to lock.
        internal DataStoreType DataStoreType
        {
            get { return _dataStoreType; }
            set
            {
                if (value != DataStoreType.SopOndisk)
                    throw new NotImplementedException(string.Format("DataStoreType {0} not supported yet.", value));
                _dataStoreType = value;
            }
        }
        private DataStoreType _dataStoreType = DataStoreType.SopOndisk;

        #region IDictionary<TKey,TValue> Members

        /// <summary>
        /// Add an item to the dictionary
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void Add(TKey key, TValue value)
        {
            Add((object)key, (object)value);
        }
        /// <summary>
        /// Add entry if its key doesn't exist yet in this Dictionary.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool AddIfNotExist(TKey key, TValue value)
        {
            return Locker.Invoke(() => { return AddIfNotExist((object)key, (object)value); });
        }

        /// <summary>
        /// Checks whether an item with key exists in the Dictionary
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool ContainsKey(TKey key)
        {
            return Contains((object)key);
        }

        private ICollection<TKey> _keys;

        /// <summary>
        /// Returns Collection of Keys for this Dictionary.
        /// The returned collection is just a wrapper object that
        /// references the same items' Keys on disk of this Dictionary.
        /// </summary>
        public ICollection<TKey> Keys
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't return Keys.");
                return Locker.Invoke(() =>
                    {
                        if (_keys == null && Collection.IsOpen)
                        {
                            try
                            {
                                _keys = new GenericCollection<TKey>(
                                    (SortedDictionaryOnDisk)Collection.Keys);
                            }
                            catch (Exception exc)
                            {
                                if (Transaction != null &&
                                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                                    Sop.Transaction.CommitPhase.UnCommitted)
                                {
                                    Transaction.Rollback();
                                    throw new Transaction.TransactionRolledbackException(
                                        "Keys call failed. Transaction was rolled back to prevent damage to your database.", exc);
                                }
                                throw;
                            }
                        }
                        return _keys;
                    });
            }
        }
        /// <summary>
        /// Change the name of this Collection.
        /// </summary>
        /// <param name="newName"></param>
        public void Rename(string newName)
        {
            if (File.Server.ReadOnly)
                throw new InvalidOperationException("Object Server was opened in read only mode.");
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't Rename it.");
            try
            {
                Locker.Invoke(() => { Collection.Rename(newName); });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Remove call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        #region Remove
        /// <summary>
        /// Remove the currently selected Item from the Sorted Dictionary
        /// </summary>
        /// <returns>if there is no selected item, returns false, otherwise true</returns>
        public bool Remove()
        {
            if (File.Server.ReadOnly)
                throw new InvalidOperationException("Object Server was opened in read only mode.");
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't Remove item from it.");
            try
            {
                return Locker.Invoke(() =>
                {
                    if (Collection.EndOfTree())
                        return false;
                    Collection.Remove();
                    return true;
                });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Remove call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        public bool Remove(TKey key, bool removeAllOccurence)
        {
            var qe = new[] { new QueryExpression<TKey> { Key = key } };
            QueryResult<TKey>[] result;
            return Remove(qe, removeAllOccurence, out result);
        }

        public bool Remove(QueryExpression<TKey>[] keyExpressions)
        {
            QueryResult<TKey>[] results;
            return Remove(keyExpressions, out results);
        }

        public bool Remove(QueryExpression<TKey>[] keyExpressions, out QueryResult<TKey>[] results)
        {
            return Remove(keyExpressions, false, out results);
        }
        /// <summary>
        /// Remove Item with key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool Remove(TKey key)
        {
            return Remove(key, false);
        }

        /// <summary>
        /// Remove Items matching the specified query filter expressions.
        /// </summary>
        /// <param name="keyExpressions"></param>
        /// <param name="removeAllOccurence"></param>
        /// <param name="results"> </param>
        /// <returns></returns>
        public bool Remove(QueryExpression<TKey>[] keyExpressions, bool removeAllOccurence,
            out QueryResult<TKey>[] results)
        {
            if (File.Server.ReadOnly)
                throw new InvalidOperationException("Object Server was opened in read only mode.");
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't Remove item from it.");
            try
            {
                Locker.Lock();
                try
                {
                    QueryResult[] results2;
                    if (!Collection.Remove(QueryExpression<TKey>.Convert(keyExpressions), removeAllOccurence, out results2))
                    {
                        results = null;
                        return false;
                    }
                    results = new QueryResult<TKey>[results2.Length];
                    for (int i = 0; i < results.Length; i++)
                    {
                        results[i] = new QueryResult<TKey>(keyExpressions[i].Key, results2[i]);
                    }
                    return true;
                }
                finally
                {
                    Locker.Unlock();
                }
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Remove call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }
        #endregion

        /// <summary>
        /// Retrieve Value if key is in Dictionary, otherwise returns false to imply key not found.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool TryGetValue(TKey key, out TValue value)
        {
            value = default(TValue);
            Locker.Lock();
            try
            {
                if (Contains(key))
                {
                    value = (TValue)CurrentValue;
                    return true;
                }
                return false;
            }
            finally
            {
                Locker.Unlock();
            }
        }

        private ICollection<TValue> _values;


        //todo: values need to be locked!

        /// <summary>
        /// Returns Collection of Values for this Dictionary.
        /// The returned collection is just a wrapper object that
        /// references the same items' Values on disk of this Dictionary.
        /// </summary>
        public ICollection<TValue> Values
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't return its Values.");
                if (_values == null && Collection.IsOpen)
                {
                    try
                    {
                        _values = new GenericCollection<TValue>(
                            (SortedDictionaryOnDisk)Collection.Values);
                    }
                    catch (Exception exc)
                    {
                        if (Transaction != null &&
                            ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                            Sop.Transaction.CommitPhase.UnCommitted)
                        {
                            Transaction.Rollback();
                            throw new Transaction.TransactionRolledbackException(
                                "Values call failed. Transaction was rolled back to prevent damage to your database.",
                                exc);
                        }
                        throw;
                    }
                }
                return _values;
            }
        }

        public bool Update(KeyValuePair<TKey, TValue>[] items)
        {
            if (File.Server.ReadOnly)
                throw new InvalidOperationException("Object Server was opened in read only mode.");
            if (items == null || items.Length == 0)
                throw new ArgumentNullException("items");
            bool found = false;
            foreach (KeyValuePair<TKey, TValue> item in items)
            {
                Locker.Invoke(() =>
                {
                    if (this.Search(item.Key))
                    {
                        found = true;
                        this.CurrentValue = item.Value;
                    }
                });
            }
            return found;
        }

        /// <summary>
        /// this accessor checks whether key is in dictionary and returns the found item's value,
        /// otherwise returns null.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public TValue this[TKey key]
        {
            get { return (TValue)this[(object)key]; }
            set { this[(object)key] = value; }
        }

        #endregion

        #region ICollection<KeyValuePair<TKey,TValue>> Members

        /// <summary>
        /// Add Item to the Dictionary
        /// </summary>
        /// <param name="item"></param>
        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add((object)item.Key, (object)item.Value);
        }

        public void Add(KeyValuePair<TKey, TValue>[] items)
        {
            if (items == null)
                throw new ArgumentNullException("items");
            foreach (KeyValuePair<TKey, TValue> item in items)
                Add(item);
        }

        /// <summary>
        /// Clear the Dictionary of all Items.
        /// NOTE: Clear is synonymous to Delete in this version.
        /// </summary>
        public void Clear()
        {
            Delete();
        }

        /// <summary>
        /// Check for existence of an Item given its Key
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return Contains((object)item.Key);
        }

        /// <summary>
        /// Copy to a target array the contents of the Sorted Dictionary starting from the 1st item.
        /// When end of dictionary or end of target array is reached, copying will end.
        /// </summary>
        /// <param name="array"></param>
        /// <param name="arrayIndex"></param>
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            Locker.Invoke(() => { CopyTo((Array)array, arrayIndex); });
        }

        public IComparer<TKey> Comparer
        {
            get
            {
                if (Collection != null && Collection.Comparer is GenericComparer<TKey>)
                    return (IComparer<TKey>)((GenericComparer<TKey>)Collection.Comparer).Comparer;
                return null;
            }
            set
            {
                if (Collection == null)
                    throw new InvalidOperationException("Can't assign comparer to a null Collection.");
                Collection.Comparer = value != null ? new GenericComparer<TKey>(value) : null;
            }
        }

        /// <summary>
        /// Returns count of items of Dictionary
        /// </summary>
        public long Count
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't return Count of Items.");
                return Collection.Count;
            }
        }
        int ICollection<KeyValuePair<TKey, TValue>>.Count
        {
            get
            {
                return (int)Count;
            }
        }

        public bool IsReadOnly
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't check if it's read only.");
                return Collection.IsReadOnly;
            }
        }

        public bool IsDataInKeySegment
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't check if it's read only.");
                return Collection.IsDataInKeySegment;
            }
        }

        public bool IsUnique
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't check if it's read only.");
                return Collection.IsUnique;
            }
        }

        /// <summary>
        /// Remove Item from Dictionary
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove((TKey)item.Key);
        }

        #endregion

        #region enumerators
        // todo: GetEnumerators need to be synchronized...

        /// <summary>
        /// Get Enumerator returns an enumerator for this Store.
        /// 
        /// NOTE: enumerator usage for iterating all Store items should
        /// not be used in Production environment due to associated cost
        /// and it is not thread safe.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't return its Enumerator.");
            try
            {
                return new GenericEnumerator<KeyValuePair<TKey, TValue>>(this.Collection.GetEnumerator());
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "GetEnumerator call failed. Transaction was rolled back to prevent damage to your database.",
                        exc);
                }
                throw;
            }
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't return its Enumerator.");
            try
            {
                return Collection.GetEnumerator();
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "IEnumerable.GetEnumerator call failed. Transaction was rolled back to prevent damage to your database.",
                        exc);
                }
                throw;
            }
        }
        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't return its Enumerator.");
            try
            {
                return new OnDisk.Algorithm.SortedDictionary.SortedDictionaryOnDisk.DictionaryEnumerator<TKey, TValue>(
                    (OnDisk.Algorithm.SortedDictionary.SortedDictionaryOnDisk)Collection);
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "IEnumerable.GetEnumerator call failed. Transaction was rolled back to prevent damage to your database.",
                        exc);
                }
                throw;
            }
        }
        #endregion

        #region IDictionary Members

        private bool AddIfNotExist(object key, object value)
        {
            if (File.Server.ReadOnly)
                throw new InvalidOperationException("Object Server was opened in read only mode.");
            if (Collection == null)
                throw new InvalidOperationException(
                    "Collection is null, can't add Item to it. Ensure it hasn't been disposed nor deleted.");

            try
            {
                return Collection.AddIfNotExist(key, value);
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Add call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }
        private void Add(object key, object value)
        {
            if (File.Server.ReadOnly)
                throw new InvalidOperationException("Object Server was opened in read only mode.");
            if (Collection == null)
                throw new InvalidOperationException(
                    "Collection is null, can't add Item to it. Ensure it hasn't been disposed nor deleted.");
            try
            {
                Locker.Invoke(() => { Collection.Add(key, value); });
            }
            catch (DuplicateKeyException)
            {
                // do nothing, 'just bubble exception for duplicate key failures.
                throw;
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Add call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        private bool Contains(object key)
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't check for Key existence.");
            try
            {
                return Locker.Invoke(() => { return Collection.Contains(key); });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Contains call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        private object this[object key]
        {
            get
            {
                try
                {
                    return Locker.Invoke(() => { return Collection[key]; });
                }
                catch (Exception exc)
                {
                    if (Transaction != null &&
                        ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                        Sop.Transaction.CommitPhase.UnCommitted)
                    {
                        Transaction.Rollback();
                        throw new Transaction.TransactionRolledbackException(
                            "this[key].Get call failed. Transaction was rolled back to prevent damage to your database.",
                            exc);
                    }
                    throw;
                }
            }
            set
            {
                try
                {
                    if (File.Server.ReadOnly)
                        throw new InvalidOperationException("Object Server was opened in read only mode.");
                    Locker.Invoke(() => { Collection[key] = value; });
                }
                catch (Exception exc)
                {
                    if (Transaction != null &&
                        ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                        Sop.Transaction.CommitPhase.UnCommitted)
                    {
                        Transaction.Rollback();
                        throw new Transaction.TransactionRolledbackException(
                            "this[key].Set call failed. Transaction was rolled back to prevent damage to your database.",
                            exc);
                    }
                    throw;
                }
            }
        }

        #endregion

        #region ICollection Members

        private void CopyTo(Array array, int index)
        {
            if (index < 0)
                throw new ArgumentOutOfRangeException("index", "index can't be negative.");
            if (array == null)
                throw new ArgumentNullException("array");
            if (index >= array.Length)
                throw new ArgumentOutOfRangeException("index", "index can't be >= array length.");
            try
            {
                DictionaryEntry[] d = new DictionaryEntry[array.Length - index];
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't copy items to target array.");
                Collection.CopyTo(d, 0);
                for (int i = 0; i < d.Length; i++)
                    array.SetValue(new KeyValuePair<TKey, TValue>((TKey)d[i].Key, (TValue)d[i].Value), index + i);
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "CopyTo call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        private bool IsSynchronized
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't check if it's Synchronized.");
                return true;
            }
        }

        private object SyncRoot
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't get sync root.");
                return Collection.SyncRoot;
            }
        }

        /// <summary>
        /// SortOrder can be ascending or descending
        /// </summary>
        public SortOrderType SortOrder
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't get sort order.");
                return Collection.SortOrder;
            }
            set
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't set sort order.");
                Collection.SortOrder = value;
            }
        }

        #endregion

        private object Collection_OnInnerMemberUnpack<T>(System.IO.BinaryReader reader)
            where T : IPersistent, new()
        {
            T k = new T();
            k.Unpack(reader);
            return k;
        }

        private void Collection_OnInnerMemberPack<T>(System.IO.BinaryWriter writer, object objectToPack)
            where T : IPersistent, new()
        {
            ((T)objectToPack).Pack(writer);
        }

        #region IPersistent Members

        public void Pack(System.IO.BinaryWriter writer)
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't Pack for serialization.");
            try
            {
                Locker.Invoke(() =>
                {
                    Collection.Flush();
                    ((OnDisk.Algorithm.SortedDictionary.ISortedDictionaryOnDisk)Collection).Pack(writer);
                });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Pack call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        public void Unpack(System.IO.BinaryReader reader)
        {
            if (Collection != null)
            {
                try
                {
                    Locker.Invoke(() => { ((OnDisk.Algorithm.SortedDictionary.ISortedDictionaryOnDisk)Collection).Unpack(reader); });
                }
                catch (Exception exc)
                {
                    if (Transaction != null &&
                        ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                        Sop.Transaction.CommitPhase.UnCommitted)
                    {
                        Transaction.Rollback();
                        throw new Transaction.TransactionRolledbackException(
                            "Unpack call failed. Transaction was rolled back to prevent damage to your database.", exc);
                    }
                    throw;
                }
            }
        }

        public bool HintSequentialRead
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't get HintSequentialRead.");
                return Locker.Invoke(() => { return Collection.HintSequentialRead; });
            }
            set
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't set HintSequentialRead.");
                Locker.Invoke(() => { this.Collection.HintSequentialRead = value; });
            }
        }

        public int HintBatchCount
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't get HintBatchCount.");
                return Locker.Invoke(() => { return Collection.HintBatchCount; });
            }
            set
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't set HintBatchCount.");
                Locker.Invoke(() => { Collection.HintBatchCount = value; });
            }
        }

        public int HintSizeOnDisk { get; private set; }

        #endregion

        /// <summary>
        /// Returns Current Item's Key
        /// </summary>
        public TKey CurrentKey
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't return CurrentKey.");
                try
                {
                    return Locker.Invoke(() =>
                    {
                        if (Collection.CurrentKey != null)
                            return (TKey)Collection.CurrentKey;
                        return default(TKey);
                    });
                }
                catch (Exception exc)
                {
                    if (Transaction != null &&
                        ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                        Sop.Transaction.CommitPhase.UnCommitted)
                    {
                        Transaction.Rollback();
                        throw new Transaction.TransactionRolledbackException(
                            "CurrentKey call failed. Transaction was rolled back to prevent damage to your database.",
                            exc);
                    }
                    throw;
                }
            }
        }

        /// <summary>
        /// Get/Set Current Item's Value
        /// </summary>
        public TValue CurrentValue
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't return CurrentValue.");
                return Locker.Invoke(() =>
                {
                    if (Collection.CurrentValue != null)
                        return (TValue)Collection.CurrentValue;
                    return default(TValue);
                });
            }
            set
            {
                if (File.Server.ReadOnly)
                    throw new InvalidOperationException("Object Server was opened in read only mode.");
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't set CurrentValue.");
                Locker.Invoke(() => { Collection.CurrentValue = value; });
            }
        }

        /// <summary>
        /// Move Current Item pointer to next item in Dictionary
        /// </summary>
        /// <returns></returns>
        public bool MoveNext()
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't MoveNext.");
            try
            {
                return Locker.Invoke(() => { return Collection.MoveNext(); });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "MoveNext call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        /// <summary>
        /// Move Current Item pointer to previous item in Dictionary
        /// </summary>
        /// <returns></returns>
        public bool MovePrevious()
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't MovePrevious.");
            try
            {
                return Locker.Invoke(() => { return Collection.MovePrevious(); });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "MovePrevious call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        /// <summary>
        /// Move Current Item pointer to 1st item in Dictionary per its ordering sequence
        /// </summary>
        /// <returns></returns>
        public bool MoveFirst()
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't MoveFirst.");
            try
            {
                return Locker.Invoke(() => { return Collection.MoveFirst(); });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "MoveFirst call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        /// <summary>
        /// Move Current Item pointer to last item in Dictionary per its ordering sequence
        /// </summary>
        /// <returns></returns>
        public bool MoveLast()
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't MoveLast.");
            try
            {
                return Locker.Invoke(() => { return Collection.MoveLast(); });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "MoveLast call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        /// <summary>
        /// true if Current Item pointer is beyond last item of the Dictionary, otherwise false
        /// </summary>
        /// <returns></returns>
        public bool EndOfTree()
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't check if it's on EndOfTree.");
            try
            {
                return Locker.Invoke(() => { return Collection.EndOfTree(); });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "EndOfTree call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        /// <summary>
        /// Query the B-Tree for each Keys submitted, retrieve their values
        /// and store them in the array out parameter Values.
        /// </summary>
        /// <param name="keys">Keys to search for</param>
        /// <param name="values">null if no Key found, otherwise an array of values in the same order as the submitted keys
        /// and having the same number of items as the keys. Key(s) not found will have null entries</param>
        /// <returns>true if found at least one key, otherwise false</returns>
        public bool Query(QueryExpression<TKey>[] keys, out QueryResult<TKey>[] values)
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't do Query.");
            try
            {
                QueryResult<TKey>[] _values = null;
                var f = Locker.Invoke(() =>
                    {
                        if (keys == null || keys.Length == 0)
                            throw new ArgumentNullException("keys");
                        var ks = new QueryExpression[keys.Length];
                        for (int i = 0; i < keys.Length; i++)
                        {
                            ks[i].Key = keys[i].Key;
                            ks[i].ValueFilterFunc = keys[i].ValueFilterFunc;
                        }
                        QueryResult[] vs;
                        if (Collection.Query(ks, out vs))
                        {
                            _values = new QueryResult<TKey>[keys.Length];
                            for (int i = 0; i < keys.Length; i++)
                            {
                                _values[i].Found = vs[i].Found;
                                _values[i].Key = keys[i].Key;
                                _values[i].Value = (TValue)vs[i].Value;
                            }
                            return true;
                        }
                        _values = null;
                        return false;
                    });
                values = _values;
                return f;
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Query call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        /// <summary>
        /// Search item with Key, passing false to GotoFirstInstance
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool Search(TKey key)
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't do Search.");
            try
            {
                return Locker.Invoke(() => { return Collection.Search(key, false); });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Search call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        /// <summary>
        /// Search B-Tree for an item with Key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="goToFirstInstance">Go to 1st key instance if the key has duplicate</param>
        /// <returns></returns>
        public bool Search(TKey key, bool goToFirstInstance)
        {
            if (Collection == null)
                throw new InvalidOperationException("Collection is null, can't fo Search.");
            try
            {
                return Locker.Invoke(() => { return Collection.Search(key, goToFirstInstance); });
            }
            catch (Exception exc)
            {
                if (Transaction != null &&
                    ((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase ==
                    Sop.Transaction.CommitPhase.UnCommitted)
                {
                    Transaction.Rollback();
                    throw new Transaction.TransactionRolledbackException(
                        "Search call failed. Transaction was rolled back to prevent damage to your database.", exc);
                }
                throw;
            }
        }

        /// <summary>
        /// Auto Dispose Item when it gets removed from Cache or when it gets deleted
        /// </summary>
        public bool AutoDisposeItem
        {
            get
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't return AutoDisposeItem value.");
                return Collection.AutoDisposeItem;
            }
            set
            {
                if (Collection != null)
                    Collection.AutoDisposeItem = value;
            }
        }

        /// <summary>
        /// When Data Value is saved in its own segment (IsDataSavedInKeySegment = false),
        /// True will auto flush the Data Value per each Add/Update action,
        /// False otherwise. Defaults to False.
        /// 
        /// Auto flush feature is useful for example, when managing 
        /// big data entities where user will want to save the record
        /// to disk and prevent it from getting buffered, which will
        /// impact server performance due to big memory requirements
        /// of the data.
        /// </summary>
        public bool AutoFlush
        {
            get
            {
                return Collection != null && this.Collection.AutoFlush;
            }
            set
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't set AutoDisposeItem value.");
                Collection.AutoFlush = value;
            }
        }

        /// <summary>
        /// Auto Dispose this Store when it gets removed from the container's cache.
        /// </summary>
        public bool AutoDispose
        {
            get
            {
                return Collection != null && this.Collection.AutoDispose;
            }
            set
            {
                if (Collection == null)
                    throw new InvalidOperationException("Collection is null, can't set AutoDisposeItem value.");
                Collection.AutoDispose = value;
            }
        }
    }
}
