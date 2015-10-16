// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections;
using Sop.Mru;
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.DataBlock;
using Sop.OnDisk.File;
using Sop.Collections.BTree;
using Sop.OnDisk.IO;
using Sop.Persistence;
using System.Threading;
using Sop.Synchronization;

//using System.Xml.Serialization;

namespace Sop.OnDisk.Algorithm.SortedDictionary
{
    /// <summary>
    /// BTree In Disk. Objects will be saved using Binary (De)Serialization
    /// to take advantage of this fast serialization method. Value(s)
    /// are recommended to implement ISerialize interface in order to control
    /// what and how their data will be saved.
    /// </summary>
    internal partial class SortedDictionaryOnDisk : ISortedDictionaryOnDisk, IInternalFileEntity
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public SortedDictionaryOnDisk()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="file"></param>
        public SortedDictionaryOnDisk(File.IFile file) : this(file, new Algorithm.BTree.BTreeDefaultComparer())
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="file"></param>
        /// <param name="comparer"></param>
        public SortedDictionaryOnDisk(File.IFile file,
                                      IComparer comparer)
            : this(file, comparer, string.Empty, false)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="file"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        public SortedDictionaryOnDisk(
            File.IFile file,
            IComparer comparer,
            string name, bool isDataInKeySegment)
        {
            Initialize(file, comparer, name, isDataInKeySegment);
        }

        int Collection.ICollectionOnDisk.OnMaxCapacity(int countOfBlocksUnloadToDisk)
        {
            return this.BTreeAlgorithm.OnMaxCapacity(countOfBlocksUnloadToDisk);
        }

        /// <summary>
        /// Is the other collection logically(their item container are the same) this Collection.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public virtual bool IsItMe(CollectionOnDisk other)
        {
            return BTreeAlgorithm == other;
        }

        public void Initialize(File.IFile file)
        {
            Initialize(file, new BTree.BTreeDefaultComparer(), string.Empty, true);
        }

        internal void Initialize(File.IFile file, bool hasMruSegments)
        {
            Initialize(file, new BTree.BTreeDefaultComparer(), string.Empty, hasMruSegments);
        }

        protected internal virtual void Initialize(File.IFile file,
                                                   IComparer comparer,
                                                   string name,
                                                   bool isDataInKeySegment)
        {
            if (BTreeAlgorithm == null)
            {
                BTreeAlgorithm = new Algorithm.BTree.BTreeAlgorithm(file, comparer, name, null,
                                                              isDataInKeySegment) {Container = this};
            }
            this.File = file;
        }

        internal SortedDictionaryOnDisk(SortedDictionaryOnDisk bTree, Collections.BTree.ItemType itemType,
            OperationType requestOperation = OperationType.Read)
        {
            bTree.Locker.Invoke(() =>
            {
                if (bTree.IsDirty)
                {
                    lock (bTree)
                    {
                        if (bTree.IsDirty)
                            bTree.Flush();
                    }
                }
                BTreeAlgorithm = (Algorithm.BTree.BTreeAlgorithm)bTree.BTreeAlgorithm.Clone();
                SyncRoot = (ISynchronizer)bTree.SyncRoot;
                BTreeAlgorithm.Container = this;
                this.File = File;
                BTreeAlgorithm.CurrentSortOrder = bTree.SortOrder;
                this.ItemType = itemType;
            }, requestOperation);
        }

        private void dispose()
        {
            if (BTreeAlgorithm == null) return;
            if (Container != null)
            {
                if (!IsCloned)
                    ((SortedDictionaryOnDisk) Container).RemoveInMemory(DataAddress);
                Container = null;
            }
            if (_keys != null)
            {
                _keys.Dispose();
                _keys = null;
            }
            if (_values != null)
            {
                _values.Dispose();
                _values = null;
            }
            BTreeAlgorithm.InternalDispose();
            BTreeAlgorithm = null;
        }

        public bool IsDisposed
        {
            get
            {
                return BTreeAlgorithm == null;
            }
            set { }
        }

        /// <summary>
        /// Dispose this Sorted Dictionary from memory. NOTE: data on disk are not removed.
        /// </summary>
        public void Dispose()
        {
            dispose();
        }

        /// <summary>
        /// Returns true if Value is in Binary Reader Stream (OnDiskBinaryReader),
        /// otherwise false
        /// </summary>
        public bool IsValueInStream
        {
            get { return BTreeAlgorithm.IsValueInStream; }
        }

        public bool IsTransactionStore
        {
            get { return BTreeAlgorithm.IsTransactionStore; }
            internal set { BTreeAlgorithm.IsTransactionStore = value; }
        }

        /// <summary>
        /// For SOP Internal use only.
        /// ParentTransactionLogger gets/sets the parent transaction logger
        /// of this Collection.
        /// </summary>
        public Transaction.ITransactionLogger ParentTransactionLogger
        {
            get { return BTreeAlgorithm.ParentTransactionLogger; }
            set { BTreeAlgorithm.ParentTransactionLogger = value; }
        }

        /// <summary>
        /// Stream Reader
        /// </summary>
        public System.IO.BinaryReader StreamReader
        {
            get { return BTreeAlgorithm.StreamReader; }
        }

        /// <summary>
        /// MRU cache manager
        /// </summary>
        public IMruManager MruManager
        {
            get
            {
                if (BTreeAlgorithm != null)
                    return BTreeAlgorithm.MruManager;
                return null;
            }
            set { BTreeAlgorithm.MruManager = value; }
        }

        /// <summary>
        /// Recreate/DeSerialize the object from source Sop.DataBlock
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public object ReadFromBlock(Sop.DataBlock source)
        {
            return BTreeAlgorithm.ReadFromBlock(source);
        }

        public bool IsDataLongInt
        {
            get { return BTreeAlgorithm.IsDataLongInt; }
            set { BTreeAlgorithm.IsDataLongInt = value; }
        }

        public bool IsUnique
        {
            get { return BTreeAlgorithm.IsUnique; }
            set { BTreeAlgorithm.IsUnique = value; }
        }

        public bool IsDataInKeySegment
        {
            get { return BTreeAlgorithm.IsDataInKeySegment; }
            set { BTreeAlgorithm.IsDataInKeySegment = value; }
        }

        /// <summary>
        /// IsDirty tells BTree whether this object needs to be rewritten to disk(dirty) or not
        /// </summary>
        public bool IsDirty
        {
            get { return BTreeAlgorithm != null && BTreeAlgorithm.IsDirty; }
            set { BTreeAlgorithm.IsDirty = value; }
        }

        public bool IsUnloading
        {
            get { return BTreeAlgorithm.IsUnloading; }
            set
            {
                if (BTreeAlgorithm != null &&
                    BTreeAlgorithm.IsOpen)
                    BTreeAlgorithm.IsUnloading = value;
            }
        }

        public bool IsCloned
        {
            get { return BTreeAlgorithm.IsCloned; }
            set
            {
                if (BTreeAlgorithm != null)
                    BTreeAlgorithm.IsCloned = value;
            }
        }

        /// <summary>
        /// Traverse the Parent hierarchy and look for a Parent of a given Type.
        /// Example, one can look for the "File" container of a Collection or a Parent
        /// Collection of a Collection and so on and so forth..
        /// </summary>
        /// <param name="parentType"></param>
        /// <returns></returns>
        public virtual IInternalPersistent GetParent(Type parentType)
        {
            return InternalPersistent.GetParent(Parent, parentType);
        }

        /// <summary>
        /// Register Change to Transaction so rollback can undo changes
        /// during rollback.
        /// </summary>
        public void RegisterChange(bool partialRegister = false)
        {
            BTreeAlgorithm.RegisterChange(partialRegister);
        }

        /// <summary>
        /// true if Change Registry is enabled, otherwise false.
        /// If false, any changes(add,remove) to the B-Tree collection
        /// will not roll back. This is for SOP internal use only
        /// </summary>
        public bool ChangeRegistry
        {
            get { return BTreeAlgorithm.ChangeRegistry; }
            set { BTreeAlgorithm.ChangeRegistry = value; }
        }

        /// <summary>
        /// Address on disk of current entry's data
        /// </summary>
        public long CurrentEntryDataAddress
        {
            get { return _currentEntryDataAddress; }
            set { _currentEntryDataAddress = value; }
        }

        private long _currentEntryDataAddress = -1;

        /// <summary>
        /// Parent of this object can be another Collection or File object.
        /// </summary>
        public IInternalPersistent Parent
        {
            get
            {
                if (BTreeAlgorithm != null)
                    return BTreeAlgorithm.Parent;
                return null;
            }
            set
            {
                if (BTreeAlgorithm != null)
                    BTreeAlgorithm.Parent = value;
            }
        }

        /// <summary>
        /// Returns current item's key
        /// </summary>
        public object CurrentKey
        {
            get { return BTreeAlgorithm.CurrentKey; }
        }

        /// <summary>
        /// Returns current item on disk
        /// </summary>
        public BTreeItemOnDisk CurrentItemOnDisk
        {
            get { return (BTreeItemOnDisk) BTreeAlgorithm.CurrentEntry; }
        }

        public IComparer Comparer
        {
            get { return BTreeAlgorithm.Comparer; }
            set { BTreeAlgorithm.Comparer = value; }
        }

        /// <summary>
        /// Container of this instance
        /// </summary>
        public Sop.ISortedDictionaryOnDisk Container { get; set; }

        /// <summary>
        /// Current item value
        /// </summary>
        public object CurrentValue
        {
            get { return BTreeAlgorithm.CurrentValue; }
            set { BTreeAlgorithm.CurrentValue = value; }
        }

        /// <summary>
        /// Search B-Tree for an item with Key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="goToFirstInstance">Go to 1st key instance if the key has duplicate</param>
        /// <returns></returns>
        public bool Search(object key, bool goToFirstInstance)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return BTreeAlgorithm.Search(key, goToFirstInstance);
        }

        /// <summary>
        /// Search B-Tree for an item with Key & Data Item with a specified Address
        /// </summary>
        /// <param name="key"></param>
        /// <param name="itemAddress">Address on disk of the data item to look for</param>
        /// <returns></returns>
        public bool Search(object key, long itemAddress)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (BTreeAlgorithm.Search(key, true))
            {
                do
                {
                    if (CurrentEntryDataAddress == itemAddress ||
                        itemAddress == -1)
                        return true;
                    if (!BTreeAlgorithm.MoveNext())
                        break;
                } while (Comparer.Compare(CurrentKey, key) == 0);
            }
            return false;
        }

        public bool Query(QueryExpression[] keys, out QueryResult[] values)
        {
            return BTreeAlgorithm.Query(keys, out values);
        }

        /// <summary>
        /// Search item with Key, passing false to GotoFirstInstance
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool Search(object key)
        {
            return Search(key, false);
        }

        /// <summary>
        /// Returns true if current record pointer is beyond last item in tree.
        /// </summary>
        /// <returns></returns>
        public bool EndOfTree()
        {
            return BTreeAlgorithm.CurrentEntry == null;
        }

        /// <summary>
        /// SortOrder can be ascending or descending
        /// </summary>
        public SortOrderType SortOrder
        {
            get { return BTreeAlgorithm.CurrentSortOrder; }
            set
            {
                BTreeAlgorithm.CurrentSortOrder = value;
                if (BTreeAlgorithm != null && BTreeAlgorithm.Count > 0)
                    MoveFirst();
            }
        }

        /// <summary>
        /// Save the Dictionary On Disk
        /// </summary>
        public void Flush()
        {
            if (this.ItemType == ItemType.Default &&
                BTreeAlgorithm != null &&
                BTreeAlgorithm.IsOpen)
                BTreeAlgorithm.Flush();
        }

        /// <summary>
        /// Before doing a sequential read, set this to true and false
        /// when done doing seq read. Knowing such a Hint, SOP can
        /// optimize disk I/O.
        /// </summary>
        public bool HintSequentialRead
        {
            get { return BTreeAlgorithm.HintSequentialRead; }
            set { BTreeAlgorithm.HintSequentialRead = value; }
        }

        public int HintBatchCount
        {
            get { return BTreeAlgorithm.HintBatchCount; }
            set { BTreeAlgorithm.HintBatchCount = value; }
        }

        /// <summary>
        /// Return the size on disk(in bytes) of this object
        /// </summary>
        public int HintSizeOnDisk { get; private set; }

        /// <summary>
        /// Hint: Key size on disk(in bytes)
        /// </summary>
        public int HintKeySizeOnDisk
        {
            get { return BTreeAlgorithm.HintKeySizeOnDisk; }
            set { BTreeAlgorithm.HintKeySizeOnDisk = value; }
        }

        /// <summary>
        /// Hint: Value size on disk(in bytes)
        /// </summary>
        public int HintValueSizeOnDisk
        {
            get { return BTreeAlgorithm.HintValueSizeOnDisk; }
            set { BTreeAlgorithm.HintValueSizeOnDisk = value; }
        }

        //** no need to implement as already implementing IInternalPersistent
        public void Pack(System.IO.BinaryWriter writer)
        {
            Pack(null, writer);
        }

        public void Unpack(System.IO.BinaryReader reader)
        {
            Unpack(null, reader);
        }

        /// <summary>
        /// Returns Current Sequence Number
        /// </summary>
        /// <returns></returns>
        public long CurrentSequence
        {
            get
            {
                return Interlocked.Read(ref BTreeAlgorithm.CurrentSequence);
            }
            set
            {
                Interlocked.Exchange(ref BTreeAlgorithm.CurrentSequence, value);
            }
        }

        /// <summary>
        /// Go to Next Sequence and return it
        /// </summary>
        /// <returns></returns>
        public long GetNextSequence()
        {
            return Interlocked.Increment(ref BTreeAlgorithm.CurrentSequence);
        }

        /// <summary>
        /// Serialize this Tree
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="writer"></param>
        public void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            BTreeAlgorithm.Pack(parent, writer);
        }

        /// <summary>
        /// DeSerialize this Tree
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        public void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            BTreeAlgorithm.Unpack(parent, reader);
            Initialize(this.File);
            BTreeAlgorithm.Initialize(File);
            Open();
        }

        /// <summary>
        /// User assigned data that isn't serialized to DB
        /// </summary>
        public object UserData;

        /// <summary>
        /// Clears contents of this Collection
        /// </summary>
        public void Clear()
        {
            if (File.Server.HasTrashBin)
                return;
            if (Count > 0)
            {
                //BTreeAlgorithm.Clear();
                bool saved = DataAddress >= 0;
                if (_keys != null)
                {
                    _keys.Clear();
                    _keys = null;
                }
                if (_values != null)
                {
                    _values.Clear();
                    _values = null;
                }
                if (!IsOpen)
                    throw new InvalidOperationException("Can't Clear a close SortedDictionaryOnDisk.");

                File.IFile f = File;
                IComparer cmp = Comparer;
                string name = this.Name;
                bool isDeletedList = BTreeAlgorithm.IsDeletedBlocksList;
                bool isDataLongInt = BTreeAlgorithm.IsDataLongInt;
                bool isDataInKey = BTreeAlgorithm.IsDataInKeySegment;
                PersistenceType pt = BTreeAlgorithm.PersistenceType;
                int hintKeySize = BTreeAlgorithm.HintKeySizeOnDisk;
                int hintSizeOnDisk = BTreeAlgorithm.HintSizeOnDisk;
                int hintValueSize = BTreeAlgorithm.HintValueSizeOnDisk;
                IInternalPersistent p = BTreeAlgorithm.Parent;
                Sop.Transaction.ITransactionLogger ptl = BTreeAlgorithm.ParentTransactionLogger;
                Sop.Transaction.ITransactionLogger t = BTreeAlgorithm.Transaction;
                BTreeAlgorithm.Delete();

                var b3 = new Algorithm.BTree.BTreeAlgorithm(f, cmp, name);

                DeletedBlockInfo dbi = f.DeletedCollections.Get(BTreeAlgorithm.DataAddress);

                long endSegmentAddress = dbi.StartBlockAddress + (int) DataBlockSize*f.Profile.StoreGrowthSizeInNob;
                bool resurface = false;
                if (endSegmentAddress == dbi.EndBlockAddress)
                    f.DeletedCollections.Remove(dbi.StartBlockAddress);
                else
                    resurface = true;

                b3.IsDeletedBlocksList = isDeletedList;

                b3.HintKeySizeOnDisk = hintKeySize;
                b3.HintSizeOnDisk = hintSizeOnDisk;
                b3.HintValueSizeOnDisk = hintValueSize;

                //b3.onAddressAcquired = BTreeAlgorithm.onAddressAcquired;
                b3.onInnerMemberKeyPack = BTreeAlgorithm.onInnerMemberKeyPack;
                b3.onInnerMemberKeyUnpack = BTreeAlgorithm.onInnerMemberKeyUnpack;
                b3.onInnerMemberValuePack = BTreeAlgorithm.onInnerMemberValuePack;
                b3.onInnerMemberValueUnpack = BTreeAlgorithm.onInnerMemberValueUnpack;
                b3.onKeyPack = BTreeAlgorithm.onKeyPack;
                b3.onKeyUnpack = BTreeAlgorithm.onKeyUnpack;
                b3.onValuePack = BTreeAlgorithm.onValuePack;
                b3.onValueUnpack = BTreeAlgorithm.onValueUnpack;
                b3.Parent = p;
                b3.Transaction = t;
                b3.ParentTransactionLogger = ptl;
                b3.IsDataLongInt = isDataLongInt;
                b3._IsDataInKeySegment = isDataInKey;
                b3.PersistenceType = pt;
                b3.Open();
                b3.DataAddress = dbi.StartBlockAddress;
                b3.HeaderData.StartAllocatableAddress = dbi.StartBlockAddress;
                b3.HeaderData.EndAllocatableAddress = dbi.StartBlockAddress +
                                                      (short) DataBlockSize*f.Profile.StoreGrowthSizeInNob;
                b3.HeaderData.NextAllocatableAddress = dbi.StartBlockAddress + (int) DataBlockSize;

                b3.HeaderData.OccupiedBlocksHead = b3.DataBlockDriver.CreateBlock(DataBlockSize);
                b3.HeaderData.OccupiedBlocksHead.DataAddress = dbi.StartBlockAddress;
                b3.HeaderData.OccupiedBlocksTail = b3.DataBlockDriver.CreateBlock(DataBlockSize);
                b3.HeaderData.OccupiedBlocksTail.DataAddress = dbi.StartBlockAddress;

                BTreeAlgorithm = b3;
                BTreeAlgorithm.Container = this;
                //** read next segment of deleted collection
                if (resurface)
                {
                    if (
                        !((DataBlockDriver)DataBlockDriver).ResurfaceDeletedBlockNextSegment(b3, dbi, endSegmentAddress))
                        f.DeletedCollections.Remove(dbi.StartBlockAddress);
                }
                if (saved)
                    b3.Flush();
            }
        }

        /// <summary>
        /// Delete the tree and send its blocks for recycling
        /// </summary>
        public void Delete()
        {
            if (Container != null)
            {
                Container.Remove(Name);
                Container = null;
            }
            File.RemoveFromPool(this);
            BTreeAlgorithm.Delete();
            Dispose();
        }

        ///// <summary>
        ///// On Address Acquired Event
        ///// </summary>
        //public event OnAddressAcquired OnAddressAcquired
        //{
        //    add { BTreeAlgorithm.OnAddressAcquired += value; }
        //    remove { BTreeAlgorithm.OnAddressAcquired -= value; }
        //}

        public event OnObjectUnpack OnValueUnpack
        {
            add { BTreeAlgorithm.OnValueUnpack += value; }
            remove { BTreeAlgorithm.OnValueUnpack -= value; }
        }

        public event OnObjectUnpack OnKeyUnpack
        {
            add { BTreeAlgorithm.OnKeyUnpack += value; }
            remove { BTreeAlgorithm.OnKeyUnpack -= value; }
        }

        public bool IsOnInnerMemberPackEventHandlerSet
        {
            get { return BTreeAlgorithm.onInnerMemberKeyPack != null; }
        }

        public event OnObjectPack OnInnerMemberKeyPack
        {
            add { BTreeAlgorithm.OnInnerMemberKeyPack += value; }
            remove { BTreeAlgorithm.OnInnerMemberKeyPack -= value; }
        }

        public event OnObjectUnpack OnInnerMemberKeyUnpack
        {
            add { BTreeAlgorithm.OnInnerMemberKeyUnpack += value; }
            remove { BTreeAlgorithm.OnInnerMemberKeyUnpack -= value; }
        }

        public event OnObjectPack OnInnerMemberValuePack
        {
            add { BTreeAlgorithm.OnInnerMemberValuePack += value; }
            remove { BTreeAlgorithm.OnInnerMemberValuePack -= value; }
        }

        public event OnObjectUnpack OnInnerMemberValueUnpack
        {
            add { BTreeAlgorithm.OnInnerMemberValueUnpack += value; }
            remove { BTreeAlgorithm.OnInnerMemberValueUnpack -= value; }
        }

        public event OnObjectPack OnValuePack
        {
            add { BTreeAlgorithm.OnValuePack += value; }
            remove { BTreeAlgorithm.OnValuePack -= value; }
        }

        public OnObjectPack OnKeyPackEventHandler
        {
            get { return BTreeAlgorithm.onKeyPack; }
        }

        public OnObjectUnpack OnKeyUnpackEventHandler
        {
            get { return BTreeAlgorithm.onKeyUnpack; }
        }

        public OnObjectPack OnValuePackEventHandler
        {
            get { return BTreeAlgorithm.onValuePack; }
        }

        public OnObjectUnpack OnValueUnpackEventHandler
        {
            get { return BTreeAlgorithm.onValueUnpack; }
        }

        public event OnObjectPack OnKeyPack
        {
            add { BTreeAlgorithm.OnKeyPack += value; }
            remove { BTreeAlgorithm.OnKeyPack -= value; }
        }

        public bool IsOnPackEventHandlerSet
        {
            get { return BTreeAlgorithm.IsOnPackEventHandlerSet; }
        }

        /// <summary>
        /// Move pointer to 1st item in tree
        /// </summary>
        /// <returns></returns>
        public bool MoveFirst()
        {
            if (this.SortOrder == SortOrderType.Ascending)
                return BTreeAlgorithm.MoveFirst();
            return BTreeAlgorithm.MoveLast();
        }

        /// <summary>
        /// Return Currenty Entry(Key/Value)
        /// </summary>
        public DictionaryEntry CurrentEntry
        {
            get
            {
                object k = this.CurrentKey;
                if (k != null)
                {
                    _currentEntry.Key = k;
                    _currentEntry.Value = this.CurrentValue;
                }
                else
                {
                    _currentEntry.Key = null;
                    _currentEntry.Value = null;
                }
                return _currentEntry;
            }
        }

        public IBTree Synchronized()
        {
            // TODO:  Add SortedDictionaryOnDisk.Synchronized implementation
            return null;
        }

        /// <summary>
        /// Move pointer to previous item
        /// </summary>
        /// <returns></returns>
        public bool MovePrevious()
        {
            if (this.SortOrder == SortOrderType.Ascending)
                return BTreeAlgorithm.MovePrevious();
            return BTreeAlgorithm.MoveNext();
        }

        /// <summary>
        /// Move pointer to last item
        /// </summary>
        /// <returns></returns>
        public bool MoveLast()
        {
            if (this.SortOrder == SortOrderType.Ascending)
                return BTreeAlgorithm.MoveLast();
            return BTreeAlgorithm.MoveFirst();
        }

        /// <summary>
        /// Move pointer to next item
        /// </summary>
        /// <returns></returns>
        public bool MoveNext()
        {
            if (this.SortOrder == SortOrderType.Ascending)
                return BTreeAlgorithm.MoveNext();
            return BTreeAlgorithm.MovePrevious();
        }

        bool Algorithm.Collection.ICollectionOnDisk.MoveTo(long dataAddress)
        {
            throw new SopException("Not implemented.");
        }

        /// <summary>
        /// true if dictionary is read only, otherwise false
        /// </summary>
        public bool IsReadOnly
        {
            get
            {
                // TODO:  Add SortedDictionaryOnDisk.IsReadOnly getter implementation
                return false;
            }
        }

        /// <summary>
        /// Returns an Enumerator used for traversing the tree
        /// </summary>
        /// <returns></returns>
        public IDictionaryEnumerator GetEnumerator()
        {
            return new DictionaryEnumerator(this);
        }

        /// <summary>
        /// ToString returns a globally unique name of this Collection on Disk.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (!string.IsNullOrEmpty(UniqueStoreName)) return UniqueStoreName;
            UniqueStoreName = string.Format("{0}{2}{1}", File.Filename, this.Name, System.IO.Path.DirectorySeparatorChar);
            return UniqueStoreName;
        }

        internal string GetHeaderInfo()
        {
            return this.BTreeAlgorithm.ToString();
        }

        /// <summary>
        /// Unique Store Name.
        /// </summary>
        internal string UniqueStoreName;

        /// <summary>
        /// Transaction Logger
        /// </summary>
        public Transaction.ITransactionLogger Transaction
        {
            get { return BTreeAlgorithm.Transaction; }
            set { BTreeAlgorithm.Transaction = value; }
        }

        Sop.ITransaction Sop.ICollectionOnDisk.Transaction
        {
            get { return (Sop.ITransaction) Transaction; }
            set { Transaction = (Transaction.ITransactionLogger) value; }
        }

        /// <summary>
        /// Get the Current Item's Value.
        /// NOTE: call one of the Move functions or the Search/Contains 
        /// function to position the Item pointer to the one you are interested
        /// about(Key) then call GetCurrentValue to get the Item Value
        /// </summary>
        /// <param name="target"></param>
        /// <returns></returns>
        public IPersistent GetCurrentValue(IPersistent target)
        {
            object o = CurrentValue;
            if (o == null)
            {
                if (target == null)
                    throw new ArgumentNullException("target");
                long currentValuePositionInStream = OnDiskBinaryReader.BaseStream.Position;
                ((IPersistent) target).Unpack(this.OnDiskBinaryReader);
                OnDiskBinaryReader.BaseStream.Seek(currentValuePositionInStream, System.IO.SeekOrigin.Begin);
                return target;
            }
            return o is IPersistent ? (IPersistent) o : null;
        }

        /// <summary>
        /// Get the Item's Value given a Key.
        /// </summary>
        /// <param name="key">key of entry whose value will be retrieved</param>
        /// <param name="target">target is the </param>
        /// <returns></returns>
        public IPersistent GetValue(object key, IPersistent target)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (!Search(key))
                return null;

            object o = this[key];
            if (o == null)
            {
                if (target == null)
                    throw new ArgumentNullException("target");
                long currentValuePositionInStream = OnDiskBinaryReader.BaseStream.Position;
                target.Unpack(OnDiskBinaryReader);
                OnDiskBinaryReader.BaseStream.Seek(currentValuePositionInStream, System.IO.SeekOrigin.Begin);
                if (target is SortedDictionaryOnDisk)
                    ((SortedDictionaryOnDisk) target).Container = this;
                return target;
            }
            if (o is IPersistent) //** should not occur for a User Defined Object(UDO)!
            {
                if (o is SortedDictionaryOnDisk &&
                    !((SortedDictionaryOnDisk) o).IsOpen)
                {
                    if (o is SortedDictionaryOnDisk)
                        ((SortedDictionaryOnDisk) o).Container = this;
                    ((SortedDictionaryOnDisk) o).Open();
                }
                return (IPersistent) o;
            }
            return null;
        }

        public bool Update(object key, long itemAddress, object value)
        {
            if (Search(key, true))
            {
                do
                {
                    if (CurrentEntryDataAddress == itemAddress ||
                        itemAddress == -1)
                    {
                        CurrentValue = value;
                        return true;
                    }
                    if (!MoveNext())
                        break;
                } while (Comparer.Compare(key, CurrentKey) == 0);
            }
            return false;
        }

        /// <summary>
        /// default accessor "this".
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public object this[object key]
        {
            get
            {
                if (key == null)
                    throw new ArgumentNullException("key");

                if (CurrentKey != null &&
                    Comparer != null && Comparer.Compare(CurrentKey, key) == 0)
                    return CurrentValue;

                if (this.Search(key, false))
                    return this.CurrentValue;
                BTreeAlgorithm.SetCurrentItemAddress(-1, 0);
                return null;
            }
            set
            {
                if (key == null)
                    throw new ArgumentNullException("key");
                if (!IsOpen)
                    throw new InvalidOperationException("Can't update a close SortedDictionaryOnDisk.");
                if ((CurrentKey != null &&
                     Comparer != null && Comparer.Compare(this.CurrentKey, key) == 0) ||
                    Search(key, false))
                    CurrentValue = value;
                else // if not found, add new entry/record. 
                    // NOTE: this is .net compliance feature
                    Add(key, value);
            }
        }

        public void Remove(object key, bool removeAllOccurence)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (!IsOpen)
                throw new InvalidOperationException("Can't remove from a close SortedDictionaryOnDisk.");
            BTreeAlgorithm.Remove(key, removeAllOccurence);
        }

        public bool Remove(QueryExpression[] keys, bool removeAllOccurence,
            out QueryResult[] results )
        {
            if (keys == null)
                throw new ArgumentNullException("keys");
            if (!IsOpen)
                throw new InvalidOperationException("Can't remove from a close SortedDictionaryOnDisk.");
            return BTreeAlgorithm.Remove(keys, removeAllOccurence, out results);
        }

        public bool Detach(QueryExpression key)
        {
            if (key.Key == null)
                throw new ArgumentNullException("key");
            if (!IsOpen)
                throw new InvalidOperationException("Can't remove from a close SortedDictionaryOnDisk.");
            QueryResult[] r;
            if (!Query(new QueryExpression[] { key }, out r))
                return false;

            var r2 = BTreeAlgorithm.Detach();
            File.RemoveFromPool(this);
            return r2;
        }


        /// <summary>
        /// Remove item with key from tree
        /// </summary>
        /// <param name="key"></param>
        public void Remove(object key)
        {
            Remove(key, false);
        }

        internal void ReloadRoot()
        {
            if (RootNode != null &&
                BTreeAlgorithm.RootNeedsReload)
            {
                BTreeAlgorithm.RootNeedsReload = false;
                BTreeAlgorithm.ReloadRoot();
            }
        }

        public bool RemoveInMemory(long dataAddress)
        {
            return BTreeAlgorithm.RemoveInMemory(dataAddress, null, false);
        }

        public bool RemoveInMemory(long dataAddress, Transaction.ITransactionLogger transaction)
        {
            return BTreeAlgorithm.RemoveInMemory(dataAddress, transaction);
        }

        /// <summary>
        /// Remove currently selected item from tree
        /// </summary>
        public void Remove()
        {
            if (!IsOpen)
                throw new InvalidOperationException("Can't remove from a close SortedDictionaryOnDisk.");
            BTreeAlgorithm.Remove();
        }

        /// <summary>
        /// Rename this Sorted Dictionary.
        /// NOTE: for implementation later...
        /// </summary>
        /// <param name="newName"></param>
        /// <returns></returns>
        public void Rename(string newName)
        {
            throw new NotImplementedException("Rename will be implemented later. For now, pls. Remove then re-Add w/ new Name the Store onto its Container to rename it.");
            #region for "full" implementation later...
            //if (Container != null)
            //{
            //    if (!Container.Detach(new QueryExpression
            //    {
            //        Key = Name,
            //        ValueFilterFunc = (v) =>
            //            {
            //                if (v is SpecializedDataStore.SpecializedStoreBase)
            //                {
            //                    return ((SpecializedDataStore.SpecializedStoreBase)v).Collection.DataAddress == this.DataAddress;
            //                }
            //                if (v is ISortedDictionaryOnDisk)
            //                {
            //                    return ((ISortedDictionaryOnDisk)v).DataAddress == this.DataAddress;
            //                }
            //                return false;
            //            }
            //    }))
            //    {
            //        throw new SopException(Log.Logger.Instance.Warning("Attempt to Detach store {0} failed.", Name));
            //    }
            //    Container.Remove(Name);
            //    Name = newName;
            //    IsDirty = true;
            //    Container.Add(newName, this);
            //    Flush();
            //    Container.Flush();
            //}
            //else
            //{
            //    Name = newName;
            //    IsDirty = true;
            //    Flush();
            //}
            #endregion
        }

        /// <summary>
        /// Returns the Root Node of the B-Tree
        /// </summary>
        public IBTreeNodeOnDisk RootNode
        {
            get { return BTreeAlgorithm.RootNode; }
        }

        /// <summary>
        /// Returns the Current Node of the B-Tree
        /// </summary>
        public IBTreeNodeOnDisk CurrentNode
        {
            get { return BTreeAlgorithm.CurrentNode; }
        }

        /// <summary>
        /// true if key is found, otherwise false
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool Contains(object key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return BTreeAlgorithm.Search(key);
        }

        void System.Collections.IDictionary.Clear()
        {
            BTreeAlgorithm.Clear();
        }

        private SortedDictionaryOnDisk _values;

        /// <summary>
        /// Returns Objects in the collection
        /// </summary>
        public System.Collections.ICollection Values
        {
            get
            {
                if (BTreeAlgorithm != null &&
                    (_values == null || (IsOpen && !_values.IsOpen)))
                    _values = new SortedDictionaryOnDisk(this, ItemType.Value);
                return _values;
            }
        }

        /// <summary>
        /// Returns Sop.DataBlock Driver
        /// </summary>
        public IDataBlockDriver DataBlockDriver
        {
            get { return BTreeAlgorithm.DataBlockDriver; }
        }

        /// <summary>
        /// true if collection is open, otherwise false
        /// </summary>
        public bool IsOpen
        {
            get { return BTreeAlgorithm != null && BTreeAlgorithm.IsOpen; }
        }

        /// <summary>
        /// Add Object to target store
        /// </summary>
        /// <param name="key">key of object</param>
        /// <param name="value">object to be saved</param>
        public void Add(object key, object value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (!IsOpen)
                throw new InvalidOperationException("Can't Add to a close SortedDictionaryOnDisk.");
            var itm = new BTreeItemOnDisk(BTreeAlgorithm.DataBlockSize, key, value)
                          {
                              Value =
                                  {
                                      DiskBuffer = BTreeAlgorithm.DataBlockDriver.
                                          CreateBlock(BTreeAlgorithm.DataBlockSize)
                                  }
                          };
            BTreeAlgorithm.Add(itm);
        }
        /// <summary>
        /// Add Object to target store
        /// </summary>
        /// <param name="key">key of object</param>
        /// <param name="value">object to be saved</param>
        public bool AddIfNotExist(object key, object value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (!IsOpen)
                throw new InvalidOperationException("Can't Add to a close SortedDictionaryOnDisk.");
            var itm = new BTreeItemOnDisk(BTreeAlgorithm.DataBlockSize, key, value)
            {
                Value =
                {
                    DiskBuffer = BTreeAlgorithm.DataBlockDriver.
                        CreateBlock(BTreeAlgorithm.DataBlockSize)
                }
            };
            return BTreeAlgorithm.AddIfNotExist(itm);
        }

        private SortedDictionaryOnDisk _keys;

        /// <summary>
        /// Returns the Keys of Objects in the collection
        /// </summary>
        public System.Collections.ICollection Keys
        {
            get
            {
                if (BTreeAlgorithm != null &&
                    (_keys == null || (IsOpen && !_keys.IsOpen)))
                    _keys = new SortedDictionaryOnDisk(this, ItemType.Key);
                return _keys;
            }
        }

        /// <summary>
        /// true if collection is fixed size or not (false)
        /// </summary>
        public bool IsFixedSize
        {
            get { return false; }
        }

        /// <summary>
        /// Item type BTree will store. Defaults to 'Default' Item type
        /// </summary>
        internal Collections.BTree.ItemType ItemType = Collections.BTree.ItemType.Default;

        public bool IsSynchronized
        {
            get { return false; }
        }

        /// <summary>
        /// Returns the count of Objects in collection
        /// </summary>
        public long Count
        {
            get { return this.BTreeAlgorithm.Count; }
        }
        int System.Collections.ICollection.Count
        {
            get { return (int)Count; }
        }

        public void CopyTo(Array destArray, int index)
        {
            if (destArray == null)
                throw new ArgumentNullException("destArray");
            if (this.EndOfTree())
                return;
            int i = index;
            HintSequentialRead = true;
            if (ItemType == ItemType.Key)
            {
                foreach (object o in Keys)
                {
                    if (i >= destArray.Length)
                        break;
                    destArray.SetValue(o, i++);
                }
            }
            else if (ItemType == ItemType.Value)
            {
                foreach (object o in Values)
                {
                    if (i >= destArray.Length)
                        break;
                    destArray.SetValue(o, i++);
                }
            }
            else
            {
                if (MoveFirst())
                {
                    do
                    {
                        destArray.SetValue(this.CurrentEntry, i++);
                    } while (MoveNext() && i < destArray.Length);
                }
            }
        }

        public ISynchronizer Locker
        {
            get
            {
                return ((ISynchronizer)SyncRoot);
            }
        }

        public object SyncRoot
        {
            get
            {
                if (_syncRoot == null && BTreeAlgorithm != null)
                    _syncRoot = BTreeAlgorithm.SyncRoot;
                return _syncRoot;
            }
            private
            set
            {
                _syncRoot = value;
            }
        }
        private object _syncRoot;

        IEnumerator IEnumerable.GetEnumerator()
        {
            var r = new DictionaryEnumerator(this);
            return r;
        }

        /// <summary>
        /// Make another copy of this collection,
        /// e.g. - useful for implementing enumerators
        /// </summary>
        /// <returns></returns>
        public object Clone()
        {
            return new SortedDictionaryOnDisk(this, this.ItemType);
        }

        public Sop.Collections.Generic.ISortedDictionary<long, Sop.DataBlock> Blocks
        {
            get { return BTreeAlgorithm.Blocks; }
            set { BTreeAlgorithm.Blocks = value; }
        }

        internal Algorithm.BTree.BTreeAlgorithm BTreeAlgorithm;

        /// <summary>
        /// Returns the OnDiskBinaryReader
        /// </summary>
        public OnDiskBinaryReader OnDiskBinaryReader
        {
            get { return BTreeAlgorithm.OnDiskBinaryReader; }
        }

        /// <summary>
        /// DataAddress of the collection on disk
        /// </summary>
        public long DataAddress
        {
            get
            {
                if (BTreeAlgorithm == null)
                    return -1;
                return BTreeAlgorithm.DataAddress;
            }
            set { BTreeAlgorithm.DataAddress = value; }
        }

        /// <summary>
        /// Load the Collection from disk. NOTE: items are not loaded
        /// to memory, but just the meta info so client code can iterate
        /// items, get or do management action(add, delete)
        /// </summary>
        public void Load()
        {
            BTreeAlgorithm.Load();
        }

        public void Reload()
        {
            BTreeAlgorithm.IsDirty = false;
            BTreeAlgorithm.Close();
            BTreeAlgorithm.Open();
        }

        /// <summary>
        /// Open the collection if not opened yet
        /// </summary>
        public void Open()
        {
            if (Parent == null)
            {
                File.AddToPool(this);
                if (CollectionOnDisk.Session != null)
                    CollectionOnDisk.Session.Register(this);
            }
            BTreeAlgorithm.Open();
        }

        public int InMemoryId
        {
            get { return BTreeAlgorithm.InMemoryId; }
        }

        void IInternalFileEntity.CloseStream()
        {
            ((IInternalFileEntity) BTreeAlgorithm).CloseStream();
        }

        void IInternalFileEntity.OpenStream()
        {
            ((IInternalFileEntity) BTreeAlgorithm).OpenStream();
        }

        /// <summary>
        /// Close the collection if not yet closed
        /// </summary>
        public void Close()
        {
            if (IsOpen)
            {
                File.RemoveFromPool(this);
                if (Parent == null)
                {
                    if (CollectionOnDisk.Session != null)
                        CollectionOnDisk.Session.UnRegister(this);
                }
                BTreeAlgorithm.Close();
            }
        }

        public void OnCommit()
        {
            BTreeAlgorithm.OnCommit();
        }

        public void OnRollback()
        {
            BTreeAlgorithm.OnRollback();
        }

        public void SetCurrentValueInMemoryData(object value)
        {
            if (BTreeAlgorithm.CurrentEntry == null)
            {
                if (value is Sop.ISortedDictionary)
                {
                    string key = ((SortedDictionaryOnDisk) ((Sop.ISortedDictionary) value).RealObject).Name;
                    if (!BTreeAlgorithm.Search(key))
                        throw new SopException(string.Format("Can't update Current Value in memory, key {0} not found in store {1}.", key, BTreeAlgorithm.Name));
                }
                else
                    throw new SopException(string.Format("Can't update Current Value in memory, SOP can't extract key from value of type {0}, your code needs to Search for the item with your known key.", value.GetType().ToString()));
            }
            BTreeAlgorithm.CurrentNode.Slots[BTreeAlgorithm.CurrentItem.NodeItemIndex].Value.Data = value;
        }

        /// <summary>
        /// Returns the ID of the collection on disk
        /// </summary>
        /// <returns></returns>
        public long GetId()
        {
            return BTreeAlgorithm.GetId();
        }

        /// <summary>
        /// Update the ID of the collection on disk
        /// </summary>
        /// <param name="address"></param>
        public void SetId(long address)
        {
            BTreeAlgorithm.SetId(address);
        }

        /// <summary>
        /// Name of the collection
        /// </summary>
        public string Name
        {
            get { return BTreeAlgorithm == null ? null : BTreeAlgorithm.Name; }
            set { BTreeAlgorithm.Name = value; }
        }

        /// <summary>
        /// File where collection lives
        /// </summary>
        public File.IFile File
        {
            get
            {
                if (BTreeAlgorithm != null)
                    return BTreeAlgorithm.File;
                return null;
            }
            set
            {
                if (BTreeAlgorithm != null)
                    BTreeAlgorithm.File = value;
            }
        }

        Sop.IFile Sop.ICollectionOnDisk.File
        {
            get { return File; }
            set { File = (File.IFile) value; }
        }

        /// <summary>
        /// FileStream of the collection
        /// </summary>
        public FileStream FileStream
        {
            get
            {
                return BTreeAlgorithm != null ? BTreeAlgorithm.FileStream : null;
            }
            set { BTreeAlgorithm.FileStream = value; }
        }

        object Algorithm.Collection.ICollectionOnDisk.CurrentEntry
        {
            get { return BTreeAlgorithm.CurrentEntry; }
        }

        /// <summary>
        /// Implement to return the number of bytes this persistent object will occupy in Persistence stream.
        /// Being able to return the size before writing the object's data bytes to stream is optimal
        /// for the "Packager". Implement this property if possible, else, implement and return -1 to tell
        /// the Packager the size is not available before this object is allowed to persist or save its data.
        /// </summary>
        public int Size
        {
            get { return -1; }
        }

        /// <summary>
        /// DiskBuffer of collection
        /// </summary>
        public Sop.DataBlock DiskBuffer
        {
            get { return BTreeAlgorithm.DiskBuffer; }
            set { BTreeAlgorithm.DiskBuffer = value; }
        }

        /// <summary>
        /// Index blocksize
        /// </summary>
        public DataBlockSize IndexBlockSize
        {
            get { return BTreeAlgorithm.IndexBlockSize; }
            set { BTreeAlgorithm.IndexBlockSize = value; }
        }

        /// <summary>
        /// Data Blocksize
        /// </summary>
        public DataBlockSize DataBlockSize
        {
            get { return BTreeAlgorithm.DataBlockSize; }
        }

        /// <summary>
        /// Auto Dispose Item when this Store removes it from Cache (and saves to disk) or when the item gets deleted.
        /// </summary>
        public bool AutoDisposeItem
        {
            get { return BTreeAlgorithm.AutoDisposeItem; }
            set { BTreeAlgorithm.AutoDisposeItem = value; }
        }
        /// <summary>
        /// Auto Dispose this Store when it gets removed from the container's cache.
        /// </summary>
        public bool AutoDispose
        {
            get { return BTreeAlgorithm != null && BTreeAlgorithm.AutoDispose; }
            set { BTreeAlgorithm.AutoDispose = value; }
        }
        /// <summary>
        /// If true and Data Value is not stored in Key segment,
        /// will cause insert/update of the Big Data Value to disk during 
        /// Add or Update of a record to the Store.
        /// </summary>
        public bool AutoFlush
        {
            get { return BTreeAlgorithm != null && BTreeAlgorithm.AutoFlush; }
            set { BTreeAlgorithm.AutoFlush = value; }
        }
        private DictionaryEntry _currentEntry;
   }
}
