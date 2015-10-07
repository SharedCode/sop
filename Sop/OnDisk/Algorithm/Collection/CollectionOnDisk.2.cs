// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using Sop.Mru;
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.OnDisk.DataBlock;
using Sop.OnDisk.File;
using Sop.OnDisk.IO;
using Sop.Persistence;
using Sop.Recycling;
using System.Threading;

namespace Sop.OnDisk.Algorithm.Collection
{
    internal abstract partial class CollectionOnDisk
    {
        /// <summary>
        /// Returns the Current Sop.DataBlock with option not to put it on cache
        /// </summary>
        /// <param name="getForRemoval"></param>
        /// <returns></returns>
        protected internal Sop.DataBlock GetCurrentDataBlock(bool getForRemoval)
        {
            if (currentDataBlock == null || DataBlockDriver.GetId(currentDataBlock) == -1)
            {
                currentDataBlock = DataBlockDriver.ReadBlockFromDisk(this, getForRemoval);
            }
            return currentDataBlock;
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
        /// Returns the Current Sop.DataBlock
        /// </summary>
        protected internal Sop.DataBlock CurrentDataBlock
        {
            get
            {
                Sop.DataBlock d = GetCurrentDataBlock();
                return d; // GetCurrentDataBlock();
            }
        }

        /// <summary>
        /// get/set Collection's FileStream
        /// </summary>
        public virtual FileStream FileStream
        {
            get
            {
                var pp = Parent as CollectionOnDisk;
                if (pp != null)
                {
                    if (!IsDeletedBlocksList || pp.IsDeletedBlocksList)
                        return pp.FileStream;
                }
                return _fileStream;
            }
            set
            {
                if (!IsDeletedBlocksList)
                {
                    if (Parent is CollectionOnDisk)
                        ((CollectionOnDisk)Parent).FileStream = value;
                    else
                        _fileStream = value;
                }
                else
                {
                    if (Parent is CollectionOnDisk &&
                        ((CollectionOnDisk)Parent).IsDeletedBlocksList)
                        ((CollectionOnDisk)Parent).FileStream = value;
                    else
                        _fileStream = value;
                }
            }
        }

        internal volatile bool isOpen;

        /// <summary>
        /// Returns true if this Collection is open, otherwise false
        /// </summary>
        public virtual bool IsOpen
        {
            get { return isOpen; }
        }

        private bool _isCloned;

        public bool IsCloned
        {
            get
            {
                var collectionOnDisk = Parent as ICollectionOnDisk;
                return collectionOnDisk != null ? (collectionOnDisk).IsCloned : _isCloned;
            }
            set
            {
                if (Parent is ICollectionOnDisk)
                    ((ICollectionOnDisk)Parent).IsCloned = value;
                else
                    _isCloned = value;
            }
        }

        #region Cache Pool related
        private void SetupCachePool()
        {
            CachePoolManager.Initialize(File.Profile.MruMinCapacity, File.Profile.MruMaxCapacity);
        }
        private void ReuseCacheFromPool()
        {
            // todo: include InstanceId to the cacheId...
            string cacheId = string.Format("{0}{1}", File.Filename, GetId());
            ICollectionCache cache = CachePoolManager.GetCache(cacheId);
            if (cache == null)
                CachePoolManager.SetCache(cacheId, this);
            else
            {
                MruManager = cache.MruManager;
                Blocks = cache.Blocks;
                MruManager.SetDataStores(this, DataBlockDriver);
            }
        }
        private static readonly CachePoolManager CachePoolManager = new CachePoolManager();
        #endregion

        /// <summary>
        /// Open the Collection
        /// </summary>
        public virtual void Open()
        {
            SetupCachePool();

            if (DataBlockDriver == null)
                throw new InvalidOperationException(
                    "DataBlockDriver is null. Make sure you have assigned valid File 'Parent'"
                    );
            if (OnDiskBinaryWriter == null && File.Server != null)
            {
                OnDiskBinaryWriter = new OnDiskBinaryWriter(File.Server.Encoding);
                OnDiskBinaryReader = new OnDiskBinaryReader(File.Server.Encoding);
            }
            if (isOpen) return;
            long fileSize = 0;
            if (FileStream == null)
            {
                int systemDetectedBlockSize;
                FileStream = File.UnbufferedOpen(out systemDetectedBlockSize);
                if (FileStream != null &&
                    File.Size < (fileSize = FileStream.Length))
                {
                    short ss = (short)DataBlockSize;
                    long segmentSize = File.StoreGrowthSizeInNob * ss;
                    long segmentCount = fileSize / segmentSize;
                    if (fileSize % segmentSize != 0 || segmentCount == 0)
                        segmentCount++;
                    File.Size = segmentCount * segmentSize;
                }
            }
            isOpen = true;
            //** read the header if there is one...
            if (DiskBuffer == null) return;
            if (fileSize == 0)
                fileSize = FileStream.Length;
            if (DataBlockDriver.GetId(DiskBuffer) >= 0 && fileSize > 0)
            {
                if (deletedBlocks != null)
                    deletedBlocks.Open();
                Load();
                ReuseCacheFromPool();
                IsDirty = false;
            }
            else
            {
                //** write header into 1st block
                if (fileSize == 0 && File.Store.IsItMe(this))
                {
                    bool shouldGenerateZeroAddress = false;
                    if (File.Server != null)
                    {
                        if (DiskBuffer.DataAddress == File.DiskBuffer.DataAddress)
                        {
                            DiskBuffer.DataAddress = -1;
                            shouldGenerateZeroAddress = true;
                        }
                        Flush();
                        IsDirty = DataAddress == -1;
                    }
                    if (shouldGenerateZeroAddress && DiskBuffer.DataAddress != 0)
                        throw new InvalidOperationException(
                            "Didn't allocate the 1st block(DataAddress=0) on collection's DiskBuffer.");
                }
            }
        }

        void IInternalFileEntity.CloseStream()
        {
            CloseStream();
        }

        void IInternalFileEntity.OpenStream()
        {
            OpenStream();
        }

        protected internal virtual void CloseStream()
        {
            if (IsOpen)
            {
                if (deletedBlocks is IInternalFileEntity)
                    ((IInternalFileEntity)deletedBlocks).CloseStream();
            }
            if (_fileStream == null) return;
            _fileStream.Close();
            _fileStream = null;
        }

        protected internal virtual void OpenStream()
        {
            if (FileStream == null)
            {
                int systemDetectedBlockSize;
                FileStream = File.UnbufferedOpen(out systemDetectedBlockSize);
            }
            if (!(deletedBlocks is IInternalFileEntity)) return;
            ((IInternalFileEntity)deletedBlocks).OpenStream();
        }

        /// <summary>
        /// Close the Collection
        /// </summary>
        public virtual void Close()
        {
            bool miscCollsClosed = false;
            if (IsOpen)
            {
                if (!IsCloned)
                    OnCommit();
                if (IsDirty && !IsCloned && DataAddress >= 0 && Count > 0)
                    Flush();
                if (deletedBlocks != null)
                    deletedBlocks.Close();
                miscCollsClosed = true;
                if (DataBlockDriver != null)
                {
                    if (!IsCloned)
                    {
                        //DataBlocksPool.RemoveFromPool(this.DataBlockDriver.MruManager);
                        //this.DataBlockDriver.MruManager.Clear();
                        DataBlockDriver.HeaderData.Clear();
                    }
                }
                if (Parent == null)
                {
                    if (OnDiskBinaryWriter != null)
                    {
                        OnDiskBinaryWriter.Close();
                        OnDiskBinaryWriter = null;
                    }
                    if (OnDiskBinaryReader != null)
                    {
                        OnDiskBinaryReader.Close();
                        OnDiskBinaryReader = null;
                    }
                }
                if (Blocks != null)
                    Blocks.Clear();
                currentDataBlock = null;
                currentEntry = null;
                _currentEntryDataAddress = -1;
                isOpen = false;
            }
            if (_fileStream != null)
            {
                _fileStream.Close();
                _fileStream = null;
            }
            if (!miscCollsClosed)
            {
                if (deletedBlocks != null)
                    deletedBlocks.Close();
            }
            if (_isUnloading)
                _isUnloading = false;
            long da = DiskBuffer.DataAddress;
            DiskBuffer.Initialize();
            DiskBuffer.DataAddress = da;
        }

        #region ICollection Members

        public bool IsSynchronized
        {
            get
            {
                // TODO:  Add CollectionOnDisk.IsSynchronized getter implementation
                return false;
            }
        }

        /// <summary>
        /// Delete & Dispose this collection.
        /// Send all allocated segments:blocks to recycle bin
        /// </summary>
        public virtual void Delete()
        {
            if (DataBlockDriver != null)
            {
                DataBlockDriver.Delete(this);
                CollectionOnDisk codParent = GetTopParent();
                if (codParent.deletedBlocks != null &&
                    codParent.deletedBlocks.DataAddress >= 0)
                {
                    IDataBlockRecycler dc = codParent.deletedBlocks;
                    codParent.deletedBlocks = null;
                    dc.Delete();
                }
            }
            Dispose();
        }

        /// <summary>
        /// Clear contents of this Collection.
        /// Changes will be saved right after clearing the contents.
        /// </summary>
        public virtual void Clear()
        {
            if (HeaderData.OccupiedBlocksHead != null && HeaderData.OccupiedBlocksTail != null)
            {
                HeaderData.DiskBuffer.IsDirty = true;
                HeaderData.OccupiedBlocksTail.DataAddress =
                    HeaderData.NextAllocatableAddress =
                    HeaderData.StartAllocatableAddress = HeaderData.OccupiedBlocksHead.DataAddress;

                int segmentSize = File.GetSegmentSize();
                if (HeaderData.OccupiedBlocksHead.DataAddress + segmentSize != HeaderData.EndAllocatableAddress)
                {
                    //**** add to File.DeletedCollections the next _region for reuse...
                    //** read next segment of deleted collection
                    var dbi = new DeletedBlockInfo();
                    Sop.DataBlock db;
                    db = DataBlockDriver.ReadBlockFromDisk(this,
                                                           HeaderData.OccupiedBlocksHead.DataAddress + segmentSize -
                                                           (int)File.DataBlockSize, true);
                    if (db.InternalNextBlockAddress >= 0)
                    {
                        dbi.StartBlockAddress = db.InternalNextBlockAddress;
                        dbi.EndBlockAddress = db.InternalNextBlockAddress + segmentSize;

                        if (File.DeletedCollections != null)
                        {
                            bool oc = ((CollectionOnDisk)File.DeletedCollections).ChangeRegistry;
                            ((CollectionOnDisk)File.DeletedCollections).ChangeRegistry = ChangeRegistry;
                            File.DeletedCollections.Add(dbi);
                            ((CollectionOnDisk)File.DeletedCollections).ChangeRegistry = oc;
                        }

                        HeaderData.EndAllocatableAddress = HeaderData.OccupiedBlocksHead.DataAddress + segmentSize;
                    }
                }
            }
            HeaderData.Count = 0;
            if (HeaderData.diskBuffer != null)
                HeaderData.diskBuffer.ClearData();
            if (deletedBlocks != null)
                deletedBlocks.Clear();
            //if (MruManager != null)
            //    MruManager.Clear();
            //if (this.DataBlockDriver != null)
            //    this.DataBlockDriver.MruManager.Clear();
            //Blocks.Clear();
            currentDataBlock = null;
            currentEntry = null;
            _currentEntryDataAddress = -1;
            RegisterChange();
            Flush();
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
        /// Returns the topmost parent
        /// </summary>
        /// <returns></returns>
        public CollectionOnDisk GetTopParent()
        {
            if (Parent is CollectionOnDisk)
                return ((CollectionOnDisk)Parent).GetTopParent();
            return this;
        }

        /// <summary>
        /// Parent of this object can be another Collection or File object.
        /// </summary>
        public virtual IInternalPersistent Parent { get; set; }

        /// <summary>
        /// Returns true if Value is in Binary Reader Stream (OnDiskBinaryReader),
        /// otherwise false
        /// </summary>
        public bool IsValueInStream
        {
            get
            {
                if (Count > 0)
                    return OnDiskBinaryReader.BaseStream.Position < OnDiskBinaryReader.BaseStream.Length;
                return false;
            }
        }

        /// <summary>
        /// Returns the Count of Items in the Collection
        /// </summary>
        public virtual long Count
        {
            get
            {
                return DataBlockDriver == null ? 0 : DataBlockDriver.HeaderData.Count;
            }
        }
        int ICollection.Count
        {
            get
            {
                return (int)Count;
            }
        }

        protected internal virtual long UpdateCount(UpdateCountType updateType)
        {
            if (updateType == UpdateCountType.Increment)
                return Interlocked.Increment(ref DataBlockDriver.HeaderData._count);
            else
                return Interlocked.Decrement(ref DataBlockDriver.HeaderData._count);
        }

        public virtual void CopyTo(Array destArray, int startIndex)
        {
            throw new SopException("The method or operation is not implemented.");
        }

        /// <summary>
        /// Returns a Collection (a.k.a. - Store) Synchronizer object.
        /// This is used for synchronizing access to the Collection in a threaded env't.
        /// </summary>
        public object SyncRoot
        {
            get
            {
                if (_syncRoot == null)
                    _syncRoot = new Sop.Collections.Synchronizer();
                return _syncRoot;
            }
            set { _syncRoot = value; }
        }
        private object _syncRoot;

        #endregion

        #region IEnumerable Members

        public virtual IEnumerator GetEnumerator()
        {
            return null;
        }

        #endregion

        /// <summary>
        /// OnDiskBinaryWriter is used for persisting Data values to target
        /// Data Block
        /// </summary>
        public OnDiskBinaryWriter OnDiskBinaryWriter = null;

        /// <summary>
        /// OnDiskBinaryReader is used for Reading Data value from target
        /// Data Block
        /// </summary>
        public OnDiskBinaryReader OnDiskBinaryReader = null;

        internal object currentEntry = null;
        internal Sop.DataBlock currentDataBlock = null;
        private FileStream _fileStream = null;

        /// <summary>
        /// Return the size on disk(in bytes) of this object
        /// </summary>
        public int HintSizeOnDisk { get; internal set; }

        /// <summary>
        /// Serialize this Collection meta info
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="writer"></param>
        public virtual void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            writer.Write(DataAddress);
            writer.Write((int)DataBlockSize);
            writer.Write(Name);
            writer.Write(HintSizeOnDisk);
            if (DataBlockDriver == null)
                writer.Write(DiskBuffer.DataAddress);
            else
                writer.Write(DataBlockDriver.GetId(this.DiskBuffer));
            bool hasHeader = HeaderData != null;
            writer.Write(hasHeader);
            if (hasHeader)
                HeaderData.Pack(parent, writer);

            bool hasDeletedBlocks = deletedBlocks != null;
            writer.Write(hasDeletedBlocks);
            if (hasDeletedBlocks)
                writer.Write(deletedBlocks.DiskBuffer.DataAddress);
        }

        private long _deletedBlocksAddress = -1;

        /// <summary>
        /// DeSerialize this Collection's Meta Info
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        public virtual void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            long da = reader.ReadInt64();
            if (da >= 0)
                DataAddress = da;
            DataBlockSize = (DataBlockSize)reader.ReadInt32();
            Name = reader.ReadString();
            HintSizeOnDisk = reader.ReadInt32();
            long l = reader.ReadInt64();
            if (l >= 0)
                DataBlockDriver.SetId(this.DiskBuffer, l);
            bool hasHeader = reader.ReadBoolean();
            if (hasHeader)
            {
                if (HeaderData == null)
                    HeaderData = new HeaderData();
                HeaderData.Unpack(parent, reader);
            }

            MruMinCapacity = ((CollectionOnDisk)parent).File.Profile.MruMinCapacity;
            MruMaxCapacity = ((CollectionOnDisk)parent).File.Profile.MruMaxCapacity;

            bool hasDeletedBlocks = reader.ReadBoolean();
            if (hasDeletedBlocks)
                _deletedBlocksAddress = reader.ReadInt64();
            else if (deletedBlocks != null)
                deletedBlocks = null;
        }

        /// <summary>
        /// Objects MRU cache manager
        /// </summary>
        public IMruManager MruManager { get; set; }

        /// <summary>
        /// Reads into memory the Collection On Disk's Header Block which contains
        /// state information of the collection
        /// </summary>
        public virtual void Load()
        {
            if (DiskBuffer == null)
                throw new InvalidOperationException("'DiskBuffer' is null.");
            if (DataBlockDriver.GetId(DiskBuffer) < 0)
                throw new InvalidOperationException("'DiskBuffer.DataAddress' is < 0.");

            //Blocks.Clear();
            //if (DataBlockDriver.MruManager != null)
            //    DataBlockDriver.MruManager.Clear();
            //if (MruManager != null)
            //    MruManager.Clear();

            currentDataBlock = null;
            DataBlockDriver.MoveTo(this, DataBlockDriver.GetId(DiskBuffer));

            Sop.DataBlock block = GetCurrentDataBlock();

            if (block.SizeOccupied > 0)
            {
                if (block.DataAddress == DiskBuffer.DataAddress &&
                    DiskBuffer.Data == null)
                    DiskBuffer = block;
                ReadFromBlock(block, this);
                DiskBuffer = block;
            }
            if (IsDirty)
                IsDirty = false;

            //** allow deleted blocks to be loaded its Header and clear its MRU cache...
            if (File.Server.HasTrashBin && _deletedBlocksAddress >= 0) // && deletedBlocks.DataAddress >= 0)
            {
                // ensure Deleted Blocks collection is loaded & initialized.
                var o = DeletedBlocks;
                //if (DeletedBlocksAddress != DeletedBlocks.DataAddress)
                //    DeletedBlocks.DataAddress = DeletedBlocksAddress;
                deletedBlocks.Load();
            }
        }

        private int _registerCallCount = 0;

        /// <summary>
        /// ChangeRegistry - true will enable registry of changes to collection,
        /// false will disable.
        /// </summary>
        public virtual bool ChangeRegistry { get; set; }

        /// <summary>
        /// Override ToString to return the Name of the Collection
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
#if (DEBUG)
            return string.Format("Name: {0} Address: {1}", Name, DataAddress);
#endif
            return Name;
        }

        [ThreadStatic]
        public static ISession Session;

        private Transaction.ITransactionLogger _instanceTransaction;
        [ThreadStatic]
        internal static Transaction.ITransactionLogger transaction;

        /// <summary>
        /// Transaction log
        /// </summary>
        public Transaction.ITransactionLogger Transaction
        {
            get
            {
                if (_instanceTransaction != null &&
                    (int)_instanceTransaction.CurrentCommitPhase >= (int)Sop.Transaction.CommitPhase.SecondPhase)
                    _instanceTransaction = null;
                if (transaction != null &&
                    (int)transaction.CurrentCommitPhase >= (int)Sop.Transaction.CommitPhase.SecondPhase)
                    transaction = null;

                if (_instanceTransaction == null)
                {
                    if (transaction is Transaction.Transaction &&
                        ((Transaction.Transaction)transaction).Server == File.Server)
                        Transaction = transaction;
                    //_instanceTransaction = transaction;
                }

                if (_instanceTransaction == null &&
                    File.Server.Transaction != null &&
                    ((Sop.Transaction.TransactionBase)File.Server.Transaction).Children != null &&
                    ((Sop.Transaction.TransactionBase)File.Server.Transaction).Children.Count > 0)
                {
                    //transaction = _instanceTransaction = ((Sop.Transaction.TransactionBase)File.Server.Transaction).Children[0];
                    Transaction = ((Sop.Transaction.TransactionBase)File.Server.Transaction).Children[0];
                }

                return _instanceTransaction;
            }
            set
            {
                if (_instanceTransaction != value)
                {
                    if (value != null &&
                        _instanceTransaction != null &&
                        (int)_instanceTransaction.CurrentCommitPhase < (int)Sop.Transaction.CommitPhase.Committed)
                        throw new SopException(
                            "Can't assign another transaction, there is already a transaction assigned. Complete the transaction before assigning a new one.");

                    transaction = _instanceTransaction = value;
                    if (HeaderData == null)
                        return;
                    HeaderData.RecycledSegmentBeforeTransaction = HeaderData.RecycledSegment;
                    if (HeaderData.RecycledSegmentBeforeTransaction != null)
                        HeaderData.RecycledSegmentBeforeTransaction =
                            (DeletedBlockInfo)HeaderData.RecycledSegmentBeforeTransaction.Clone();
                }
            }
        }

        ITransaction Sop.ICollectionOnDisk.Transaction
        {
            get { return Transaction; }
            set { Transaction = (Transaction.ITransactionLogger)value; }
        }

        protected internal virtual SaveTypes SaveState { get; set; }

        private bool IsPartOfCollection
        {
            get { return HeaderData == null; }
        }

        /// <summary>
        /// Flush all "modified" data blocks in cache onto Disk.
        /// NOTE: cache fault event will also cause modified data blocks 
        /// to be saved to disk and Save will only save those "modified ones" 
        /// that didn't get evicted from cache during said event.
        /// </summary>
        public virtual void Flush()
        {
            bool saveIt = false;
            bool saveDelayed = DelaySaveBlocks;
            if (IsOpen && IsDirty)
            {
                //RegisterChange();
                IsDirty = false;
                if (FileStream != null)
                {
                    //** save the collection Item(s) in cache
                    SaveState |= SaveTypes.CollectionSave;
                    if (!DelaySaveBlocks)
                        DelaySaveBlocks = MruManager.Count < 20;
                    MruManager.Flush();
                    SaveState ^= SaveTypes.CollectionSave;

                    //** save the collection's meta data
                    WriteToDisk(this, !IsPartOfCollection);
                    saveIt = true;
                    AddToBlocks(DiskBuffer, Blocks);
                }
            }
            if (deletedBlocks != null)
                deletedBlocks.Flush();
            if (!saveIt) return;
            DelaySaveBlocks = saveDelayed;
            SaveBlocks(false);
        }

        public virtual void OnCommit()
        {
            if (deletedBlocks != null)
                deletedBlocks.OnCommit();
            HeaderData.IsModifiedInTransaction = false;
        }

        public virtual void OnRollback()
        {
            if (HeaderData != null)
                HeaderData.IsModifiedInTransaction = false;
            if (deletedBlocks != null)
                deletedBlocks.OnRollback();
            if (_blocks != null)
                _blocks.Clear();
        }

        protected virtual int SaveBlocks(bool clear)
        {
            return SaveBlocks(this, 1, clear);
        }

        protected virtual int SaveBlocks(int maxBlocks, bool clear)
        {
            return SaveBlocks(this, maxBlocks, clear);
        }

        protected int SaveBlocks(ICollectionOnDisk parent, bool clear)
        {
            return SaveBlocks(parent, 1, clear);
        }

        protected virtual void WriteBlocksToDisk(ICollectionOnDisk parent,
                                                 IDictionary<long, Sop.DataBlock> blocks, bool clear)
        {
            DataBlockDriver.WriteBlocksToDisk(parent, blocks, clear);
        }

        protected internal void RemoveBlock(long id)
        {
            Blocks.Remove(id);
        }

        protected bool DelaySaveBlocks = false;

        protected internal virtual int SaveBlocks(ICollectionOnDisk parent, int maxBlocks, bool clear)
        {
            int r = 0;
            if (parent != null && parent.FileStream != null)
            {
                if (!_inSaveBlocks && !DelaySaveBlocks)
                {
                    _inSaveBlocks = true;
                    r = Blocks.Count;
                    if (Blocks.Count >= maxBlocks)
                    {
                        WriteBlocksToDisk(parent, Blocks, clear);
                        if (DataBlockDriver.BlockRecycler == null)
                        {
                            ((DataBlockDriver)DataBlockDriver).BlockRecycler =
                                new DataBlockRecycler(File.Profile.MaxInMemoryBlockCount);
                            ((DataBlockRecycler)((DataBlockDriver)DataBlockDriver).BlockRecycler).PreAllocateBlocks(DataBlockSize);
                        }
                        DataBlockDriver.BlockRecycler.Recycle(Blocks.Values);
                        Blocks.Clear();
                    }
                    _inSaveBlocks = false;
                }
            }
            return r;
        }

        /// <summary>
        /// Register current collection's state
        /// </summary>
        public virtual void RegisterChange(bool partialRegister = false)
        {
            IsDirty = true;
            _registerCallCount++;
            if (_registerCallCount == 1)
            {
                if (DiskBuffer == null)
                    throw new InvalidOperationException("'DiskBuffer' is null.");

                bool stateSerialized = false;
                if (!partialRegister || DiskBuffer.SizeOccupied == 0)
                {
                    stateSerialized = true;
                    DiskBuffer.ClearData();
                    OnDiskBinaryWriter.WriteObject(File, this, DiskBuffer);
                }
                if (!ChangeRegistry)
                {
                    _registerCallCount = 0;
                    if (!partialRegister || stateSerialized)
                        Blocks.Add(DataBlockDriver.GetId(DiskBuffer), DiskBuffer);  //90;
                    //DataBlockDriver.MruManager.Add(DataBlockDriver.GetId(DiskBuffer), DiskBuffer);
                }
                else
                {
                    DataBlockDriver.SetDiskBlock(this, DiskBuffer, true);
                    if (_registerCallCount > 1)
                    {
                        _registerCallCount = 0;
                        //IsDirty = true;
                        RegisterChange(partialRegister);
                    }
                    _registerCallCount = 0;
                }
            }
        }

        /// <summary>
        /// Address of this Collection on disk or on virtual store
        /// </summary>
        public virtual long DataAddress
        {
            get { return _dataAddress; }
            set
            {
                _dataAddress = value;
                if (_diskBuffer != null)
                    _diskBuffer.DataAddress = value;
            }
        }

        private long _dataAddress = -1;
        private Sop.DataBlock _diskBuffer;

        /// <summary>
        /// Default implementation is to retrieve the disk buffer from MRU,
        /// override if needed to read/save data from/to Disk if not in MRU
        /// </summary>
        public Sop.DataBlock DiskBuffer
        {
            get
            {
                Sop.DataBlock d = _diskBuffer;
                if (d == null)
                {
                    d = CreateBlock();  // new Sop.DataBlock(DataBlockSize);
                    _diskBuffer = d;
                    DataBlockDriver.SetId(d, _dataAddress);
                }
                else
                {
                    _dataAddress = DataBlockDriver == null ? d.DataAddress : DataBlockDriver.GetId(d);
                }
                return d;
            }
            set
            {
                if (value == null)
                    throw new ArgumentNullException("value");
                _dataAddress = DataBlockDriver.GetId(value);
                _diskBuffer = value;
            }
        }

        /// <summary>
        /// Sop.DataBlock Size
        /// </summary>
        public DataBlockSize DataBlockSize { get; protected internal set; }

        #region deleted blocks related
        public long DeletedBlocksCount
        {
            get
            {
                long c = 0;
                if (DeletedBlocks != null)
                    c = DeletedBlocks.Count;
                if (File.DeletedCollections != null &&
                    File.DeletedCollections != this &&
                    File.DeletedCollections != this.Parent)
                    c += File.DeletedCollections.Count;
                return c;
            }
        }

        /// <summary>
        /// Get a deleted block from Collection 'block recycle bin' or from File 'collection recycle bin'.
        /// </summary>
        /// <param name="requestedBlockSize"> </param>
        /// <param name="isCollectionBlock"> </param>
        /// <param name="collectionDeletedBlock"> </param>
        /// <returns></returns>
        public DeletedBlockInfo GetDeletedBlock(int requestedBlockSize, bool isCollectionBlock,
                                                out bool collectionDeletedBlock)
        {
            collectionDeletedBlock = true;

            #region try to recycle from File collection recycle bin
            if (HeaderData == null || HeaderData.RecycledSegment == null)
            {
                if (File.DeletedCollections != null && GetTopParent() != File.DeletedCollections &&
                    File.DeletedCollections.Count > 0)
                {
                    collectionDeletedBlock = false;
                    return File.DeletedCollections.GetTop();
                }
            }
            if (HeaderData != null && HeaderData.RecycledSegment != null)
            {
                long blockSize = HeaderData.RecycledSegment.Count * (int)DataBlockSize;
                if (blockSize >= requestedBlockSize)
                    return HeaderData.RecycledSegment;
                if (!IsDeletedBlocksList)
                {
                    if (DeletedBlocks != null)
                        DeletedBlocks.AddAvailableBlock(HeaderData.RecycledSegment.StartBlockAddress,
                                                        blockSize);
                    HeaderData.RecycledSegment = null;
                }
            }
            #endregion

            #region try to recycle from Collection block recycle bin
            if (!IsDeletedBlocksList && DeletedBlocks != null && DeletedBlocks.Count > 0)
            {
                long availableBlockAddress;
                long availableBlockSize;
                if (DeletedBlocks.GetAvailableBlock(IsDeletedBlocksList, requestedBlockSize,
                                                    out availableBlockAddress, out availableBlockSize))
                {
                    var dbi = new DeletedBlockInfo
                    {
                        StartBlockAddress = availableBlockAddress,
                        Count = (int)(availableBlockSize / (int)DataBlockSize)
                    };
                    return dbi;
                }
            }
            #endregion
            return null;
        }

        /// <summary>
        /// Deleted blocks maintains list of all deleted blocks on this Collection.
        /// Deleted blocks are tracked for recycling.
        /// </summary>
        public IDataBlockRecycler DeletedBlocks
        {
            get
            {
                if (!File.Server.HasTrashBin)
                    return null;
                if (Parent is CollectionOnDisk)
                    return ((CollectionOnDisk)Parent).DeletedBlocks;

                //** create the mru segments on disk
                if (deletedBlocks == null &&
                    File != null && !IsDeletedBlocksList)
                {
                    //if (File.Profile.TrashBinType == TrashBinType.FileWide)
                    //{
                    //    if (this != ((SortedDictionaryOnDisk) File.ObjectStore).BTreeAlgorithm)
                    //        return ((SortedDictionaryOnDisk) File.ObjectStore).BTreeAlgorithm.DeletedBlocks;
                    //}
                    deletedBlocks = new IndexedBlockRecycler(
                        File, new BTreeDefaultComparer(), string.Empty);
                    if (_deletedBlocksAddress >= 0)
                        deletedBlocks.DataAddress = _deletedBlocksAddress;

                    ((IndexedBlockRecycler)deletedBlocks).Blocks =
                        new Sop.Collections.Generic.SortedDictionary<long, Sop.DataBlock>();
                    deletedBlocks.Parent = this;
                    deletedBlocks.IsDeletedBlocksList = true;
                    deletedBlocks.Open();
                }
                return deletedBlocks;
            }
            set
            {
                if (!File.Server.HasTrashBin)
                    return;

                if (IsDeletedBlocksList)
                    return;

                //if (File.Profile.TrashBinType == TrashBinType.FileWide)
                //{
                //    if (this != ((SortedDictionaryOnDisk) File.ObjectStore).BTreeAlgorithm)
                //        return;
                //}

                if (Parent is CollectionOnDisk)
                    ((CollectionOnDisk)Parent).DeletedBlocks = value;
                else
                {
                    if (deletedBlocks == null || value == null)
                        deletedBlocks = value;
                    else
                        throw new InvalidOperationException("File.DeletedBlocks already has an assigned value.");
                }
            }
        }

        internal IDataBlockRecycler deletedBlocks;

        /// <summary>
        /// true means this Collection is the DeletedBlocksList (a.k.a. - the recycle bin).
        /// </summary>
        public virtual bool IsDeletedBlocksList { get; set; }
        #endregion

        private bool _inSaveBlocks = false;
    }
}