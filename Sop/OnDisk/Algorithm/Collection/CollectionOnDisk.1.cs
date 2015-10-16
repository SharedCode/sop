// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)


#region Log

/*
 * - Deleted Collection's Data Blocks need to be recycled by making the Blocks available
 * to Collection(s) that need to Grow.
 * Logic:
 * - During a Collection Grow Event, Block Recycler will get Growth sizeful of Blocks from 
 * Deleted Collection's Blocks and assign them to the Collection to Grow Deleted Blocks Head
 * making them available for allocation.
 * - Do this for each Collection To Grow event until there are Deleted Blocks of Deleted Collection(s)
 * 
 * -File Value can have 1 or more Collections In Disk
 * -Each Collection In Disk has 1 DataBlockDriver for Disks I/O
 * -Each Collection In Disk can be cloned and thus enumerated separately.
 * -Implement BTree's I/O execution/locking mechanism:
 *	* multiple reads execute in parallel
 *	* Write waits until all reads done
 */

#endregion

using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using Sop.Mru;
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.DataBlock;
using Sop.OnDisk.File;
using Sop.Persistence;
using Sop.Mru.Generic;

namespace Sop.OnDisk.Algorithm.Collection
{
    /// <summary>
    /// CollectionOnDisk is the base class of all collections on disk in SOP
    /// </summary>
    internal abstract partial class CollectionOnDisk : ICollectionOnDisk,
                                                       IInternalPersistentRef,
                                                       IDisposable,
                                                       IMruClient,
                                                       IFileEntity,
                                                       IInternalFileEntity
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        protected CollectionOnDisk()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="file"></param>
        /// <param name="dataBlocksMruManager"></param>
        protected CollectionOnDisk(File.IFile file)
        {
            Initialize(file, string.Empty);
        }

        /// <summary>
        /// true means this CollectionOnDisk is a Transaction Store.
        /// NOTE: Transaction Stores are handled differently than
        /// normal Collections within a Transaction.
        /// </summary>
        public bool IsTransactionStore
        {
            get
            {
                if (!Sop.Transaction.Transaction.DiskBasedMetaLogging)
                    return false;
                var collectionOnDisk = Parent as CollectionOnDisk;
                if (collectionOnDisk != null)
                    return (collectionOnDisk).IsTransactionStore;
                return ParentTransactionLogger != null || _isTransactionStore;
            }
            internal set
            {
                if (Sop.Transaction.Transaction.DiskBasedMetaLogging)
                    _isTransactionStore = value;
            }
        }

        private bool _isTransactionStore;
        private Transaction.ITransactionLogger _parentTransactionLogger;

        /// <summary>
        /// INTERNAL USE ONLY. Parent Transaction Logger
        /// </summary>
        public Transaction.ITransactionLogger ParentTransactionLogger
        {
            get { return !Sop.Transaction.Transaction.DiskBasedMetaLogging ? null : GetTopParent()._parentTransactionLogger; }
            set
            {
                if (Sop.Transaction.Transaction.DiskBasedMetaLogging)
                    GetTopParent()._parentTransactionLogger = value;
            }
        }

        #region Move methods

        /// <summary>
        /// Move current item pointer to 1st item
        /// </summary>
        /// <returns></returns>
        public virtual bool MoveFirst()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Move current item pointer to next item
        /// </summary>
        /// <returns></returns>
        public virtual bool MoveNext()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Move current item pointer to previous item
        /// </summary>
        /// <returns></returns>
        public virtual bool MovePrevious()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Move current item pointer to last item
        /// </summary>
        /// <returns></returns>
        public virtual bool MoveLast()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Move current item pointer to a specific item
        /// </summary>
        /// <param name="dataAddress">Address of item</param>
        /// <returns></returns>
        public virtual bool MoveTo(long dataAddress)
        {
            throw new NotImplementedException();
        }

        #endregion

        /// <summary>
        /// Collection Default Stream Reader
        /// </summary>
        public virtual System.IO.BinaryReader StreamReader
        {
            get { return OnDiskBinaryReader; }
        }

        /// <summary>
        /// protected virtual dispose.
        /// Closes this Collection on disk, its deleted blocks and Mru Segments.
        /// Sets the data block driver to null.
        /// </summary>
        protected internal virtual void InternalDispose()
        {
            // note: SOP's Dispose pattern is created to provide way to do early
            // garbage collection of the "graph" objects, simply. Not for freeing up unmanaged
            // resources, thus, no finalizer/SafeHandle "patterns". All members are "virtualized"
            // objects and they have custom Dispose for the same.
            if (isDisposed) return;
            isDisposed = true;
            // FileStream is a wrapper, not the .Net FileStream.
            if (FileStream != null)
                Close();
            if (deletedBlocks != null)
            {
                deletedBlocks.Dispose();
                deletedBlocks = null;
            }
            Parent = null;
            if (DataBlockDriver != null)
            {
                //if (!IsCloned)
                DataBlockDriver.Dispose();
                DataBlockDriver = null;
            }
            if (OnDiskBinaryReader != null)
            {
                OnDiskBinaryReader.Close();
                OnDiskBinaryReader = null;
            }
            if (OnDiskBinaryWriter != null)
            {
                OnDiskBinaryWriter.Close();
                OnDiskBinaryWriter = null;
            }
            _instanceTransaction = null;
            _parentTransactionLogger = null;
            File = null;
            Blocks = null;
        }
        private bool isDisposed;
        protected bool IsDisposed
        {
            get
            {
                return isDisposed;
            }
        }

        /// <summary>
        /// Dispose from memory this collection
        /// </summary>
        public void Dispose()
        {
            InternalDispose();
        }

        protected internal virtual void AddToBlocks(Sop.DataBlock blockSource)
        {
            IDictionary<long, Sop.DataBlock> t = Blocks;
            AddToBlocks(blockSource, t);
        }

        /// <summary>
        /// Add to target block collection a certain Block
        /// </summary>
        /// <param name="blockSource"></param>
        /// <param name="blocksDest"></param>
        protected internal virtual void AddToBlocks(Sop.DataBlock blockSource,
                                                    IDictionary<long, Sop.DataBlock> blocksDest)
        {
            if (blockSource != null && !_inSaveBlocks) //(!InSaveBlocks || Blocks != BlocksDest))
            {
                Sop.DataBlock db = blockSource;
                IDictionary<long, Sop.DataBlock> target = blocksDest;
                while (db != null)
                {
                    db.IsDirty = false;
                    long da = GetId(db);
                    Sop.DataBlock db2 = target[da];
                    if (db2 != null && db2 != db)
                    {
                        if (db2.InternalNextBlockAddress >= 0)
                        {
                            if (db2.InternalNextBlockAddress != db.InternalNextBlockAddress)
                            {
                                Log.Logger.Instance.Log(Log.LogLevels.Information,
                                    "CollectionOnDisk.AddToBlocks(segment boundary related): db.InternalNextBlockAddress={0}, db2.InternalNextBlockAddress={1}",
                                    db.InternalNextBlockAddress, db2.InternalNextBlockAddress);
                                db.InternalNextBlockAddress = db2.InternalNextBlockAddress;
                            }
                        }
                    }
                    target[da] = db;
                    db = db.Next;
                }
            }
        }

        /// <summary>
        /// OnRead gets called to read Object from Disk
        /// </summary>
        /// <param name="address">Address of Object on disk</param>
        /// <returns></returns>
        protected internal virtual object OnRead(long address)
        {
            //** read Node including Keys
            Sop.DataBlock d = DataBlockDriver.ReadBlockFromDisk(this, address, false);
            return ReadFromBlock(d);
        }

        /// <summary>
        /// IsDirty tells BTree whether this object needs to be rewritten to disk(dirty) or not
        /// </summary>
        public virtual bool IsDirty
        {
            get
            {
                return IsOpen && !IsCloned &&
                       !IsUnloading && //GetIsDirty(DiskBuffer);
                       (GetIsDirty(DiskBuffer) ||
                        (HeaderData != null && HeaderData.IsDirty) ||
                        MruHasDirtyItem());
            }
            set
            {
                SetIsDirty(DiskBuffer, value);
                if (HeaderData != null)
                    HeaderData.IsDirty = value;
            }
        }

        private bool MruHasDirtyItem()
        {
            return MruManager.IsDirty;
        }

        /// <summary>
        /// true means File Entity is new, otherwise false
        /// </summary>
        public bool IsNew { get; set; }

        private bool _isUnloading;

        public bool IsUnloading
        {
            get { return Parent is ICollectionOnDisk ? ((ICollectionOnDisk)Parent).IsUnloading : _isUnloading | _isCloned; }
            set
            {
                if (Parent is ICollectionOnDisk)
                    ((ICollectionOnDisk)Parent).IsUnloading = value;
                else
                    _isUnloading = value;
            }
        }

        #region MRU Min/Max Capacity

        /// <summary>
        /// When maximum capacity of DataBlocks' MRU Manager is reached, 
        /// it calls this version of "OnMaxCapacity" in order to 
        /// reduce the number of objects kept in-memory
        /// </summary>
        /// <param name="countOfBlocksUnloadToDisk">Recommended number of objects for removal</param>
        /// <returns>Actual Number of objects removed from memory</returns>
        public virtual int OnMaxCapacity(int countOfBlocksUnloadToDisk)
        {
            return SaveBlocks(true);
        }

        /// <summary>
        /// When maximum capacity of Collection's Objects' MRU Manager is reached, 
        /// it calls this version of "OnMaxCapacity" in order to 
        /// reduce the number of objects kept in-memory
        /// </summary>
        /// <param name="nodes">List of Collection Objects or "Nodes" that were removed
        /// from memory and should be saved to disk</param>
        public virtual int OnMaxCapacity(IEnumerable nodes)
        {
            foreach (IInternalPersistent node in nodes)
            {
                if (!node.IsDirty) continue;
                //** save Items' Data
                var block = WriteToBlock(node);
                DataBlockDriver.SetDiskBlock(this, block, false);
                AddToBlocks(block, Blocks);
                node.IsDirty = false;
            }
            int r = Blocks.Count;
            SaveBlocks(false);
            return r;
        }

        /// <summary>
        /// On Max Capacity
        /// </summary>
        public virtual void OnMaxCapacity()
        {
        }

        /// <summary>
        /// When MRU max capacity event is reached, MRU cache manager
        /// offloads all items between MruMaxCapacity & MruMinCapacity.
        /// </summary>
        public int MruMinCapacity
        {
            get
            {
                if (MruManager != null)
                    _mruMinCapacity = MruManager.MinCapacity;
                return _mruMinCapacity;
            }
            set
            {
                _mruMinCapacity = value;
                if (MruManager != null)
                    MruManager.MinCapacity = value;
            }
        }

        /// <summary>
        /// When MRU max capacity event is reached, MRU cache manager
        /// offloads all items between MruMaxCapacity & MruMinCapacity.
        /// </summary>
        public int MruMaxCapacity
        {
            get
            {
                if (MruManager != null)
                    _mruMaxCapacity = MruManager.MaxCapacity;
                return _mruMaxCapacity;
            }
            set
            {
                _mruMaxCapacity = value;
                if (MruManager != null)
                    MruManager.MaxCapacity = value;
            }
        }

        private int _mruMinCapacity;
        private int _mruMaxCapacity;

        #endregion

        #region Blocks

        /// <summary>
        /// Data Blocks cache.
        /// Set of Blocks for write to Disk. An Object is serialized
        /// into a series of Data Blocks which get written to Disk.
        /// </summary>
        protected internal Sop.Collections.Generic.ISortedDictionary<long, Sop.DataBlock> Blocks
        {
            get
            {
                if (_blocks == null)
                {
                    var collectionOnDisk = Parent as CollectionOnDisk;
                    if (collectionOnDisk != null)
                        return (collectionOnDisk).Blocks;
                    //_blocks = new Collections.Generic.SortedDictionary<long, Sop.DataBlock>();
                    _blocks = new Collections.Generic.ConcurrentSortedDictionary<long, Sop.DataBlock>();
                }
                return _blocks;
            }
            set { _blocks = value; }
        }
        private Collections.Generic.ISortedDictionary<long, Sop.DataBlock> _blocks;

        Collections.Generic.ISortedDictionary<long, Sop.DataBlock> ICollectionCache.Blocks
        {
            get { return Blocks; }
            set { Blocks = value; }
        }

        #endregion

        /// <summary>
        /// Serialize object and write its byte array to disk
        /// </summary>
        /// <param name="value"></param>
        /// <param name="isCollectionBlock"> </param>
        protected void WriteToDisk(IInternalPersistent value, bool isCollectionBlock)
        {
            Sop.DataBlock block = WriteToBlock(value, value.DiskBuffer);
            DataBlockDriver.SetDiskBlock(this, block, isCollectionBlock);
        }

        /// <summary>
        /// Serialize Object to destination block
        /// </summary>
        /// <param name="value">Object to Serialize</param>
        /// <param name="destination">Target Block</param>
        /// <returns></returns>
        protected internal Sop.DataBlock WriteToBlock(object value,
                                                      Sop.DataBlock destination)
        {
            return WriteToBlock(value, destination, true);
        }

        /// <summary>
        /// Write Value to the Sop.DataBlock.
        /// </summary>
        protected internal virtual Sop.DataBlock WriteToBlock(object value,
                                                              Sop.DataBlock destination, bool clearBlock)
        {
            if (destination == null)
                destination = CreateBlock();    //  new Sop.DataBlock(this.DataBlockSize);
            else
            {
                if (clearBlock)
                    destination.ClearData();
            }
            if (OnDiskBinaryWriter != null)
            {
                OnDiskBinaryWriter.DataBlock = destination;
                WritePersistentData(this, value, OnDiskBinaryWriter);
            }
            return destination;
        }

        /// <summary>
        /// Grow will expand the file to create space for new blocks' allocation.
        /// </summary>
        /// <param name="startOfGrowthBlocks"></param>
        /// <returns>Growth _region Size in bytes</returns>
        public long Grow(out long startOfGrowthBlocks)
        {
            return ((File.File)File).Grow(out startOfGrowthBlocks);
        }

        #region Initialize

        /// <summary>
        /// Initialize.
        /// </summary>
        /// <param name="file"></param>
        /// <param name="dataBlocksMruManager"></param>
        /// <param name="name"></param>
        /// <param name="extraParams"></param>
        protected void Initialize(File.IFile file, string name,
                                  params KeyValuePair<string, object>[] extraParams)
        {
            this.Name = name;
            KeyValuePair<string, object>[] p = extraParams;
            Initialize(file, p);
        }

        /// <summary>
        /// Initialize. NOTE: this function doesn't open the file.
        /// </summary>
        /// <param name="file"></param>
        /// <param name="parameters"> </param>
        protected internal virtual void Initialize(File.IFile file, params KeyValuePair<string, object>[] parameters)
        {
            if (file == null)
                throw new ArgumentNullException("file");

            if (transaction == null ||
                (transaction is Transaction.Transaction &&
               ((Transaction.Transaction)transaction).Server != file.Server))
            {
                Transaction.ITransactionLogger trans = file.Transaction;
                if (trans != null)
                    trans = ((Transaction.TransactionBase)trans).GetLeafChild();
                if (trans == null ||
                    trans is Transaction.Transaction)
                    transaction = trans;
            }

            if (string.IsNullOrEmpty(this.Name))
            {
                var f = new FileInfo(file.Filename);
                Name = string.Format("{0} Collection {1}", f.Name, ((Sop.OnDisk.File.File)file).GetNewStoreId());
            }
            if (MruMinCapacity == 0)
                MruMinCapacity = file.Profile.MruMinCapacity;
            if (MruMaxCapacity == 0)
                MruMaxCapacity = file.Profile.MruMaxCapacity;

            if (File == null)
                File = file;
            if (DataBlockSize == DataBlockSize.Unknown)
                DataBlockSize = file.DataBlockSize;
            HeaderData hd = null;
            if (parameters != null && parameters.Length > 0)
            {
                foreach (KeyValuePair<string, object> o in parameters)
                {
                    switch (o.Key)
                    {
                        case "HasMruSegments":
                            break;
                        case "HeaderData":
                            hd = (HeaderData)o.Value;
                            break;
                        default:
                            if (o.Key == "DataBlockDriver" && o.Value != null)
                                DataBlockDriver = (IDataBlockDriver)o.Value;
                            break;
                    }
                }
            }
            if (DataBlockDriver == null)
            {
                DataBlockDriver = new DataBlockDriver(this, hd);
            }
            else
            {
                if (DataBlockDriver.HeaderData == null)
                {
                    DataBlockDriver.HeaderData = hd ?? new HeaderData(DataBlockSize);
                }
            }
            if (MruManager == null)
            {
                int min = MruMinCapacity;
                int max = MruMaxCapacity;

                MruManager = new ConcurrentMruManager(min, max);
                //MruManager = new MruManager(min, max);

                MruManager.SetDataStores(this, DataBlockDriver);
            }
            if (_diskBuffer == null)
                _diskBuffer = CreateBlock(); //new Sop.DataBlock(DataBlockSize);
        }

        #endregion

        /// <summary>
        /// Retrieve Parameter Value from Parameter list
        /// </summary>
        /// <param name="parameters"></param>
        /// <param name="paramName">Name of Parameter to retrieve</param>
        /// <returns></returns>
        public static object GetParamValue(KeyValuePair<string, object>[] parameters, string paramName)
        {
            if (parameters != null && parameters.Length > 0)
            {
                foreach (KeyValuePair<string, object> o in parameters)
                {
                    if (o.Key == paramName)
                        return o.Value;
                }
            }
            return null;
        }

        /// <summary>
        /// Retrieves from the stream the size of the object to be DeSerialized
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        internal static int GetSize(BinaryReader reader)
        {
            if (reader.PeekChar() == -1) return -1;
            switch ((PersistenceType)reader.ReadByte())
            {
                case PersistenceType.Custom:
                    // ignore the type ID of object to be deserialized
                    reader.ReadInt32();
                    // return the size
                    return reader.ReadInt32();
                default:
                    return -1;
            }
        }

        /// <summary>
        /// Name of the Collection
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// HeaderData contains the Collection's Block Allocation Table(BAT) information
        /// </summary>
        public HeaderData HeaderData
        {
            get
            {
                if (DataBlockDriver != null)
                    return DataBlockDriver.HeaderData;
                return null;
            }
            set
            {
                if (DataBlockDriver != null)
                    DataBlockDriver.HeaderData = value;
            }
        }

        /// <summary>
        /// Read or DeSerialize Object from its Source Sop.DataBlock
        /// </summary>
        /// <param name="source"></param>
        /// <param name="target">Destination of Object DeSerialization</param>
        /// <returns></returns>
        public virtual object ReadFromBlock(Sop.DataBlock source, object target = null)
        {
            if (source.SizeOccupied > 0)
            {
                if (OnDiskBinaryReader == null)
                    Open();
                OnDiskBinaryReader.DataBlock = source;
                object r = target;
                bool? f = ReadPersistentData(this, OnDiskBinaryReader, ref r);
                if (f != null && !f.Value)
                    throw new InvalidOperationException("Can't deserialize.");
                if (!(r is IInternalPersistent)) return r;
                ((IInternalPersistent)r).DiskBuffer = source;
                ((IInternalPersistent)r).IsDirty = false;
                return r;
            }
            return target;
        }

        /// <summary>
        /// Write Value to the Sop.DataBlock
        /// </summary>
        protected internal Sop.DataBlock WriteToBlock(object value)
        {
            if (!(value is IInternalPersistent))
            {
                var db = CreateBlock();
                return WriteToBlock(value, db, false);
            }
            int sizeOnDisk = ((IInternalPersistent)value).HintSizeOnDisk;
            if (sizeOnDisk == 0 && this is BTreeAlgorithm)
                sizeOnDisk = ((BTreeAlgorithm)this).HintValueSizeOnDisk;
            if (((IInternalPersistent)value).DiskBuffer == null)
            {
                ((IInternalPersistent)value).DiskBuffer = CreateBlock();
                ((IInternalPersistent)value).DiskBuffer.IsHead = true;
            }
            else
                ((IInternalPersistent)value).DiskBuffer.ClearData();
            Sop.DataBlock r = WriteToBlock(value, ((IInternalPersistent)value).DiskBuffer);
            int sizeWrittenOnStream = r.GetSizeOccupied();
            if (sizeOnDisk > sizeWrittenOnStream)
            {
                var b = new byte[sizeOnDisk - sizeWrittenOnStream];
                OnDiskBinaryWriter.Write(b);
            }
            return r;
        }
        internal Sop.DataBlock CreateBlock()
        {
            return CreateBlock(DataBlockSize);
        }
        internal Sop.DataBlock CreateBlock(DataBlockSize size)
        {
            return DataBlockDriver.CreateBlock(size);
        }

        /// <summary>
        /// get/set Sop.DataBlock Driver used by this Collection
        /// </summary>
        protected internal IDataBlockDriver DataBlockDriver { get; protected set; }

        /// <summary>
        /// Returns the File interface
        /// </summary>
        public virtual File.IFile File { get; set; }

        Sop.IFile Sop.ICollectionOnDisk.File
        {
            get { return File; }
            set { File = (File.IFile)value; }
        }

        /// <summary>
        /// Returns the Current entry DeSerialized from File Stream.
        /// Will return:
        /// - byte[] if IInternalPersistent was saved
        /// - DeSerialized Value if object was Serialized
        /// </summary>
        public virtual object CurrentEntry
        {
            get
            {
                OnDiskBinaryReader.DataBlock = this.GetCurrentDataBlock();
                object o = null;
                try
                {
                    ReadPersistentData(this, OnDiskBinaryReader, ref o);
                    if (!(o is CollectionOnDisk)) return o;
                    ((CollectionOnDisk)o).DiskBuffer = this.GetCurrentDataBlock();
                    ((CollectionOnDisk)o).File = File;
                    ((CollectionOnDisk)o).Open();
                }
                catch (EndOfStreamException)
                {
                } //** ignore empty Collection exception..
                return o;
            }
        }

        /// <summary>
        /// Returns the Data Address on disk(or virtual data store) of this Collection
        /// </summary>
        /// <returns></returns>
        public long GetId()
        {
            return GetId(DiskBuffer);
        }

        #region In-Memory ID

        /// <summary>
        /// Returns in-memory ID of this collection.
        /// </summary>
        public int InMemoryId
        {
            get
            {
                if (_inMemoryId == 0)
                    _inMemoryId = ((File.File)File).GetNewInMemoryId();
                return _inMemoryId;
            }
        }

        private int _inMemoryId;

        #endregion

        /// <summary>
        /// Update the Data Address of this Collection on disk
        /// </summary>
        /// <param name="address"></param>
        public void SetId(long address)
        {
            SetId(DiskBuffer, address);
        }

        /// <summary>
        /// Returns flag that tells whether this Collection had been modified or not
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        public bool GetIsDirty(Sop.DataBlock block)
        {
            if (DataBlockDriver == null)
                return block.IsDirty;
            return DataBlockDriver.GetIsDirty(block);
        }

        public void SetIsDirty(bool newValue)
        {
            SetIsDirty(DiskBuffer, newValue);
        }

        /// <summary>
        /// Mark this Collection as modified or not
        /// </summary>
        /// <param name="block"></param>
        /// <param name="newValue"></param>
        public void SetIsDirty(Sop.DataBlock block, bool newValue)
        {
            if (DataBlockDriver == null)
                block.IsDirty = newValue;
            else
                DataBlockDriver.SetIsDirty(block, newValue);
        }

        /// <summary>
        /// Returns the ID(DataAddress if block is not virtualized block) of a given block
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        public long GetId(Sop.DataBlock block)
        {
            return DataBlockDriver.GetId(block);
        }

        /// <summary>
        /// Set Block ID to a given Address
        /// </summary>
        /// <param name="block"></param>
        /// <param name="address"></param>
        public void SetId(Sop.DataBlock block, long address)
        {
            DataBlockDriver.SetId(block, address);
        }

        /// <summary>
        /// Set Target Block Dest ID with ID of Source block
        /// </summary>
        /// <param name="dest"></param>
        /// <param name="source"></param>
        public void SetId(Sop.DataBlock dest, Sop.DataBlock source)
        {
            SetId(dest, GetId(source));
        }

        /// <summary>
        /// Returns the Current Sop.DataBlock
        /// </summary>
        /// <returns></returns>
        protected internal Sop.DataBlock GetCurrentDataBlock()
        {
            return GetCurrentDataBlock(false);
        }

        /// <summary>
        /// Remove the Object with "Address" from Object and Sop.DataBlock MRUs.
        /// </summary>
        /// <param name="transaction"> </param>
        public virtual void RemoveFromCache(Transaction.ITransactionLogger transaction)
        {
            //if (MruManager != null)
            //    MruManager.Remove(transaction);
            //DataBlockDriver.MruManager.Remove(transaction);
        }

        /// <summary>
        /// Remove from in-memory blocks the block referenced
        /// by DataAddress.
        /// NOTE: this function is invoked when Rolling back changes
        /// </summary>
        /// <param name="dataAddress"></param>
        /// <param name="transaction"> </param>
        internal virtual bool RemoveInMemory(long dataAddress,
                                             Transaction.ITransactionLogger transaction)
        {
            //** current block
            if (currentDataBlock != null &&
                (currentDataBlock.DataAddress == -1 || currentDataBlock.IsBlockOfThis(dataAddress)))
            {
                currentEntry = currentDataBlock = null;
                _currentEntryDataAddress = -1;
            }
            //** MRU
            RemoveFromCache(transaction);

            if (DataBlockDriver.HeaderData.RecycledSegment != null)
            {
                if (dataAddress >= DataBlockDriver.HeaderData.RecycledSegment.StartBlockAddress &&
                    dataAddress <= (DataBlockDriver.HeaderData.RecycledSegment.StartBlockAddress +
                                    DataBlockDriver.HeaderData.RecycledSegment.Count * (int)DataBlockSize))
                    DataBlockDriver.HeaderData.RecycledSegment = null;
            }

            //** take care of deleted blocks list... 
            if (deletedBlocks is CollectionOnDisk)
                ((CollectionOnDisk)deletedBlocks).RemoveInMemory(dataAddress, transaction);

            //** Header and DiskBuffer
            return DiskBuffer.DataAddress == -1 ||
                   DiskBuffer.IsBlockOfThis(dataAddress) ||
                   HeaderData.DiskBuffer.DataAddress == -1 ||
                   HeaderData.DiskBuffer.IsBlockOfThis(dataAddress);
        }
    }
}
