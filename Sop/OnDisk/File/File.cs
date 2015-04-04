// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.IO;
using Sop.Persistence;
using Sop.SystemInterface;
using System.Threading;

namespace Sop.OnDisk.File
{
    /// <summary>
    /// File object. A File can have one or more Collections On Disk.
    /// </summary>
    internal class File : IFile, IInternalFileEntity
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public File() { }
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="server"></param>
        /// <param name="name"> </param>
        /// <param name="filename"></param>
        /// <param name="accessMode"></param>
        /// <param name="profile"></param>
        public File(ObjectServer server,
                    string name = null,
                    string filename = null,
                    AccessMode accessMode = AccessMode.ReadWrite,
                    Profile profile = null)
        {
            Initialize(server, name, filename, accessMode, profile);
        }

        /// <summary>
        /// Returns the _region Size on disk of the File
        /// </summary>
        /// <returns></returns>
        public int GetSegmentSize()
        {
            return StoreGrowthSizeInNob * (int)DataBlockSize;
        }

        public System.Text.Encoding Encoding
        {
            get { return Server.Encoding; }
        }

        /// <summary>
        /// Create a Lookup Table
        /// </summary>
        /// <returns></returns>
        public Algorithm.SortedDictionary.ISortedDictionaryOnDisk CreateLookupTable()
        {
            return CreateLookupTable(null);
        }

        /// <summary>
        /// Create a Lookup table
        /// </summary>
        /// <returns></returns>
        public Algorithm.SortedDictionary.ISortedDictionaryOnDisk CreateLookupTable(IInternalPersistent parent)
        {
            Algorithm.SortedDictionary.ISortedDictionaryOnDisk lookup = null;
            if (Transaction != null)
                lookup = ((Transaction.TransactionBase)Transaction).CreateCollection(this);
            lookup.Parent = parent;
            if (parent is Algorithm.Collection.ICollectionOnDisk && lookup.Transaction == null)
                lookup.Transaction = ((Algorithm.Collection.ICollectionOnDisk)parent).Transaction;
            return lookup;
        }

        /// <summary>
        /// Initialize this File.
        /// </summary>
        /// <param name="server"></param>
        /// <param name="name">Name of the File Object</param>
        /// <param name="filename"></param>
        /// <param name="accessMode"></param>
        /// <param name="profile"></param>
        public void Initialize(ObjectServer server,
                               string name,
                               string filename,
                               AccessMode accessMode,
                               Profile profile)
        {
            this.Server = server;
            if (profile != null)
                this._profile = profile;
            else if (server != null && server.Profile != null)
                this._profile = new Profile(server.Profile);

            this.Name = name;
            if (string.IsNullOrEmpty(filename))
                filename = string.Format("{0}{1}.{2}", server.Path, name, ObjectServer.DefaultFileExtension);
            this.Filename = filename;
            this.AccessMode = accessMode;

            var fi = new FileInfo(filename);
            if (!System.IO.Directory.Exists(fi.DirectoryName))
                System.IO.Directory.CreateDirectory(fi.DirectoryName);
        }

        private void dispose()
        {
            if (_store != null)
            {
                if (IsOpen)
                    Close();
                if (_store != null)
                {
                    _store.Locker.Invoke(() =>
                    {
                        _store.Dispose();
                        if (_storeAddress == -1)
                            _storeAddress = _store.DataAddress;
                    });
                    _store = null;
                }
                if (DeletedCollections != null)
                {
                    DeletedCollections.Dispose();
                    if (_deletedCollectionsAddress == -1)
                        _deletedCollectionsAddress = DeletedCollections.DataAddress;
                    DeletedCollections = null;
                }
                CollectionsPool = null;
                Parent = null;
                Server = null;
            }
        }
        private long _storeAddress = -1;
        private long _deletedCollectionsAddress = -1;

        /// <summary>
        /// Dispose this File from Memory
        /// </summary>
        public void Dispose()
        {
            dispose();
        }


        /// <summary>
        /// Grow will expand the file to create space for new blocks' allocation.
        /// </summary>
        /// <param name="startOfGrowthBlocks"></param>
        /// <returns>Growth _region Size in bytes</returns>
        public long Grow(out long startOfGrowthBlocks)
        {
            lock (this)
            {
                var ss = (short)DataBlockSize;
                long segmentSize = StoreGrowthSizeInNob * ss;
                long fileSize = Store.FileStream.Length;
                if (Size < fileSize)
                {
                    long segmentCount = fileSize / segmentSize;
                    if (fileSize % segmentSize != 0 || segmentCount == 0)
                        segmentCount++;
                    Size = segmentCount * segmentSize;
                }
                // return the Start of the newly allocated Blocks.
                startOfGrowthBlocks = Size;
                if (startOfGrowthBlocks == 0)
                    segmentSize = Profile.StoreSegmentSize;
                Size = startOfGrowthBlocks + segmentSize;
                IsDirty = true;
                return segmentSize;
            }
        }
        /// <summary>
        /// Returns the Sop.DataBlock Size of the File
        /// </summary>
        public DataBlockSize DataBlockSize
        {
            get { return Profile.DataBlockSize; }
        }

        private bool _isDirty;

        /// <summary>
        /// IsDirty tells BTree whether this object needs to be rewritten to disk(dirty) or not
        /// </summary>
        public bool IsDirty
        {
            get { return _isDirty || (_store != null && _store.IsDirty); }
            set { _isDirty = _store.IsDirty = value; }
        }

        /// <summary>
        /// MRU Minimum capacity for Store of this file.
        /// </summary>
        private const long mruMinCapacity = 3750;

        /// <summary>
        /// MRU Maximum capacity for Store of this file.
        /// </summary>
        private const long mruMaxCapacity = 5000;

        /// <summary>
        /// MRU Minimum Capacity
        /// </summary>
        public long MruMinCapacity
        {
            get { return mruMinCapacity; }
        }

        /// <summary>
        /// MRU Maximum Capacity
        /// </summary>
        public long MruMaxCapacity
        {
            get { return mruMaxCapacity; }
        }


        /// <summary>
        /// Open the file.
        /// </summary>
        /// <param name="filename">name of file to open</param>
        /// <param name="accessMode">AccessMode's are Read Only, Read/Write</param>
        protected internal virtual void Open(string filename, AccessMode accessMode)
        {
            if (!IsOpen)
            {
                this.Filename = filename;
                this.AccessMode = accessMode;
                bool justCreated = false;
                if (_store == null)
                {
                    lock (this)
                    {
                        if (_store == null)
                        {
                            justCreated = true;
                            if (Transaction != null)
                                _store =
                                    ((Sop.Transaction.TransactionBase)Transaction.GetOuterChild()).CreateCollection(this);
                            else
                                _store = ObjectServer.CreateDictionaryOnDisk(this);
                        }
                    }
                }
                else
                    _store.Locker.Invoke(() => { _store.Open(); });

                if (_store.FileStream != null &&
                    (Size == 0 || Size < _store.FileStream.Length))
                    Size = _store.FileStream.Length;

                if (Server.TrashBinType != TrashBinType.Nothing)
                {
                    if (DeletedCollections == null)
                        CreateDeletedCollections();
                    else
                        DeletedCollections.Open();
                }
                if (Server != null)
                {
                    if (this == this.Server.SystemFile)
                        Store.DataAddress =
                            ((IInternalPersistent)this).DiskBuffer.DataAddress;
                    Store.Locker.Invoke(() =>
                    {
                        Store.Open();
                        if (justCreated)
                        {
                            justCreated = Store.IsDirty;
                            if (justCreated && !Server.ReadOnly)
                                //** save the ObjectStore as we need the "System" Stores 
                                //** persisted right after initialization and Open...
                                Store.Flush();
                        }
                    });
                }
                if (DeletedCollections != null)
                    DeletedCollections.Open();
            }
            else if (Server.HasTrashBin)
                CreateDeletedCollections();
        }

        /// <summary>
        /// Open the File
        /// </summary>
        public void Open()
        {
            Open(Filename, AccessMode);
        }

        /// <summary>
        /// Default File Data Block size is 512 bytes
        /// </summary>
        private static int _fileBlockSize = (int)DataBlockSize.Minimum; //8 * 1024;// 512;	//2 * 1024;

        /// <summary>
        /// Open a File with buffering disabled
        /// </summary>
        /// <param name="filename"></param>
        /// <returns></returns>
        public static FileStream UnbufferedOpen(string filename)
        {
            int blockSize;
            return UnbufferedOpen(filename, out blockSize);
        }

        /// <summary>
        /// Open a File with buffering disabled
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="blockSize"></param>
        /// <returns></returns>
        public static FileStream UnbufferedOpen(string filename, out int blockSize)
        {
            return UnbufferedOpen(filename, System.IO.FileAccess.ReadWrite, _fileBlockSize, out blockSize);
        }

        /// <summary>
        /// Open a File with buffering disabled.
        /// NOTE: BTree Gold needs to provide its own buffering for performance.
        /// </summary>
        /// <param name="bufferSize"> </param>
        /// <param name="blockSize"></param>
        /// <param name="filename"> </param>
        /// <param name="accessMode"> </param>
        /// <returns></returns>
        public static FileStream UnbufferedOpen(string filename,
                                                System.IO.FileAccess accessMode,
                                                int bufferSize,
                                                out int blockSize)
        {
            if (string.IsNullOrEmpty(filename))
                throw new ArgumentNullException("filename");
            if (SectorSize == 0)
                SectorSize = SystemAdaptor.SystemInterface.GetDriveSectorSize(filename);
            if (SectorSize > bufferSize)
                bufferSize = SectorSize;
            double d = (double)bufferSize / SectorSize;
            if (d < 1)
                d = 1;
            d = d * SectorSize;
            blockSize = (int)d;
            return new FileStream(filename,
                                        FileMode.OpenOrCreate,
                                        accessMode,
                                        FileShare.ReadWrite,
                                        false, true, blockSize);
        }

        internal static int SectorSize;

        /// <summary>
        /// Open a File with buffering disabled.
        /// NOTE: BTree Gold needs to provide its own buffering for performance.
        /// </summary>
        /// <param name="systemBlockSize"> </param>
        /// <returns></returns>
        public FileStream UnbufferedOpen(out int systemBlockSize)
        {
            if (string.IsNullOrEmpty(Filename))
                throw new ArgumentNullException("Filename");

            string fname = Filename;
            if (Server != null)
                fname = Server.NormalizePath(Filename);

            if (_blockSize <= 0)
            {
                systemBlockSize = SystemAdaptor.SystemInterface.GetDriveSectorSize(fname);
                this._systemDetectedBlockSize = systemBlockSize;

                if (systemBlockSize > _fileBlockSize)
                    _fileBlockSize = systemBlockSize;

                double d = (double)_fileBlockSize / systemBlockSize;
                if (d < 1) d = 1;
                d = d * systemBlockSize;
                _blockSize = (int)d;
            }
            else
                systemBlockSize = this._systemDetectedBlockSize;
            if (Server != null && Server.Transaction != null)
                ((Sop.Transaction.TransactionRoot)((Sop.Transaction.TransactionBase)Server.Transaction).Root).
                    RegisterOpenFile(fname);

            FileMode fm = FileMode.OpenOrCreate;
            FileShare fs = FileShare.ReadWrite;
            var fa = (System.IO.FileAccess)AccessMode;
            if (Server != null && Server.ReadOnly)
            {
                fm = FileMode.Open;
                fs = FileShare.Read;
                fa = System.IO.FileAccess.Read;
            }
            return new FileStream(fname, fm, fa, fs, false, true, _blockSize);
        }

        private int _systemDetectedBlockSize;

        /// <summary>
        /// Block Size
        /// </summary>
        private int _blockSize = 0;

        /// <summary>
        /// Save ObjectStores of this File Object
        /// </summary>
        public void Flush()
        {
            if (DeletedCollections != null)
                DeletedCollections.Flush();
            if (CollectionsPool != null && CollectionsPool.Count > 0)
            {
                foreach (Algorithm.Collection.ICollectionOnDisk coll in CollectionsPool.Values)
                {
                    if (coll != _store)
                        ((Sop.Collections.ISynchronizer)coll.SyncRoot).Invoke(() => { coll.Flush(); });
                }
            }
            if (_store != null)
                _store.Locker.Invoke(() => { _store.Flush(); });
            _isDirty = false;
        }

        /// <summary>
        /// For SOP internal use
        /// </summary>
        public void MarkNotDirty()
        {
            if (DeletedCollections != null)
                DeletedCollections.IsUnloading = true;
            if (CollectionsPool != null && CollectionsPool.Count > 0)
            {
                var colls = new Algorithm.Collection.ICollectionOnDisk[CollectionsPool.Count];
                CollectionsPool.Values.CopyTo(colls, 0);
                for (int i = 0; i < colls.Length; i++)
                {
                    ((Sop.Collections.ISynchronizer)colls[i].SyncRoot).Invoke(() => { colls[i].IsUnloading = true; });
                }
            }
            if (_store != null)
                _store.Locker.Invoke(() => { _store.IsUnloading = true; });
        }

        void IInternalFileEntity.CloseStream()
        {
            OpenOrCloseStream(false);
        }

        void IInternalFileEntity.OpenStream()
        {
            OpenOrCloseStream(true);
        }

        private void OpenOrCloseStream(bool open)
        {
            if (DeletedCollections != null)
            {
                if (open)
                    ((IInternalFileEntity)DeletedCollections).OpenStream();
                else
                    ((IInternalFileEntity)DeletedCollections).CloseStream();
            }
            if (CollectionsPool != null && CollectionsPool.Count > 0)
            {
                var colls = new Algorithm.Collection.ICollectionOnDisk[CollectionsPool.Count];
                CollectionsPool.Values.CopyTo(colls, 0);
                for (int i = 0; i < colls.Length; i++)
                {
                    ((Collections.ISynchronizer)colls[i].SyncRoot).Invoke(() =>
                    {
                        if (open)
                            ((IInternalFileEntity)colls[i]).OpenStream();
                        else
                            ((IInternalFileEntity)colls[i]).CloseStream();
                    });
                }
            }
            if (_store != null)
            {
                _store.Locker.Invoke(() =>
                {
                    if (open)
                        ((IInternalFileEntity)_store).OpenStream();
                    else
                        ((IInternalFileEntity)_store).CloseStream();
                });
            }
        }
        #region Manage Store Locks
        /// <summary>
        /// Manage this File's Store locks.
        /// </summary>
        /// <param name="lockStores"></param>
        /// <returns></returns>
        public List<Collections.ISynchronizer> ManageLock(bool lockStores = true)
        {
            LockSystemStores(lockStores);
            List<Collections.ISynchronizer> result = new List<Collections.ISynchronizer>();
            if (CollectionsPool != null && CollectionsPool.Count > 0)
            {
                Algorithm.Collection.ICollectionOnDisk[] colls = null;
                #region lock/copy Store Pools
                if (lockStores)
                {
                    CollectionsPool.Locker.Lock();
                    colls = new Algorithm.Collection.ICollectionOnDisk[CollectionsPool.Count];
                    CollectionsPool.Values.CopyTo(colls, 0);
                }
                else
                {
                    colls = new Algorithm.Collection.ICollectionOnDisk[CollectionsPool.Count];
                    CollectionsPool.Values.CopyTo(colls, 0);
                    CollectionsPool.Locker.Unlock();
                }
                #endregion
                var systemStoreLockers = new List<Collections.ISynchronizer>(2);
                // Get collection of System & client Store Lockers, request client Stores to get locked for commit...
                for (int i = 0; i < colls.Length; i++)
                {
                    // no need to manage disposed Stores!
                    if (colls[i] is ISortedDictionaryOnDisk &&
                        ((Algorithm.SortedDictionary.SortedDictionaryOnDisk)colls[i]).IsDisposed)
                        continue;
                    if (SystemManagedStore(colls[i]))
                    {
                        systemStoreLockers.Add((Collections.ISynchronizer)colls[i].SyncRoot);
                        continue;
                    }
                    result.Add((Collections.ISynchronizer)colls[i].SyncRoot);
                    ((Collections.ISynchronizer)colls[i].SyncRoot).CommitLockRequest(lockStores);
                }

                // wait until each Store grants the commit lock/unlock request...
                foreach (var locker in result)
                {
                    locker.WaitForCommitLock(lockStores);
                }
                result.AddRange(systemStoreLockers);
                #region under study for removal (not needed)
                //if (lockStores)
                //{
                //    // track those modified Stores so they can get flushed in the commit process...
                //    for (int i = 0; i < colls.Length; i++)
                //    {
                //        if (SystemManagedStore(colls[i]))
                //        {
                //            continue;
                //        }
                //        if (colls[i] is BTreeAlgorithm)
                //        {
                //            if (((BTreeAlgorithm)colls[i]).IsDirty)
                //                ((Sop.Transaction.TransactionBase)((BTreeAlgorithm)colls[i]).Transaction).
                //                    TrackModification((Algorithm.Collection.CollectionOnDisk)colls[i]);
                //        }
                //        else if (colls[i] is ISortedDictionaryOnDisk)
                //        {
                //            if (((ISortedDictionaryOnDisk)colls[i]).IsDirty)
                //                ((Sop.Transaction.TransactionBase)((ISortedDictionaryOnDisk)colls[i]).Transaction).
                //                    TrackModification(((Algorithm.SortedDictionary.SortedDictionaryOnDisk)colls[i]).BTreeAlgorithm);
                //        }
                //    }
                //}
                #endregion
            }
            return result;
        }
        private void LockSystemStores(bool lockStores = true)
        {
            if (lockStores)
                ((FileSet)Server.FileSet).Btree.Locker.Lock();
            else
                ((FileSet)Server.FileSet).Btree.Locker.Unlock();
            if (_store != null)
            {
                if (lockStores)
                    _store.Locker.Lock();
                else
                    _store.Locker.Unlock();
            }
            #region no need to lock these system Stores
            //if (DeletedCollections != null)
            //{
            //    if (lockStores)
            //        ((FileRecycler)DeletedCollections).Locker.Lock();
            //    else
            //        ((FileRecycler)DeletedCollections).Locker.Unlock();
            //}
            #endregion
        }
        private bool SystemManagedStore(ICollectionOnDisk store)
        {
            return store == ((FileSet)Server.FileSet).Btree ||
                   store == _store ||
                   store == ((FileRecycler)DeletedCollections).Collection;
        }
        #endregion

        /// <summary>
        /// Close the file
        /// </summary>
        public virtual void Close()
        {
            if (IsClosing || _store == null || _store.FileStream == null) return;
            if (IsDirty) Flush();
            if (_storeAddress == -1)
                _storeAddress = _store.DataAddress;
            IsClosing = true;
            try
            {
                if (DeletedCollections != null)
                {
                    DeletedCollections.Close();
                    if (_deletedCollectionsAddress == -1)
                        _deletedCollectionsAddress = DeletedCollections.DataAddress;
                }
                if (CollectionsPool != null && CollectionsPool.Count > 0)
                {
                    var colls = new Algorithm.Collection.ICollectionOnDisk[CollectionsPool.Count];
                    CollectionsPool.Values.CopyTo(colls, 0);
                    for (int i = 0; i < colls.Length; i++)
                        colls[i].Close();
                    CollectionsPool.Clear();
                }
                if (_store != null)
                    _store.Close();
                _store = null;
                if (_diskBuffer != null)
                    _diskBuffer.ClearData();
            }
            finally
            {
                IsClosing = false;
            }
        }

        /// <summary>
        /// Returns the Transaction this File object belongs to
        /// </summary>
        public Transaction.ITransactionRoot Transaction
        {
            get
            {
                if (Server != null)
                    return (Transaction.TransactionRoot)Server.Transaction;
                return null;
            }
        }

        /// <summary>
        /// Returns Name of this instance
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
#if (DEBUG)
            return string.Format("File {0}, Store {1}, DeletedCollections {2}",
                this.Filename, ((Sop.OnDisk.Algorithm.SortedDictionary.SortedDictionaryOnDisk)Store).GetHeaderInfo(),
                DeletedCollections != null ? DeletedCollections.ToString() : "");
#else
            return Filename;
#endif
        }

        #region for cleanup
        ///// <summary>
        ///// Delete the File
        ///// </summary>
        //public void Delete()
        //{
        //    bool o = IsOpen;
        //    Close();
        //    int SystemDetectedBlockSize;
        //    FileStream fs = UnbufferedOpen(out SystemDetectedBlockSize);
        //    fs.SetLength(0);
        //    fs.Dispose();
        //    if (o)
        //    {
        //        DeletedCollections = null;
        //        this._objectStore = null;
        //        Open();
        //        Flush();
        //    }
        //    CollectionCounter = 0;
        //}
        ///// <summary>
        ///// Unimplimented in this version, 'doesn't do anything...
        ///// </summary>
        //public virtual void Shrink()
        //{
        //}
        #endregion
        /// <summary>
        /// Name of the file where Collections of this File Objects
        /// store/read data.
        /// </summary>
        public string Filename { get; private set; }

        /// <summary>
        /// Default FileStream is the File's ObjectStore's FileStream
        /// </summary>
        public FileStream DefaultFileStream
        {
            get
            {
                if (_store != null)
                {
                    return _store.Locker.Invoke(() => { return Store.FileStream; });
                }
                return null;
            }
        }

        /// <summary>
        /// Returns Name of this File Entity
        /// </summary>
        public string Name
        {
            get { return _name; }
            internal set { _name = value; }
        }

        private string _name;

        /// <summary>
        /// Size of the File
        /// </summary>
        public long Size { get; set; }

        /// <summary>
        /// Growth Size in Number of Blocks
        /// </summary>
        public int StoreGrowthSizeInNob
        {
            get { return Profile.StoreGrowthSizeInNob; }
            //set { Profile.StoreGrowthSizeInNob = value; }
        }

        /// <summary>
        /// AccessMode defaults to Read Write
        /// </summary>
        public AccessMode AccessMode
        {
            get { return _accessMode; }
            private set { _accessMode = value; }
        }

        private AccessMode _accessMode = AccessMode.ReadWrite;

        /// <summary>
        /// Deleted Collections are recycled and reused using methods of this Collection.
        /// </summary>
        public IFileRecycler DeletedCollections { get; set; }

        private void CreateDeletedCollections()
        {
            if (Server.HasTrashBin && DeletedCollections == null)
            {
                lock (this)
                {
                    if (DeletedCollections != null)
                        return;
                    DeletedCollections = new FileRecycler(this);
                }
                DeletedCollections.Open();
            }
        }

        private Profile _profile;

        /// <summary>
        /// Contains B-Tree profile for this file object
        /// </summary>
        public Profile Profile
        {
            get
            {
                if (_profile == null)
                    _profile = new Profile();
                return _profile;
            }
            internal set { _profile = value; }
        }

        /// <summary>
        /// Returns a new unique to this File Store ID.
        /// </summary>
        /// <returns></returns>
        public int GetNewStoreId()
        {
            return System.Threading.Interlocked.Increment(ref CollectionCounter);
        }
        private int CollectionCounter;

        /// <summary>
        /// ObjectStore provides storage management and retrieval for persisted Objects.
        /// </summary>
        public Algorithm.SortedDictionary.ISortedDictionaryOnDisk Store
        {
            get
            {
                if (_store == null)
                {
                    if (string.IsNullOrEmpty(this.Filename))
                        throw new InvalidOperationException("Filename");
                    Open();
                }
                return _store;
            }
        }

        /// <summary>
        /// Object Server
        /// </summary>
        public OnDisk.ObjectServer Server { get; set; }

        /// <summary>
        /// Return the size on disk(in bytes) of this object
        /// </summary>
        public int HintSizeOnDisk { get; private set; }

        /// <summary>
        /// true means File is new, otherwise false
        /// </summary>
        public bool IsNew { get; set; }

        #region Pack related
        /// <summary>
        /// Pack this File object
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="writer"></param>
        public void Pack(IInternalPersistent parent, BinaryWriter writer)
        {
            writer.Write(Size);
            writer.Write(Name);
            writer.Write(Filename);

            //** save the profile info
            Profile.Pack(writer);

            bool hasDeletedBlocks = DeletedCollections != null || _deletedCollectionsAddress != -1;
            writer.Write(hasDeletedBlocks);
            if (hasDeletedBlocks)
            {
                if (DeletedCollections != null && DeletedCollections.DataAddress < 0)
                {
                    DeletedCollections.Flush();
                }
                if (_deletedCollectionsAddress == -1 && DeletedCollections != null)
                    _deletedCollectionsAddress = DeletedCollections.DataAddress;
                writer.Write(_deletedCollectionsAddress);
            }
            writer.Write(_store != null || _storeAddress != -1);
            if (_store != null)
            {
                if (_store.IsOpen && _store.IsDirty)
                {
                    _store.RegisterChange();
                    _store.Flush();
                }
                if (_storeAddress == -1)
                    _storeAddress = _store.DataAddress;
            }
            if (_storeAddress != -1)
                writer.Write(_storeAddress);
        }

        /// <summary>
        /// Unpack this object
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        public void Unpack(IInternalPersistent parent, BinaryReader reader)
        {
            if (this.Server == null && parent is Algorithm.Collection.ICollectionOnDisk)
                this.Server = ((Algorithm.Collection.ICollectionOnDisk)parent).File.Server;

            Size = reader.ReadInt64();
            _name = reader.ReadString();
            this.Filename = reader.ReadString();
            if (Server != null && (Server.HomePath != Server.Path &&
                                   !System.IO.File.Exists(Filename) && Filename.StartsWith(Server.HomePath)))
            {
                string nameOfFile = Filename.Substring(Server.HomePath.Length);
                string newFilename = string.Format("{0}{1}", Server.Path, nameOfFile);
                if (System.IO.File.Exists(newFilename))
                    Filename = newFilename;
            }
            Profile = new Profile(((Algorithm.Collection.ICollectionOnDisk)parent).File.Profile);

            //** Read the profile info...
            Profile.Unpack(reader);

            bool hasDeletedBlocks = reader.ReadBoolean();
            long collectionRecycler = -1;
            if (hasDeletedBlocks)
                collectionRecycler = _deletedCollectionsAddress = reader.ReadInt64();
            bool deserializeObjectStore = reader.ReadBoolean();
            if (deserializeObjectStore)
            {
                _store = Transaction != null ? ((Transaction.TransactionBase)Transaction).CreateCollection(this) :
                    ObjectServer.CreateDictionaryOnDisk(this);
                _store.DataAddress = _storeAddress = reader.ReadInt64();
                if (!_store.IsOpen)
                    _store.Open();
            }
            if (Server != null && Server.HasTrashBin)
            {
                if (collectionRecycler >= 0)
                {
                    if (DeletedCollections == null)
                        CreateDeletedCollections();
                    DeletedCollections.DataAddress = collectionRecycler;
                    DeletedCollections.Load();
                }
                else if (DeletedCollections == null)
                    CreateDeletedCollections();
            }
        }
        #endregion

        /// <summary>
        /// Returns the Parent of this File
        /// </summary>
        /// <param name="parentType"></param>
        /// <returns></returns>
        public IInternalPersistent GetParent(Type parentType)
        {
            if (parentType == null)
                throw new ArgumentNullException("parentType");
            if (Parent != null)
            {
                Type t = Parent.GetType();
                if (t == parentType || t.IsSubclassOf(parentType))
                    return Parent;
                else if (parentType == typeof(ObjectServer))
                    return Server;
                else
                    return Parent.GetParent(parentType);
            }
            return null;
        }

        /// <summary>
        /// Parent of a File is a Collection. e.g. - FileSet is a specialized Collection.
        /// </summary>
        public Algorithm.Collection.ICollectionOnDisk Parent = null;

        private Sop.DataBlock _diskBuffer = null;

        Sop.DataBlock IInternalPersistent.DiskBuffer
        {
            get
            {
                if (_diskBuffer == null)
                    _diskBuffer = _store != null ? _store.DataBlockDriver.CreateBlock(DataBlockSize) : new Sop.DataBlock(DataBlockSize);
                return _diskBuffer;
            }
            set
            {
                if (value == null)
                    throw new ArgumentNullException("value");
                _diskBuffer = value;
            }
        }

        /// <summary>
        /// Object Store
        /// </summary>
        protected internal Algorithm.SortedDictionary.ISortedDictionaryOnDisk _store;


        internal int GetNewInMemoryId()
        {
            return System.Threading.Interlocked.Increment(ref _currentInMemoryId);
        }
        private int _currentInMemoryId;

        /// <summary>
        /// Rename this File
        /// </summary>
        /// <param name="newName"></param>
        public void Rename(string newName)
        {
            if (Server.FileSet.Contains(Name))
            {
                ((FileSet)Server.FileSet).Btree.Locker.Invoke(() =>
                {
                    ((FileSet)Server.FileSet).Btree.Remove(Name);
                    Name = newName;
                    IsDirty = true;
                    Flush();
                    ((FileSet)Server.FileSet).Btree.Add(newName, this);
                    ((FileSet)Server.FileSet).Btree.Flush();
                });
            }
        }


        protected internal bool IsClosing = false;

        /// <summary>
        /// true if File is open, else false
        /// </summary>
        public bool IsOpen
        {
            get { return _store != null && _store.Locker.Invoke(() => { return _store.IsOpen; }); }
        }

        #region Collections Pool
        /// <summary>
        /// Add Collection to the Collections' Pool
        /// </summary>
        /// <param name="collection"></param>
        public void AddToPool(Algorithm.Collection.ICollectionOnDisk collection)
        {
            lock (CollectionsPool)
            {
                if (IsClosing)
                    CollectionsPool.Remove(collection.InMemoryId);
                else
                {
                    //if (!CollectionsPool.Contains(CollName))
                    CollectionsPool[collection.InMemoryId] = collection;
                }
            }
        }
        /// <summary>
        /// Remove the Collection from Collections' Pool
        /// </summary>
        /// <param name="collection"></param>
        public void RemoveFromPool(Algorithm.Collection.ICollectionOnDisk collection)
        {
            RemoveFromPool(collection, false);
        }
        protected internal void RemoveFromPool(Algorithm.Collection.ICollectionOnDisk collection, bool willClose)
        {
            if (CollectionsPool != null)
            {
                lock (CollectionsPool)
                {
                    var cod = collection;
                    if (cod != null)
                    {
                        if (willClose)
                            cod.Close();
                        CollectionsPool.Remove(collection.InMemoryId);
                    }
                }
            }
        }
        /// <summary>
        /// Collections' Pool is a dictionary.
        /// </summary>
        internal Collections.Generic.ISortedDictionary<int, Algorithm.Collection.ICollectionOnDisk> CollectionsPool =
            new Collections.Generic.SortedDictionary<int, Algorithm.Collection.ICollectionOnDisk>();
        #endregion

        #region IFile Store overload
        Sop.ISortedDictionaryOnDisk Sop.Client.IFile.Store
        {
            get { return Store; }
        }
        Sop.IObjectServer Sop.Client.IFile.Server
        {
            get { return Server; }
        }
        #endregion
    }
}