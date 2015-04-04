// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.IO;
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.IO;
using Sop.Persistence;

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
        public File()
        {
        }

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
            return CollectionGrowthSizeInNob * (int)DataBlockSize;
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
        /// Initialize this File
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
                _store.Dispose();
                _store = null;
                if (DeletedCollections != null)
                {
                    DeletedCollections.Dispose();
                    DeletedCollections = null;
                }
                CollectionsPool = null;
                Parent = null;
                Server = null;
            }
        }

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
                long segmentSize = CollectionGrowthSizeInNob * ss;
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
                    segmentSize = Profile.CollectionSegmentSize;
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
                    justCreated = true;
                    if (Transaction != null)
                        _store =
                            ((Sop.Transaction.TransactionBase)Transaction.GetOuterChild()).CreateCollection(this);
                    else
                        _store = ObjectServer.CreateDictionaryOnDisk(this);
                }
                else
                    _store.Open();

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
                    Store.Open();
                    if (justCreated)
                    {
                        justCreated = Store.IsDirty;
                        if (justCreated && !Server.ReadOnly)
                            //** save the ObjectStore as we need the "System" Stores 
                            //** persisted right after initialization and Open...
                            Store.Flush();
                    }
                }
                if (DeletedCollections != null)
                    DeletedCollections.Open();
            }
            else if (Server.HasTrashBin)
                CreateDeletedCollections();
        }

        ///// <summary>
        ///// Re-open the File.
        ///// </summary>
        //public void ReOpen()
        //{
        //    Close();
        //    Open();
        //}

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
        private static int _fileBlockSize = 512; //8 * 1024;// 512;	//2 * 1024;

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
                SectorSize = Utility.Win32.GetDriveSectorSize(filename);
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
                systemBlockSize = Utility.Win32.GetDriveSectorSize(fname);
                this._systemDetectedBlockSize = systemBlockSize;

                if (systemBlockSize > _fileBlockSize)
                    _fileBlockSize = systemBlockSize;

                //** prepare a 1 MB buffer
                double d = (double)_fileBlockSize / systemBlockSize;
                if (d < 1)
                    d = 1;
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
                        coll.Flush();
                }
            }
            if (_store != null)
                _store.Flush();
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
                    colls[i].IsUnloading = true;
            }
            if (_store != null)
                _store.IsUnloading = true;
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
                    if (open)
                        ((IInternalFileEntity)colls[i]).OpenStream();
                    else
                        ((IInternalFileEntity)colls[i]).CloseStream();
                }
            }
            if (_store != null)
            {
                if (open)
                    ((IInternalFileEntity)_store).OpenStream();
                else
                    ((IInternalFileEntity)_store).CloseStream();
            }
        }

        /// <summary>
        /// Close the file
        /// </summary>
        public virtual void Close()
        {
            if (IsClosing)
                return;

            if (IsDirty)
                Flush();

            IsClosing = true;
            try
            {
                if (DeletedCollections != null)
                    DeletedCollections.Close();
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
            return this.Filename;
        }

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
                    return Store.FileStream;
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
        public int CollectionGrowthSizeInNob
        {
            get { return Profile.CollectionGrowthSizeInNob; }
            //set { Profile.CollectionGrowthSizeInNob = value; }
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
                DeletedCollections = new FileRecycler(this);
                DeletedCollections.Open();
            }
        }

        private Profile _profile;

        /// <summary>
        /// Contains B-Tree profile for this file object
        /// </summary>
        public Profile Profile
        {
            get { return _profile ?? (_profile = new Profile()); }
            internal set { _profile = value; }
        }

        public int CollectionCounter { get; set; }

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

        Sop.Client.IObjectServer Sop.Client.IFile.Server
        {
            get { return Server; }
        }

        /// <summary>
        /// Return the size on disk(in bytes) of this object
        /// </summary>
        public int HintSizeOnDisk { get; private set; }

        /// <summary>
        /// true means File is new, otherwise false
        /// </summary>
        public bool IsNew { get; set; }

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
            //Writer.Write((short)Profile.ProfileScheme);
            //Writer.Write((int)((ServerProfile)Profile).DataBlockSize);
            //Writer.Write(((ServerProfile)Profile).BTreeSlotLength);
            //Writer.Write(((ServerProfile)Profile).CollectionSegmentSize);
            //Writer.Write(((ServerProfile)Profile).HasTrashBin);

            //writer.Write(CollectionGrowthSizeInNob);
            //bool HasDeletedBlocks = DeletedCollections != null;
            bool hasDeletedBlocks = DeletedCollections != null;
            writer.Write(hasDeletedBlocks);
            if (hasDeletedBlocks)
            {
                if (DeletedCollections.DataAddress < 0)
                    DeletedCollections.Flush();
                writer.Write(DeletedCollections.DataAddress);
            }
            writer.Write(_store != null);
            if (_store != null)
            {
                if (_store.IsOpen && _store.IsDirty)
                {
                    _store.RegisterChange();
                    _store.Flush();
                }
                writer.Write(_store.DataAddress);
            }
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
                //** c://SopBin/o.dta -> c://SopBin3/o.dta
                string nameOfFile = Filename.Substring(Server.HomePath.Length);
                string newFilename = string.Format("{0}{1}", Server.Path, nameOfFile);
                if (System.IO.File.Exists(newFilename))
                    Filename = newFilename;
            }

            //** Read the profile info...
            //ProfileSchemeType ps = (ProfileSchemeType)Reader.ReadInt16();
            //DataBlockSize dbs = (DataBlockSize)Reader.ReadInt32();
            //Profile.ProfileScheme = ps;
            //Profile.DataBlockSize = dbs;
            Profile = new Profile(((Algorithm.Collection.ICollectionOnDisk)parent).File.Profile);

            //CollectionGrowthSizeInNob = reader.ReadInt32();

            bool hasDeletedBlocks = reader.ReadBoolean();
            long collectionRecycler = -1;
            if (hasDeletedBlocks)
                collectionRecycler = reader.ReadInt64();
            bool deserializeObjectStore = reader.ReadBoolean();
            if (deserializeObjectStore)
            {
                _store = Transaction != null ? ((Transaction.TransactionBase)Transaction).CreateCollection(this) :
                    ObjectServer.CreateDictionaryOnDisk(this);
                _store.DataAddress = reader.ReadInt64();
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
            get { return _diskBuffer ?? (_diskBuffer = _store != null ? _store.DataBlockDriver.CreateBlock(DataBlockSize) : new Sop.DataBlock(DataBlockSize)); }
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

        /// <summary>
        /// Add Collection to the Collections' Pool
        /// </summary>
        /// <param name="collection"></param>
        public void AddToPool(Algorithm.Collection.ICollectionOnDisk collection)
        {
            if (IsClosing)
                CollectionsPool.Remove(collection.InMemoryId);
            else
            {
                //if (!CollectionsPool.Contains(CollName))
                CollectionsPool[collection.InMemoryId] = collection;
            }
        }

        internal int GetNewInMemoryId()
        {
            lock (this)
            {
                return ++_currentInMemoryId;
            }
        }
        private int _currentInMemoryId;

        /// <summary>
        /// Remove the Collection from Collections' Pool
        /// </summary>
        /// <param name="collection"></param>
        public void RemoveFromPool(Algorithm.Collection.ICollectionOnDisk collection)
        {
            RemoveFromPool(collection, false);
        }

        /// <summary>
        /// Rename this File
        /// </summary>
        /// <param name="newName"></param>
        public void Rename(string newName)
        {
            if (Server.FileSet.Contains(Name))
            {
                ((FileSet)Server.FileSet).Btree.Remove(Name);
                Name = newName;
                IsDirty = true;
                Flush();
                ((FileSet)Server.FileSet).Btree.Add(newName, this);
                ((FileSet)Server.FileSet).Btree.Flush();
            }
        }

        protected internal void RemoveFromPool(Algorithm.Collection.ICollectionOnDisk collection, bool willClose)
        {
            if (CollectionsPool != null)
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

        protected internal bool IsClosing = false;

        /// <summary>
        /// true if File is open, else false
        /// </summary>
        public bool IsOpen
        {
            get { return _store != null && _store.IsOpen; }
        }

        /// <summary>
        /// Collections' Pool is a dictionary.
        /// </summary>
        internal Collections.Generic.ISortedDictionary<int, Algorithm.Collection.ICollectionOnDisk> CollectionsPool =
            new Collections.Generic.SortedDictionary<int, Algorithm.Collection.ICollectionOnDisk>();

        Sop.ISortedDictionaryOnDisk Sop.Client.IFile.Store
        {
            get { return Store; }
        }
    }
}