// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)


//#define TRIAL

using System;
using System.Collections;
using System.Security;
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.File;
using Sop.OnDisk.IO;
using Sop.Persistence;
using Sop.Transaction;
using Sop.Utility;

#region History Log

/*
 * 3/29/2003 - I found my Holy Grail of narra:
 * - BTree index and data are optionally saved on either every node or index on node and data on data segment
 * - ObjectServer will use 1 PC if capable performance wise
 *		- moment perf degrades beyond maximum threshold, will start index/data distribution to nearby fastest PC
 *		- distribution starts w/ node(s)
 *		- we'll not distribute as much to maintain server perf
 *		- every so often distribution resumes
 *		- distribution "climbs up" the tree. ie - starts w/ the outermost node then on next distribution event, 
 *			next level up and so on so forth until perf stabilizes on current PC
 *		- if insertion tends to occur on the linked PC then distribution will not resume on next distribution event
 *			as perf is stable in this case. We'll find the right formula to balance this
 *		- each pc on the Grid can be replicated to improve parallel serving perf. ie - more clients can be served if
 *			more than 1 PC can serve them.
 *		- still the same plan, all Grid activities are mirrored capable so as to be able to maintain mirror copy of the
 *			overall data. When a PC retires from being member of Grid, then Grid can distribute the retiring PC's
 *			data to the remaining members of the Grid w/o interruption.
 *		- if a new member joins in, this new member PC will get the priority as destination of the retired PC's data
 */

#endregion

[assembly: AllowPartiallyTrustedCallers]

namespace Sop.OnDisk
{
    /// <summary>
    /// Object Very Large Database Server (ObjectServer).
    /// It takes full advantage of the Grid resources.
    ///
    /// Grid Tree:
    /// When insertion made, check if disk/mem full, distribute half of the tree to another Computer. ie, get the sub-tree(s)
    /// of root and move them to the other PC.
    /// When deletion made, check whether there are PCs' whose loads can be combined to 1 PC to 
    /// optimize performance.
    /// 
    /// -ObjectServer can have multiple instances on the same PC, so, no internal singleton please..
    /// </summary>
    internal class ObjectServer : IInternalPersistent, IFileEntity, IObjectServer
    {
        /// <summary>
        /// System File Serializer
        /// </summary>
        internal class SystemFileSerializer
        {
            public long FileSize;
            public string HomePath;

            public override string ToString()
            {
                return string.Format("{0};{1}", FileSize, HomePath);
            }

            public static SystemFileSerializer FromString(string s)
            {
                if (string.IsNullOrEmpty(s)) return new SystemFileSerializer();
                string[] parts = s.Split(new[] {";"}, StringSplitOptions.None);
                if (parts.Length < 2) return new SystemFileSerializer();
                var r = new SystemFileSerializer();
                long.TryParse(parts[0], out r.FileSize);
                r.HomePath = parts[1];
                return r;
            }
        }

        /// <summary>
        /// Object Server literal text
        /// </summary>
        public const string ObjectServerLiteral = "ObjectServer";

        /// <summary>
        /// Default Constructor
        /// </summary>
        public ObjectServer()
        {
            Encoding = System.Text.Encoding.UTF8;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="transLogger"> </param>
        /// <param name="profileScheme"> </param>
        /// <param name="readOnly"> </param>
        public ObjectServer(string filename,
                            Transaction.TransactionRoot transLogger,
                            Preferences preferences = null,
                            bool readOnly = false)
            : this()
        {
            if (transLogger != null && readOnly)
                throw new ArgumentException("Transaction isn't supported and isn't needed on ReadOnly mode.");
            this.ReadOnly = readOnly;
            if (preferences != null)
            {
                this.TrashBinType = preferences.TrashBinType;
                Encoding = preferences.Encoding;
            }
            this.Profile = new Sop.Profile(preferences);
            Transaction = transLogger;
            if (transLogger != null)
                transLogger.Server = this;
            Initialize(filename);
            if (string.IsNullOrEmpty(GenericLogger.DefaultLogFilename))
                GenericLogger.DefaultLogFilename = string.Format("{0}\\BTreeLog", this.Path);
        }

        public static string GetRootPath(string serverSystemFilename)
        {
            if (string.IsNullOrEmpty(serverSystemFilename))
                return string.Empty;
            var path = System.IO.Path.GetDirectoryName(serverSystemFilename);
            if (path == string.Empty)
                path = System.Environment.CurrentDirectory;
            path += System.IO.Path.DirectorySeparatorChar;
            return path;
        }

        /// <summary>
        /// Object Server Data file Inf extension literal.
        /// </summary>
        public const string DataInfExtensionLiteral = "inf";

        private void ReadSystemFileAttributes()
        {
            //** retrieve the File size from Xml file
            string xmlFilename = string.Format("{0}.{1}", SystemFile.Filename, DataInfExtensionLiteral);
            if (System.IO.File.Exists(xmlFilename))
            {
                try
                {
                    using (var sr = new System.IO.StreamReader(xmlFilename))
                    {
                        string s = sr.ReadToEnd();
                        _systemFileInfo = SystemFileSerializer.FromString(s);
                        ((File.IFile) SystemFile).Size = _systemFileInfo.FileSize;
                        HomePath = _systemFileInfo.HomePath;
                    }
                }
                catch
                {
                } //** ignore exception as system file size will be computed from file size itself if not provided...
            }
        }

        private void WriteSystemFileAttributes()
        {
            if (_systemFileInfo.FileSize != 0 &&
                _systemFileInfo.FileSize == ((File.IFile) SystemFile).Size)
                return;
            string xmlFilename = string.Format("{0}.{1}", SystemFile.Filename, DataInfExtensionLiteral);
            try
            {
                if (Transaction != null)
                    ((TransactionRoot)Transaction).RegisterOpenFile(xmlFilename);
                using (var sw = new System.IO.StreamWriter(xmlFilename))
                {
                    _systemFileInfo.FileSize = ((File.IFile) SystemFile).Size;
                    _systemFileInfo.HomePath = Path;
                    sw.Write(_systemFileInfo.ToString());
                }
            }
            catch (Exception)
            {
            } //** ignore for now any error writing the SystemFile Size
        }

        private SystemFileSerializer _systemFileInfo = new SystemFileSerializer();
        internal string HomePath;

        /// <summary>
        /// Initialize this Object Server. NOTE: this is created for SOP internal use only.
        /// </summary>
        /// <param name="filename"></param>
        public void Initialize(string filename)
        {
            if (string.IsNullOrEmpty(filename))
                throw new ArgumentNullException("filename");

            if (!ReadOnly && !Sop.Utility.Utility.HasRequiredAccess(filename))
                throw new InvalidOperationException(string.Format("No Write access on file '{0}'.", filename));

            if (string.IsNullOrEmpty(Path))
                Path = GetRootPath(filename);

            //** Create/Open the System File object
            if (SystemFile == null)
            {
                if (Transaction != null)
                    SystemFile =
                        ((Sop.Transaction.TransactionBase)
                         ((Sop.Transaction.ITransactionLogger) Transaction).GetOuterChild()).CreateFile();
                else
                    SystemFile = new File.File();
            }
            if (Profile == null)
            {
                Profile = new Profile();
            }
            //90;
            //ProfileScheme.IsDataInKeySegment = true;
            ((File.File)SystemFile).Profile = Profile;
            ((IInternalPersistent)SystemFile).DiskBuffer.DataAddress = 0;
            ((File.IFile) SystemFile).Initialize(this, "SystemFile", filename, AccessMode.ReadWrite, null);
            ReadSystemFileAttributes();
            Open();
        }

        /// <summary>
        /// If Filename is not a complete path, returns Filename
        /// relative to this ObjectServer's Path.
        /// Otherwise return Filename unchanged.
        /// </summary>
        /// <param name="filename"></param>
        /// <returns></returns>
        public string NormalizePath(string filename)
        {
            if (string.IsNullOrEmpty(filename))
                return string.Empty;
            string path = System.IO.Path.GetDirectoryName(filename);
            if (path == string.Empty)
                return this.Path + filename;
            return filename;
        }

        /// <summary>
        /// If Filename is not a complete path, returns Filename
        /// relative to the passed in Server Path. Otherwise return Filename unchanged.
        /// </summary>
        /// <param name="serverPath"></param>
        /// <param name="filename"></param>
        /// <returns></returns>
        public static string NormalizePath(string serverPath, string filename)
        {
            string path = System.IO.Path.GetDirectoryName(filename);
            if (path == string.Empty)
            {
                string s = System.IO.Path.GetDirectoryName(serverPath);
                if (!string.IsNullOrEmpty(s))
                    return s + filename;
            }
            return filename;
        }

        /// <summary>
        /// Object Server's Profile Scheme. Defaults to Embedded device
        /// profile scheme. Select Server or Enterprise scheme if wanting
        /// to utilize more of this server's memory and to allocate
        /// BIG Data Segments during Collection on disk's grow event.
        /// </summary>
        public Profile Profile
        {
            get { return _profile; }
            set { _profile= value; }
        }
        private Profile _profile = new Profile();

        /// <summary>
        /// Create Dictionary On Disk
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static Algorithm.SortedDictionary.ISortedDictionaryOnDisk CreateDictionaryOnDisk(File.IFile file)
        {
            return CreateDictionaryOnDisk(file, new BTreeDefaultComparer());
        }

        /// <summary>
        /// Create Dictionary On Disk
        /// </summary>
        /// <param name="file"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        public static Algorithm.SortedDictionary.ISortedDictionaryOnDisk CreateDictionaryOnDisk(File.IFile file, IComparer comparer)
        {
            return CreateDictionaryOnDisk(file, comparer, string.Empty, file.Profile.IsDataInKeySegment);
        }

        public static Algorithm.SortedDictionary.ISortedDictionaryOnDisk CreateDictionaryOnDisk(File.IFile file, string name)
        {
            return CreateDictionaryOnDisk(file, null, name, file.Profile.IsDataInKeySegment);
        }

        /// <summary>
        /// Create Dictionary On Disk
        /// </summary>
        /// <param name="file"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        /// <returns></returns>
        public static Algorithm.SortedDictionary.ISortedDictionaryOnDisk CreateDictionaryOnDisk(File.IFile file, IComparer comparer, string name,
                                                                     bool isDataInKeySegment)
        {
            return
                (Algorithm.SortedDictionary.ISortedDictionaryOnDisk)
                ObjectFactory.Instance.CreateDictionaryOnDisk(file, comparer, name, isDataInKeySegment);
        }

        internal void dispose(bool isDisposing)
        {
            if (Encoding != null)
            {
                if (IsOpen)
                {
                    if (!isDisposing)
                    {
                        if (_fileset is IInternalFileEntity)
                            ((IInternalFileEntity) _fileset).CloseStream();
                        ((IInternalFileEntity) SystemFile).CloseStream();
                    }
                    else
                        this.Close();
                }
                if (_fileset != null)
                {
                    if (isDisposing)
                        _fileset.Dispose();
                    this._fileset = null;
                }
                if (SystemFile != null)
                {
                    if (isDisposing)
                        SystemFile.Dispose();
                    this.SystemFile = null;
                }
            }
        }

        /// <summary>
        /// Dispose this Object Server
        /// </summary>
        public void Dispose()
        {
            dispose(true);
        }

        /// <summary>
        /// Server root path.
        /// NOTE: set is for SOP used internally
        /// </summary>
        public string Path { get; set; }

        /// <summary>
        /// IsDirty tells BTree whether this object needs to be rewritten to disk(dirty) or not
        /// </summary>
        public bool IsDirty
        {
            get { return _isDirty || _fileset != null && _fileset.IsDirty; }
            set { _isDirty = value; }
        }

        private bool _isDirty;

        /// <summary>
        /// Server encoding.
        /// </summary>
        public System.Text.Encoding Encoding { get; set; }

        /// <summary>
        /// true if Object Server is open, otherwise false
        /// </summary>
        public bool IsOpen
        {
            get { return this.SystemFile != null && ((File.IFile) SystemFile).IsOpen; }
        }

        /// <summary>
        /// true means File is new, otherwise false
        /// </summary>
        public bool IsNew { get; set; }

        /// <summary>
        /// Save changes made to all Files including System File.
        /// </summary>
        public void Flush()
        {
            if (ReadOnly)
                throw new InvalidOperationException("Can't save, Object Server is in Read Only mode.");
            WriteSystemFileAttributes();
            FileSet.Flush();
            if (((File.IFile) SystemFile).DeletedCollections != null)
                ((File.IFile) SystemFile).DeletedCollections.Flush();
            if (SystemFile.Store != null)
            {
                if (HasTrashBin)
                    SystemFile.Store[SystemFileDeletedCollectionsLiteral] =
                        ((File.IFile) SystemFile).DeletedCollections.DataAddress;
#if (TRIAL)
				if (!SystemFile.ObjectStore.Contains("_SystemCreateDate"))
					SystemFile.ObjectStore["_SystemCreateDate"] = DateTime.Now;
#endif
                SaveLicense();
            }
            SystemFile.Flush();
        }

        /// <summary>
        /// Unload this Object Server
        /// </summary>
        public virtual void Unload()
        {
            if (IsOpen)
            {
                File.IFileSet fs = _fileset;
                if (fs != null)
                {
                    fs.MarkNotDirty();
                    fs.Close();
                }
                ((File.IFile) SystemFile).MarkNotDirty();
                ((File.IFile) SystemFile).Close();
            }
        }

        private const string SystemFileDeletedCollectionsLiteral = "__System_FileDeletedCollectionsAddress";

        /// <summary>
        /// Write Logs or not
        /// </summary>
        public bool WriteLogs { get; set; }

        /// <summary>
        /// Open the Object Server
        /// </summary>
        public virtual void Open()
        {
            if (!IsOpen)
            {
                ((File.IFile) SystemFile).Open();
                object o = SystemFile.Store[SystemFileDeletedCollectionsLiteral];
                if (o != null && o is long)
                {
                    if (HasTrashBin)
                    {
                        ((File.IFile) SystemFile).DeletedCollections.DataAddress = (long) o;
                        ((File.IFile) SystemFile).DeletedCollections.Load();
                    }
#if (TRIAL)
					if (!SystemFile.ObjectStore.Contains("_SystemCreateDate"))
						SystemFile.ObjectStore["_SystemCreateDate"] = DateTime.Now;
					DateTime dt = (DateTime)SystemFile.ObjectStore["_SystemCreateDate"];
					if (DateTime.Now.Subtract(dt).TotalDays > 30)
						throw new InvalidOperationException(
							"This trial version of BTreeGold v4.0 - SOP is expired. Visit www.4atech.net for details on getting your production license."
							);
#endif
                    ValidateLicense();
                }

                File.IFileSet fs = (File.IFileSet) FileSet;
                if (fs != null)
                    fs.Open();

                if (CollectionOnDisk.Session != null)
                    CollectionOnDisk.Session.Register(this);
            }
        }

        internal const string SystemKeyToken = "__System_";

        private static readonly string LicenseKeyName = string.Format("{0}{1}", SystemKeyToken, Crypto.EncryptString("LicenseKey", SecretWord));
        private const string SecretWord = "TaalVolcano";

        private void ValidateLicense()
        {
            if (!SystemFile.Store.Contains(LicenseKeyName))
                SystemFile.Store[LicenseKeyName] = _LicenseKey;
            string readLicenseKey = (string) SystemFile.Store[LicenseKeyName];
            if (readLicenseKey != _LicenseKey)
                throw new InvalidOperationException("License Key wasn't provided or is invalid.");
        }

        private void SaveLicense()
        {
            if (!_licenseSaved && !SystemFile.Store.Contains(LicenseKeyName))
            {
                SystemFile.Store[LicenseKeyName] = _LicenseKey;
                SystemFile.Store[FileVersionLiteral] = FileVersion;
                _licenseSaved = true;
            }
        }

        private const string FileVersionLiteral = "__System_SOP File Version";
        private const string FileVersion = "4.1";

        private bool _licenseSaved;

        /// <summary>
        /// Close the Object Server
        /// </summary>
        public virtual void Close()
        {
            if (!IsOpen) return;
            _licenseSaved = false;
            if (HomePath != Path)
                WriteSystemFileAttributes();
            File.IFileSet fs = _fileset;
            if (fs != null)
                fs.Close();
            ((File.IFile) SystemFile).Close();

            if (CollectionOnDisk.Session != null)
                CollectionOnDisk.Session.UnRegister(this);
        }

        private ITransactionRoot _instanceTransaction;
        [ThreadStatic] internal static ITransactionRoot transaction;

        /// <summary>
        /// Transaction Logger holds the 2ndary logger to text file.
        /// NOTE: this is set with the TransactionLogger param received
        /// on the Constructor.
        /// </summary>
        public ITransaction Transaction
        {
            get { return _instanceTransaction ?? (_instanceTransaction = transaction); }
            set
            {
                if (_instanceTransaction == value) return;
                if (value != null &&
                    _instanceTransaction != null &&
                    (int) _instanceTransaction.CurrentCommitPhase < (int) Sop.Transaction.CommitPhase.Committed)
                    throw new InvalidOperationException(
                        "Can't assign another transaction, there is already a transaction assigned. " +
                        "Complete the transaction before assigning a new one.");
                transaction = _instanceTransaction = (ITransactionRoot) value;
            }
        }

        /// <summary>
        /// Returns the File object with its Name equal to Name argument
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public IFile GetFile(string name)
        {
            File.IFile r = (File.IFile) SystemFile;
            if (r.Name == name || r.Filename == name)
                return r;
            r = (File.IFile) FileSet[name];
            if (r == null)
            {
                string s = NormalizePath(name);
                r = (File.IFile) FileSet[s];
            }
            return r;
        }
        Client.IFile Client.IObjectServer.GetFile(string name) { return GetFile(name); }

        /// <summary>
        /// Returns the default file extension of the Object Server
        /// </summary>
        public const string DefaultFileExtension = "dta";

        /// <summary>
        /// Return the FileSet of the ObjectServer
        /// </summary>
        public IFileSet FileSet
        {
            get
            {
                if (_fileset == null)
                {
                    //** create a File Set on the System file.
                    _fileset = (FileSet) SystemFile.Store[File.FileSet.FileSetLiteral];
                    if (_fileset == null)
                    {
                        if (Transaction != null)
                            _fileset =
                                ((TransactionBase)
                                 ((ITransactionLogger) Transaction).GetOuterChild()).CreateFileSet(
                                     (File.IFile) SystemFile);
                        else
                        {
                            _fileset = new FileSet((File.IFile)SystemFile);
                            _fileset.Open();
                            SystemFile.Store[File.FileSet.FileSetLiteral] = _fileset;
                        }
                    }
                }
                return _fileset;
            }
        }
        Client.IFileSet Client.IObjectServer.FileSet { get { return FileSet; } }

        /// <summary>
        /// Type Store is the typing system of the Server.
        /// It allows type registrationg and object instantiation based
        /// on registered type id
        /// </summary>
        public TypeStore TypeStore
        {
            get { return _typeStore; }
        }
        private readonly TypeStore _typeStore = new TypeStore();

        /// <summary>
        /// Filename returns the Server's System Filename
        /// </summary>
        public string Filename
        {
            get
            {
                return SystemFile == null ? string.Empty : SystemFile.Filename;
            }
        }

        /// <summary>
        /// Returns Filename as Name of this Entity
        /// </summary>
        public string Name
        {
            get
            {
                return SystemFile == null ? string.Empty : SystemFile.Name;
            }
        }

        public Sop.ISortedDictionary<string, string> StoreTypes
        {
            get
            {
                if ((Profile.TrackStoreTypes != null && Profile.TrackStoreTypes.Value) ||
                    SystemFile.Store.Contains(StoreTypesLiteral))
                {
                    if (_createStoreLogs == null || _createStoreLogs.IsDisposed)
                    {
                        var sf = new StoreFactory();
                        _createStoreLogs = sf.Get<string, string>(SystemFile.Store, StoreTypesLiteral,
                                                      isDataInKeySegment: false);
                    }
                    return _createStoreLogs;
                }
                return null;
            }
        }
        private Sop.ISortedDictionary<string, string> _createStoreLogs;

        /// <summary>
        /// Returns the Create Store Logs literal text.
        /// </summary>
        public static readonly string StoreTypesLiteral = string.Format("{0}StoreTypes", SystemKeyToken);

        /// <summary>
        /// System File
        /// </summary>
        public IFile SystemFile { get; private set; }
        Client.IFile Client.IObjectServer.SystemFile { get { return SystemFile; } }

        internal static string _LicenseKey = Crypto.EncryptString("FREE LICENSE FROM 4A", SecretWord);

        /// <summary>
        /// License Key
        /// </summary>
        public static string LicenseKey
        {
            get { return Crypto.DecryptString(_LicenseKey, SecretWord); }
            set { _LicenseKey = Crypto.EncryptString(value, SecretWord); }
        }

        /// <summary>
        /// Set to false in order to turn off Trash Bin and save space.
        /// NOTE: HasTrashBin = false is only for Applications that
        /// don't intend to delete any item, e.g. - Document Indexing
        /// Applications.
        /// </summary>
        public bool HasTrashBin
        {
            get { return !ReadOnly && TrashBinType != TrashBinType.Nothing; }
        }

        /// <summary>
        /// Trash Bin Type.
        /// </summary>
        public TrashBinType TrashBinType
        {
            get { return _trashBinType; }
            set { _trashBinType = value; }
        }
        private TrashBinType _trashBinType = TrashBinType.Default;

        private File.IFileSet _fileset = null;
        //private AccessMode AccessMode = AccessMode.ReadWrite;

        #region IInternalPersistent Members

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
        /// true will open the Files in Read Only mode, otherwise R/W.
        /// This is useful for Applications where they want to open SOP data
        /// files in Read only mode, not requiring Write access which is
        /// Admin friendly
        /// </summary>
        public bool ReadOnly { get; internal set; }

        /// <summary>
        /// Return the size on disk(in bytes) of this object
        /// </summary>
        public int HintSizeOnDisk { get; private set; }

        public void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            throw new InvalidOperationException("Unimplemented");
        }

        public void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            throw new InvalidOperationException("Unimplemented");
        }

        public Sop.DataBlock DiskBuffer
        {
            get { throw new InvalidOperationException("Unimplemented"); }
            set { throw new InvalidOperationException("Unimplemented"); }
        }

        #endregion

        #region Unreferenced interface methods
        ITransaction Sop.IObjectServer.BeginTransaction()
        {
            return null;
        }
        void Sop.IObjectServer.Commit()
        {
        }
        void Sop.IObjectServer.Rollback()
        {
        }
        #endregion
    }
}