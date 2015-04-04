using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Caching;
using System.Threading;

namespace Sop.Caching
{
    /// <summary>
    /// Virtual Cache Base.
    /// </summary>
    public abstract class VirtualCacheBase : ObjectCache, IDisposable
    {
        abstract public void Dispose();

        #region Cache Entries' Update and Remove callbacks
        /// <summary>
        /// Cache Entry Set update callback. Set this to your application callback to be called
        /// when one or more Cache Entries are about to expire.
        /// </summary>
        public event CacheEntrySetUpdateCallback OnCacheEntrySetUpdate
        {
            add { CacheEntrySetUpdateCallback += value; }
            remove { CacheEntrySetUpdateCallback -= value; }
        }
        protected CacheEntrySetUpdateCallback CacheEntrySetUpdateCallback;

        /// <summary>
        /// Cache Entry Set removed callback. Set this to your application callback to be called
        /// when one or more Cache Entries were expired and removed from the cache.
        /// </summary>
        public event CacheEntrySetRemovedCallback OnCacheEntrySetRemoved
        {
            add { CacheEntrySetRemovedCallback += value; }
            remove { CacheEntrySetRemovedCallback -= value; }
        }
        protected CacheEntrySetRemovedCallback CacheEntrySetRemovedCallback;
        #endregion

        /// <summary>
        /// Allows external code to set its method to get date time.
        /// NOTE: this allows unit test code to set date time to some
        /// test driven values.
        /// </summary>
        public Func<long, long> GetCurrentDate
        {
            get { return _getCurrentDate; }
            set { _getCurrentDate = value; }
        }
        private Func<long, long> _getCurrentDate = (timeOffsetInTicks) => DateTimeOffset.UtcNow.Ticks + timeOffsetInTicks;

        /// <summary>
        /// true will persist cached data, false (default) will only use disk to extend memory 
        /// capacity and reset the disk storage during application start.
        /// </summary>
        public static bool Persisted
        {
            get
            {
                return _persisted != null && _persisted.Value;
            }
            set
            {
                if (_persisted != null)
                    throw new SopException("Persisted property can only be set once.");
                _persisted = value;
            }
        }
        private static bool? _persisted;

        /// <summary>
        /// Virtual Cache Constructor. Virtual Cache now defaults to being a Memory Extender.
        /// I.e. - Persisted static property defaults to false. Set VirtualCache.Persisted to true
        /// if wanting to persist the cached data across application runs.
        /// </summary>
        /// <param name="storePath">Data Store URI path or the Store name.</param>
        /// <param name="clusteredData">true (default) will configure the Store to save
        /// data together with the Keys in the Key Segment (a.k.a. - clustered), 
        /// false will configure Store to save data in its own Data Segment. For small to medium sized
        /// data, Clustered is recommended, otherwise set this to false.</param>
        /// <param name="storeFileName">Valid complete file path where to create the File to contain the data Store. 
        /// It will be created if it does not exist yet. Leaving this blank will create the Store within the default 
        /// SOP SystemFile, or in the referenced File portion of the storePath, if storePath has a directory path.</param>
        /// <param name="fileConfig">File Config should contain description how to manage data allocations on disk and B-Tree 
        /// settings for all the Data Stores of the File. If not set, SOP will use the default configuration.</param>
        public VirtualCacheBase(string storePath, bool clusteredData = true, string storeFileName = null, Sop.Profile fileConfig = null)
        {
            if (string.IsNullOrWhiteSpace(storePath))
                throw new ArgumentNullException("storePath");

            Initialize(fileConfig, Persisted);
            Server.SystemFile.Store.Locker.Invoke(() =>
            {
                SetupStoreFile(storePath, storeFileName, fileConfig);
                _store = Server.StoreNavigator.GetStorePersistentKey<CacheKey, CacheEntry>(storePath,
                    new StoreParameters<CacheKey>
                    {
                        IsDataInKeySegment = clusteredData,
                        AutoFlush = !clusteredData,
                        StoreKeyComparer = new CacheKeyComparer(),
                        MruManaged = false
                    });

                Logger = new Log.Logger(string.Format("{0}{2}{1}.txt", _server.Path, _store.Name, System.IO.Path.DirectorySeparatorChar));
                Logger.Verbose("Created/retrieved store {0}.", _store.Name);
            });
        }

        #region SOP data Store related

        /// <summary>
        /// Virtual Cache logger useful for generating analyzable execution history & details.
        /// </summary>
        public Log.Logger Logger;

        /// <summary>
        /// Object Server Filename. Set this to your desired full path
        /// and filename which will store the VirtualCache managed data.
        /// If left unspecified, SOP will auto-create a data file
        /// within the vicinity of the Sop.dll assembly folder.
        /// It will check/find "App_Data" folder or the Assembly's
        /// outer folder and create the default (OServer.dta) data file there.
        /// </summary>
        public static string ServerFilename
        {
            get
            {
                return _serverFilename;
            }
            set
            {
                if (!string.IsNullOrWhiteSpace(_serverFilename))
                    throw new Server.SopServerException("Can't update VirtualCache.ServerFilename once it has been set or when the Registry DB has been created.");
                _serverFilename = value;
            }
        }
        protected static string _serverFilename;

        protected static IObjectServer Server
        {
            get
            {
                return _server;
            }
        }
        // Initialize the Registry DB (SOP ObjectServer & Data file).
        protected static void Initialize(Sop.Profile fileConfig = null, bool persisted = true)
        {
            // commit every 10 minutes.
            if (CommitInterval == null)
            {
                CommitInterval = new TimeSpan(0, 10, 0);
                //#if (DEBUG)
                //                CommitInterval = new TimeSpan(0, 1, 0);
                //#else
                //                CommitInterval = new TimeSpan(0, 10, 0);
                //#endif
            }
            if (_server != null)
                return;
            lock (_serverLocker)
            {
                // create the SOP Server file for use as registry...
                if (_server != null) return;
                if (fileConfig == null)
                    fileConfig = new Profile();
                fileConfig.MemoryExtenderMode = !persisted;
                _server = new Sop.ObjectServer(ServerFilename, false, fileConfig);
                if (string.IsNullOrWhiteSpace(ServerFilename))
                    ServerFilename = _server.Filename;
                //#if (DEBUG)
                //                Log.Logger.Instance = new Log.Logger(string.Format("{0}\\SopLog.txt", _server.Path));
                //                Log.Logger.Instance.LogLevel = Log.LogLevels.Verbose;
                //#endif
            }
        }
        protected static IObjectServer _server;
        protected static object _serverLocker = new object();

        protected Sop.ISortedDictionary<CacheKey, CacheEntry> _store;

        private void SetupStoreFile(string storePath, string storeFileName, Sop.Profile fileConfig)
        {
            // sample storePath and storeFilename:
            // storePath = "MyStore", storeFilename = null
            // storePath = "MyDataFile/MyStore", storeFilename = "f://CacheData/MyDataFile.dta"
            // storePath = "MyDataFile/MyStore", storeFilename = null
            if (!string.IsNullOrWhiteSpace(storeFileName))
            {
                // if File was referenced in the storePath && the storeFilename was specified, 
                // configure/create this File to contain the Store.
                string[] parts;
                if (!_server.StoreNavigator.TryParse(storePath, out parts))
                    throw new ArgumentException(string.Format("storePath {0} is not a valid Store URI path.", storePath));
                if (parts.Length > 1)
                {
                    var f = _server.GetFile(parts[0]);
                    if (f == null)
                    {
                        // delete the storeFilename if it exists but is not registered in the SystemFile...
                        if (Sop.Utility.Utility.FileExists(storeFileName))
                            Sop.Utility.Utility.FileDelete(storeFileName);
                    }
                    // create the storeFilename and use it to contain the Store referenced in storePath.
                    _server.FileSet.Add(parts[0], storeFileName, fileConfig);
                }
            }
        }

        /// <summary>
        /// Commit Interval.
        /// Data Store is transactional and changes are commited to disk
        /// after every period (CommitInterval value) had elapsed.
        /// </summary>
        static public TimeSpan? CommitInterval { get; set; }
        #endregion
    }
}
