// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)


#region Transaction "handling"
/**
 *      12/14/08
 *      - Transaction actions (Add, FileGrowth and Save) tracking needs to be
 *      recursive. SOP should track all transactions in a list, destroy on Commit or
 *      complete rollback. On startup, should be empty and IF not, SOP should rollback
 *      each transaction in list to complete the Rollback session.
 * 
 *      12/06/08
        - Save Indexed Transaction Logs data on text log file for rollback.
        - Overwrite the text log file for each new activity on the Indexed Log file
        - If crashed during saving to text log file, ignore the text log file changes (indexed log file is intact)
            - proceed to rollback everything on the indexed transaction log
        - If crashed during saving to indexed log file, on next run restore the saved blocks on text log file
            - proceed to rollback everything on the indexed transaction log
        - If no crash, then no issue. Commit is completed, Rollback is completed.
 */
#endregion

using System;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using System.Linq;
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.OnDisk.DataBlock;
using Sop.OnDisk.IO;
using FileStream = Sop.OnDisk.File.FileStream;

namespace Sop.Transaction
{
    using OnDisk;
    using System.Text;
    using Synchronization;

    /// <summary>
    /// Transaction management class.
    /// Our transaction model:
    /// 1) Two phase commit: 
    ///		- 1st phase, save all transaction changed records in the 
    /// collection on disk's transaction segment. Save the transaction log
    ///		- 2nd phase, update the changed records' current pointers to reference
    /// the updated records in transaction segment.
    /// 2) Mark the transaction as completed. ie - delete the transaction table records,
    /// </summary>
    internal partial class Transaction : TransactionBase
    {
        /// <summary>
        /// true will use disk based transaction meta logging, otherwise in-memory.
        /// Default is in-memory AND disk based has yet to prove its need... probably
        /// in super large transaction where changes are in the span of Terabytes of
        /// non-contiguous blocks, which we haven't seen/imagined to be used yet AS
        /// SOP merges contiguous blocks as possible to minimize entries & fragmentation.
        /// </summary>
        public static bool DiskBasedMetaLogging;

        /// <summary>
        /// Dispose all resources and close all transaction opened collections/files.
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            if (_logger != null)
            {
                _logger.Dispose();
                _logger = null;
            }
            _appendLogger = null;
            ModifiedCollections = null;
            _updateLogger = null;
        }

        public override bool IsDisposing
        {
            get
            {
                if (Parent != null)
                    return ((TransactionBase)Parent).IsDisposing;
                return base.IsDisposing;
            }
            set
            {
                if (Parent != null)
                    ((TransactionBase)Parent).IsDisposing = value;
                base.IsDisposing = value;
            }
        }

        public virtual void Initialize(ObjectServer server)
        {
            if (server == null)
                throw new ArgumentNullException("server");
            if (DiskBasedMetaLogging)
                throw new NotImplementedException("Disk Based Meta Logging isn't supported in this SOP release.");

            Server = server;
            lock (server)
            {
                //** increment number of ongoing transactions
                Interlocked.Increment(ref Count);
                //** note: we only support up to int.MaxValue number of active transactions at any given time.
                do
                {
                    Id = _counter++;
                } while (Id == 1 &&
                         Sop.Utility.Utility.FileExists(server, (string.Format("{0}{1}{2}.txt",
                                                                             server.Path, AppendLogLiteral, Id))));

                if (_counter >= int.MaxValue)
                    _counter = 1;
                DataBackupFilename = string.Format("{0}{1}", DataBackupFilenameLiteral, Id);

                _appendLogger = _updateLogger = null;

                //** add store
                var rkc = new RecordKeyComparer<RecordKey>();
                _addBlocksStore =
                    new Collections.Generic.ConcurrentSortedDictionary<RecordKey, long>(
                        (byte)BTreeAlgorithm.DefaultSlotLength, rkc);

                _recycledBlocksStore =
                    new Collections.Generic.ConcurrentSortedDictionary<RecordKey, long>(
                        (byte)BTreeAlgorithm.DefaultSlotLength, rkc);

                CollectionOnDisk.transaction = this;
                //** file growth store
                var fsc = new FileSegmentComparer<RecordKey>();
                _fileGrowthStore = new Collections.Generic.ConcurrentSortedDictionary<RecordKey, long>(
                        (byte)BTreeAlgorithm.DefaultSlotLength, fsc);
                //** recycled collection store
                _recycledSegmentsStore = new Collections.Generic.ConcurrentSortedDictionary<RecordKey, long>(
                        (byte)BTreeAlgorithm.DefaultSlotLength, fsc);

                //** log collection
                if (LogCollection == null)
                {
                    lock (Locker)
                    {
                        if (LogCollection == null)
                        {
                            var lkc = new BackupDataLogKeyComparer<BackupDataLogKey>();
                            if (_backupCache == null)
                            {
                                var p = new Profile(server.Profile);
                                _backupCache = new Mru.Generic.ConcurrentMruManager<BackupDataLogKey, byte[]>(p.MruMinCapacity * 2,
                                                                                                      p.MruMaxCapacity * 2,
                                                                                                      lkc);
                            }
                            LogCollection = new Collections.Generic.ConcurrentSortedDictionary<BackupDataLogKey, BackupDataLogValue>
                                    (new BackupDataLogKeyComparer<BackupDataLogKey>());
                        }
                    }
                }

                //** make this transaction the trans owner of all collections on this thread
                if (CollectionOnDisk.Session != null)
                    CollectionOnDisk.Session.Transaction = this;
            }
        }


        public ObjectServer Server
        {
            get { return _server; }
            set
            {
                if (_server == null || value == null || value == _server)
                    _server = value;
                else
                    throw new InvalidOperationException(
                        "Can't set Server, this Transaction is already assigned a Server.");
            }
        }
        private ObjectServer _server;

        private static readonly object Locker = new object();

        public override ITransactionLogger Parent
        {
            get { return base.Parent; }
            set
            {
                base.Parent = value;
                if (_addBlocksStore != null && _addBlocksStore is SortedDictionaryOnDisk)
                    ((SortedDictionaryOnDisk)_addBlocksStore).ParentTransactionLogger = value;
                if (_recycledBlocksStore != null && _recycledBlocksStore is SortedDictionaryOnDisk)
                    ((SortedDictionaryOnDisk)_recycledBlocksStore).ParentTransactionLogger = value;
                if (_fileGrowthStore != null && _fileGrowthStore is SortedDictionaryOnDisk)
                    ((SortedDictionaryOnDisk)_fileGrowthStore).ParentTransactionLogger = value;
                if (LogCollection != null && LogCollection is SortedDictionaryOnDisk)
                    ((SortedDictionaryOnDisk)LogCollection).ParentTransactionLogger = value;
            }
        }

        /// <summary>
        /// Begin a Root Transaction on Server &
        /// Begin a nested Transaction for use on Collections.
        /// </summary>
        /// <param name="server"></param>
        /// <returns></returns>
        public static ITransactionLogger BeginWithNewRoot(Sop.ObjectServerWithTransaction server)
        {
            TransactionRoot root = TransactionRoot.BeginRoot(server.Path);
            return Begin(root, server, true);
        }

        public static ITransactionLogger Begin(TransactionRoot rootTrans,
                                               Sop.ObjectServerWithTransaction server)
        {
            return Begin(rootTrans, server, false);
        }

        public static ITransactionLogger Begin(TransactionRoot rootTrans,
                                               Sop.ObjectServerWithTransaction server, bool ownsRoot)
        {
            rootTrans.Server = server;
            server.Transaction = rootTrans;
            var t = (TransactionBase)rootTrans.Begin(ownsRoot);
            return t;
        }

        public bool OwnsRoot = false;

        /// <summary>
        /// Cycle will Commit or Rollback the ongoing Transaction and Begin a new Transaction
        /// upholding Store locks in the process.
        /// </summary>
        /// <param name="commit">true will commit the ongoing transaction, false will rollback. A new Transaction will be started right after.</param>
        /// <returns></returns>
        public override ITransaction Cycle(bool commit = true)
        {
            var synchs = LockStores();
            InCycleTransaction = true;
            bool serverRollback = false;
            try
            {
                var commitOnDispose = Server.CommitOnDispose;
                if (commit)
                {
                    if (Commit())
                    {
                        Server.CommitOnDispose = commitOnDispose;
                        return Sop.Transaction.Transaction.BeginWithNewRoot((ObjectServerWithTransaction)Server);
                    }
                    return null;
                }
                var serverFilename = Server.Filename;
                Rollback();
                // Re-initialize Server if it gets rolled back & its data file deleted...
                if (Server.SystemFile != null)
                    Server.Dispose();
                _server = BeginOpenServer(serverFilename, Server.Profile);
                serverRollback = true;
                Server.CommitOnDispose = commitOnDispose;

                // signal to raise transaction rollback event on other threads requesting a Store Lock.
                foreach (var s in synchs)
                {
                    ((ISynchronizer)s).TransactionRollback = true;
                    ((ISynchronizer)s).CommitLockRequest(false);
                    //((Collections.Synchronizer)s).Unlock();
                }
                return Server.SystemFile.Store.Transaction;
            }
            finally
            {
                InCycleTransaction = serverRollback;
                UnlockStores();
                if (serverRollback)
                    InCycleTransaction = false;
                Dispose();
            }
        }

        private void UpdateStoreTransaction(ITransactionLogger newTrans)
        {
            ModifiedCollections.Locker.Invoke(() =>
                {
                    foreach (CollectionOnDisk collection in ModifiedCollections.Values)
                    {
                        collection.Transaction = newTrans;
                    }
                });
        }

        /// <summary>
        /// Commit changes to Containers/members of this Transaction
        /// </summary>
        /// <returns></returns>
        public override bool Commit()
        {
            LockStores();
            try
            {
                if (Server != null)
                    Server.CommitOnDispose = null;
                bool r = base.Commit();
                if (r && OwnsRoot)
                    return Parent.Commit();
                return r;
            }
            finally
            {
                UnlockStores();
            }
        }

        /// <summary>
        /// Rollback changes to Containers/members of this Transaction
        /// </summary>
        public override void Rollback()
        {
            LockStores();
            try
            {
                if (Server != null)
                    Server.CommitOnDispose = null;
                base.Rollback();
                if (OwnsRoot)
                {
                    Parent.Rollback();
                    Parent = null;
                }
            }
            finally
            {
                UnlockStores();
            }
        }

        private int _inCommit = 0;

        /// <summary>
        /// Roll back other transaction(s) that modified one or more blocks
        /// modified also by this transaction
        /// </summary>
        protected virtual void RollbackConflicts()
        {
            //todo
        }

        /// <summary>
        /// Lock all the transaction modified Stores.
        /// </summary>
        protected override List<ISynchronizer> LockStores()
        {
            if (InCycleTransaction)
                return null;
            return Server.ManageLock();
        }
        /// <summary>
        /// Unlock all the transaction modified Stores.
        /// </summary>
        protected override void UnlockStores()
        {
            if (InCycleTransaction)
                return;
            Log.Logger.Instance.Verbose("UnlockStores: Unlocking the Stores.");
            Server.ManageLock(false);
        }

        /// <summary>
        /// Commit a transaction.
        /// NOTE: this Transaction is not implemented to do two phase commit in the general
        /// sense. Names of variables, enumerations were just done so they can get reused
        /// in future when/if two phase commit is needed in the distributed Server version.
        /// </summary>
        /// <param name="phase">
        /// FirstPhase will make changes permanent but keep transaction log so rollback 
        /// is still possible.
        /// 
        /// SecondPhase will:
        /// 1. call FirstPhase commit if this transaction is in UnCommitted phase
        /// 2. clear the transaction log to complete Commit
        /// NOTE: Rollback is no longer allowed after completion of SecondPhase
        /// </param>
        ///<returns>true if successful otherwise false</returns>
        public override bool InternalCommit(CommitPhase phase)
        {
            Log.Logger.Instance.Verbose("InternalCommit Started.");
            if (CurrentCommitPhase == CommitPhase.Committed)
                throw new InvalidOperationException(string.Format("Transaction '{0}' is already committed.", Id));
            _inCommit++;
            try
            {
                switch (phase)
                {
                    case CommitPhase.FirstPhase:
                        if (CurrentCommitPhase == CommitPhase.UnCommitted)
                        {
                            RollbackConflicts();
                            // save all cached data of each collection
                            List<RecordKey> closeColls = null;
                            Dictionary<CollectionOnDisk, object> parents = null;
                            ModifiedCollections.Locker.Invoke(() =>
                                {
                                    parents = new Dictionary<CollectionOnDisk, object>(ModifiedCollections.Count);
                                    closeColls = new List<RecordKey>();
                                    foreach (KeyValuePair<RecordKey, CollectionOnDisk> kvp in ModifiedCollections)
                                    {
                                        CollectionOnDisk collection = kvp.Value;
                                        CollectionOnDisk ct = collection.GetTopParent();
                                        if (ct.IsOpen)
                                            parents[ct] = null;
                                        else
                                            closeColls.Add(kvp.Key);
                                    }
                                });
                            // flush/commit changes on each Store that are still open within this Transaction...
                            // NOTE: closed/disposed Store changes will be committed as well and their changes at this point are all flushed ready for trans commit.
                            foreach (CollectionOnDisk collection in parents.Keys)
                            {
                                if (!collection.IsOpen) continue;
                                collection.Flush();
                                collection.OnCommit();
                            }
                            foreach (RecordKey k in closeColls)
                                ModifiedCollections.Remove(k);
                            //File.DeletedCollections.Flush();
                            CurrentCommitPhase = CommitPhase.FirstPhase;
                            // don't clear transaction log so rollback is still possible
                            return true;
                        }
                        break;
                    case CommitPhase.SecondPhase:
                        if (CurrentCommitPhase == CommitPhase.UnCommitted)
                        {
                            if (!Commit(CommitPhase.FirstPhase))
                                break;
                        }
                        if (CurrentCommitPhase == CommitPhase.FirstPhase)
                        {
                            // mark second phase completed as when it starts, no turning back...
                            CurrentCommitPhase = CommitPhase.SecondPhase;

                            // preserve the recycled segment so on rollback it can be restored...
                            ModifiedCollections.Locker.Invoke(() =>
                                {
                                    foreach (CollectionOnDisk collection in ModifiedCollections.Values)
                                    {
                                        if (!collection.IsOpen) continue;
                                        collection.HeaderData.RecycledSegmentBeforeTransaction =
                                            collection.HeaderData.RecycledSegment;
                                        if (collection.HeaderData.RecycledSegmentBeforeTransaction != null)
                                            collection.HeaderData.RecycledSegmentBeforeTransaction =
                                                (DeletedBlockInfo)
                                                collection.HeaderData.RecycledSegmentBeforeTransaction.Clone();
                                    }
                                });

                            // delete new (AddStore), updated (LogCollection) and 
                            // file growth segments (FileGrowthStore) "log entries" 
                            ClearStores(true);

                            // todo: Record on Trans Log the FileSet Remove action + info needed for
                            // commit resume "on crash and restart" 11/9/08

                            File.Delete(Server.Path + DataBackupFilename);

                            // todo: remove from trans Log the FileSet Remove action... 11/09/08

                            return true;
                        }
                        break;
                }
                // auto roll back this transaction if commit failed above
                if (CurrentCommitPhase != CommitPhase.Rolledback &&
                    CurrentCommitPhase != CommitPhase.SecondPhase)
                {
                    Rollback();
                }
                return false;
            }
            finally
            {
                _inCommit--;
                if (Parent == null)
                    CollectionOnDisk.transaction = null;
                else
                    Parent.Children.Remove(this);
                Log.Logger.Instance.Verbose("InternalCommit Ended.");
            }
        }

        /// <summary>
        /// Begin transaction and open the ObjectServer in the specified path/filename
        /// </summary>
        /// <param name="serverFilename"></param>
        /// <param name="serverProfile"></param>
        /// <returns></returns>
        public static ObjectServerWithTransaction BeginOpenServer(string serverFilename, Preferences preferences)
        {
            ObjectServerWithTransaction r = RollbackAll(serverFilename, preferences, false);
            if (r != null)
            {
                if (r.Transaction == null)
                {
                    string serverRootPath = Path.GetDirectoryName(serverFilename);
                    if (string.IsNullOrEmpty(serverRootPath))
                        serverRootPath = System.Environment.CurrentDirectory;
                    TransactionRoot root = TransactionRoot.BeginRoot(serverRootPath);
                    root.Server = r;
                    r.Transaction = root;
                    root.Begin(true);
                }
            }
            else
            {
                string serverRootPath = Path.GetDirectoryName(serverFilename);
                if (string.IsNullOrEmpty(serverRootPath))
                    serverRootPath = System.Environment.CurrentDirectory;
                TransactionRoot root = TransactionRoot.BeginRoot(serverRootPath);
                root.IsDisposing = true;

                r = new Sop.ObjectServerWithTransaction(serverFilename, root, preferences);
                ITransactionLogger trans = root.Begin(true);
                trans.IsDisposing = true;
            }
            return r;
        }

        private const string BackupFromToken = "Backup ";
        private const string GrowToken = "Grow ";

        /// <summary>
        /// Rollback uncomitted transactions.
        /// NOTE: this should be invoked upon restart so uncommited transaction(s)
        /// of previous application instance can be rolled back.
        /// </summary>
        /// <param name="serverFilename"> </param>
        /// <param name="serverProfile"> </param>
        /// <param name="createOpenObjectServerIfNoRollbackLog"> </param>
        public static Sop.ObjectServerWithTransaction RollbackAll(string serverFilename, Preferences preferences,
                                                                  bool createOpenObjectServerIfNoRollbackLog = true)
        {
            if (string.IsNullOrEmpty(serverFilename))
                throw new ArgumentNullException("serverFilename");

            if (!Sop.Utility.Utility.HasRequiredDirectoryAccess(serverFilename))
                throw new InvalidOperationException(
                    string.Format("Not enough rights/access on directory containing file '{0}'.",
                                  serverFilename));

            Log.Logger.Instance.Verbose("RollbackAll: rolling back uncommitted changes of previous run.");

            string serverRootPath = System.IO.Path.GetDirectoryName(serverFilename);
            if (string.IsNullOrEmpty(serverRootPath))
                serverRootPath = System.Environment.CurrentDirectory;
            string[] appendLogs = null;

            if (preferences != null && preferences.MemoryExtenderMode)
            {
                if (Sop.Utility.Utility.FileExists(serverFilename))
                {
                    Sop.Utility.Utility.FileDelete(serverFilename);
                    Sop.Utility.Utility.FileDelete(string.Format("{0}.{1}", 
                        serverFilename, ObjectServer.DataInfExtensionLiteral));
                }
            }

            // NOTE: ProcessUpdateLog needs to be done ahead of RollbackAll as the latter 
            // removes backup files which are used by the former

            // rollback all pending transaction updates...
            ProcessUpdateLog(serverRootPath, true);

            // Rollback (delete) root trans created DB objects...
            if (TransactionRoot.RollbackAll(serverRootPath))
            {
                /* AppendLogxx.txt
                    Grow d:\Sopbin\Sop\File.dta 1050624 2096
                 */
                appendLogs = Directory.GetFiles(serverRootPath, string.Format("{0}*.txt", AppendLogLiteral));
            }
            #region Process append logs
            if (appendLogs != null &&
                (createOpenObjectServerIfNoRollbackLog || appendLogs.Length > 0))
            {
                if (Sop.Utility.Utility.FileExists(serverFilename))
                {
                    var r = new ObjectServerWithTransaction(serverFilename, null, preferences);
                    r.Open();
                    foreach (string s in appendLogs)
                    {
                        ITransactionLogger trans = Transaction.BeginWithNewRoot(r);
                        // open the file and do restore for each backed up entry
                        using (var sr = new StreamReader(s))
                        {
                            while (sr.Peek() >= 0)
                            {
                                string l = sr.ReadLine();
                                if (l.StartsWith(GrowToken))
                                {
                                    int i1 = l.LastIndexOf(' ');
                                    int i2 = l.LastIndexOf(' ', i1 - 1);
                                    string s2 = l.Substring(i2, i1 - i2);
                                    long address;
                                    if (long.TryParse(s2, out address))
                                    {
                                        string fName = l.Substring(GrowToken.Length, i2 - GrowToken.Length);
                                        var f = (OnDisk.File.IFile)r.GetFile(fName);
                                        if (f != null)
                                        {
                                            var dbi = new DeletedBlockInfo();
                                            dbi.StartBlockAddress = address;
                                            int segmentSize;
                                            if (int.TryParse(l.Substring(i1), out segmentSize))
                                            {
                                                dbi.EndBlockAddress = dbi.StartBlockAddress + segmentSize;
                                                if (f.DeletedCollections != null)
                                                {
                                                    f.DeletedCollections.Transaction = trans;
                                                    f.DeletedCollections.Add(dbi);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        r.Flush();
                        trans.Commit();
                        // remove the Backup log file, we're done rolling back and it's no longer needed
                        Sop.Utility.Utility.FileDelete(s);
                    }
                    r.Dispose();
                    //return r;
                }
                else
                {
                    foreach (string s in appendLogs)
                        Sop.Utility.Utility.FileDelete(s);
                }
            }
            #endregion

            FileStream.CloseAll();
            return null;
        }

        private static void ProcessUpdateLog(string serverRootPath, bool cleanup)
        {
            /** UpdateLogxx.txt
				Backup d:\Sopbin\Sop\File.dta:62976 to _SystemTransactionDataBackup1:386048 Size=2560
				Backup d:\Sopbin\Sop\File.dta:66048 to _SystemTransactionDataBackup2:388608 Size=2560
			 */
            string[] updateLogs = null;
            try
            {
                updateLogs = Directory.GetFiles(serverRootPath,
                                                string.Format("{0}*.txt", UpdateLogLiteral));
            }
            catch
            {
                return;
            }

            var backupFiles = new Dictionary<string, object>();
            var restoreLookup = new List<CopyParams>();
            foreach (string s in updateLogs)
            {
                restoreLookup.Clear();
                //** open the file and do restore for each backed up entry
                FileStream fs = null;
                try
                {
                    fs = new FileStream(s, FileMode.Open,
                                        FileAccess.Read, FileShare.None);
                }
                catch
                {
                    if (fs != null)
                    {
                        try { fs.Dispose(); }
                        catch { }
                        fs = null;
                    }
                }
                if (fs == null) continue;
                using (fs)
                {
                    using (var sr = new StreamReader(fs.RealStream))
                    {
                        while (sr.Peek() >= 0)
                        {
                            string l = sr.ReadLine();
                            if (l.StartsWith(BackupFromToken))
                            {
                                const string ToToken = " to ";
                                int i2 = l.IndexOf(ToToken + DataBackupFilenameLiteral, BackupFromToken.Length);
                                string from = l.Substring(BackupFromToken.Length, i2 - BackupFromToken.Length);
                                long fromAddress;
                                int d = from.LastIndexOf(':');
                                if (d > 0 &&
                                    long.TryParse(from.Substring(d + 1), out fromAddress))
                                {
                                    from = from.Substring(0, d);
                                    string s2 = l.Substring(i2 + ToToken.Length);
                                    // + DataBackupFilenameLiteral.Length + 1);
                                    if (!string.IsNullOrEmpty(s2))
                                    {
                                        string[] toP = s2.Split(new char[] { ' ' });
                                        if (toP.Length > 1)
                                        {
                                            int indexOfSemi = toP[0].IndexOf(':',
                                                                             DataBackupFilenameLiteral.Length);
                                            string addressText = toP[0].Substring(indexOfSemi + 1);
                                            long toAddress;
                                            if (long.TryParse(addressText, out toAddress))
                                            {
                                                int dataSize;
                                                if (toP[1].Length > 5 &&
                                                    int.TryParse(toP[1].Substring(5), out dataSize) &&
                                                    Sop.Utility.Utility.FileExists(from))
                                                {
                                                    string sourceFilename = toP[0].Substring(0, indexOfSemi);

                                                    if (!backupFiles.ContainsKey(sourceFilename))
                                                        backupFiles.Add(sourceFilename, null);

                                                    var rs = new CopyParams();
                                                    rs.DataSize = dataSize;
                                                    rs.TargetAddress = fromAddress;
                                                    rs.TargetFilename = from;
                                                    rs.SourceAddress = toAddress;
                                                    rs.SourceFilename = sourceFilename;
                                                    restoreLookup.Add(rs);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if (restoreLookup.Count > 0)
                    CopyData(serverRootPath, restoreLookup);
                // delete the Update log file after processing its contents... (we're done with it)
                if (cleanup)
                    File.Delete(s);
            }

            // delete the system transaction backup file
            if (cleanup && backupFiles.Count > 0)
            {
                foreach (string s in backupFiles.Keys)
                    File.Delete(string.Format("{0}{2}{1}", serverRootPath, s, System.IO.Path.DirectorySeparatorChar));
            }
        }

        /// <summary>
        /// Count of ongoing transactions for the entire application
        /// </summary>
        public static int Count = 0;

        private void ClearLogs()
        {
            if (LogCollection == null)
                return;
            LogCollection.Locker.Lock();
            List<BackupDataLogKey> logsForRemoval = null;
            var lc = LogCollection;
            try
            {
                Sop.IFile targetFile = null;
                logsForRemoval = new List<BackupDataLogKey>(lc.Count);
                foreach (var de in lc)
                {
                    var key = (BackupDataLogKey)de.Key;
                    if (targetFile == null || targetFile.Filename != key.SourceFilename)
                        targetFile = Server.GetFile(key.SourceFilename);
                    if (targetFile != null)
                        logsForRemoval.Add(key);
                }
            }
            finally
            {
                LogCollection.Locker.Unlock();
            }
            foreach (BackupDataLogKey k in logsForRemoval)
                lc.Remove(k);
        }

        /// <summary>
        /// Rollback a transaction
        /// </summary>
        public override void InternalRollback(bool isDisposing)
        {
            Log.Logger.Instance.Verbose("InternalRollback Started.");
            if (_addBlocksStore == null && ModifiedCollections == null)
                return;
            if (_recycledBlocksStore == null && ModifiedCollections == null)
                return;

            if (CurrentCommitPhase == CommitPhase.Committed)
                throw new InvalidOperationException(
                    string.Format("Transaction '{0}' is already committed, can't rollback.", Id));
            if (CurrentCommitPhase == CommitPhase.Rolledback)
                throw new InvalidOperationException(
                    string.Format("Transaction '{0}' was rolled back, can't roll it back again.", Id));

            _inCommit++;
            try
            {
                // Step 1. truncate all newly added blocks beyond eof before transaction began
                // Step 2. copy all preserved blocks in transaction log onto respective
                // collection on disk to revert changes. Ensure to mark each reverted block so during
                // crash while rollback, we can resume where we left off.
                // Step 3. Clear memory of this transaction objects

                // Revert from backup
                RestoreData();

                Dictionary<CollectionOnDisk, object> parents = null;
                // Clear memory of this transaction's objects
                ModifiedCollections.Locker.Invoke(() =>
                    {
                        parents = new Dictionary<CollectionOnDisk, object>(ModifiedCollections.Count);
                        foreach (KeyValuePair<RecordKey, CollectionOnDisk> de in ModifiedCollections)
                        {
                            de.Value.HeaderData.IsModifiedInTransaction = false;
                            // clear memory of objects belonging to the transaction...
                            if (de.Key.Filename != DataBackupFilename)
                            {
                                parents[de.Value.GetTopParent()] = null;
                                de.Value.HeaderData.RecycledSegment = de.Value.HeaderData.RecycledSegmentBeforeTransaction;
                                if (de.Value.HeaderData.RecycledSegment != null)
                                    de.Value.HeaderData.RecycledSegment =
                                        (DeletedBlockInfo)de.Value.HeaderData.RecycledSegment.Clone();
                                RemoveInMemory(de.Value, de.Key.Address);
                            }
                        }
                    });
                if (!(OwnsRoot && isDisposing))
                {
                    foreach (CollectionOnDisk cod in parents.Keys)
                    {
                        cod.OnRollback();
                        if (cod is BTreeAlgorithm)
                        {
                            var sdod = ((BTreeAlgorithm)cod).Container;
                            if (sdod == null) continue;
                            sdod.IsUnloading = true;
                            sdod.Reload();
                        }
                        else
                            cod.Load();
                    }
                }
                // Truncate all newly added blocks beyond eof
                if (_fileGrowthStore != null)
                {
                    _fileGrowthStore.Locker.Lock();
                    if (((OnDisk.File.FileSet)Server.FileSet).Btree != null)
                    {
                        var dbis = new List<KeyValuePair<DeletedBlockInfo, OnDisk.File.IFile>>(_fileGrowthStore.Count);
                        foreach (var de in _fileGrowthStore)
                        {
                            var key = de.Key;
                            var f = (OnDisk.File.IFile)Server.FileSet[key.Filename];
                            if (f == null) continue;
                            // add to deleted blocks the newly extended blocks!
                            var dbi = new DeletedBlockInfo { StartBlockAddress = de.Key.Address };
                            dbi.EndBlockAddress = dbi.StartBlockAddress + de.Value;
                            dbis.Add(new KeyValuePair<DeletedBlockInfo, OnDisk.File.IFile>(dbi, f));
                        }
                        _addBlocksStore.Clear();
                        _recycledBlocksStore.Clear();
                        _fileGrowthStore.Clear();
                        _recycledSegmentsStore.Clear();
                        int oldCommit = _inCommit;
                        _inCommit = 0;
                        if (Server.HasTrashBin)
                        {
                            foreach (KeyValuePair<DeletedBlockInfo, OnDisk.File.IFile> itm in dbis)
                            {
                                // add to deleted blocks the newly extended blocks!
                                itm.Value.DeletedCollections.Add(itm.Key);
                            }
                            foreach (KeyValuePair<DeletedBlockInfo, OnDisk.File.IFile> itm in dbis)
                            {
                                itm.Value.DeletedCollections.Flush();
                                itm.Value.DeletedCollections.OnCommit();
                            }
                        }
                        _inCommit = oldCommit;
                    }
                    _fileGrowthStore.Locker.Unlock();
                }
                if (OwnsRoot && isDisposing)
                {
                    foreach (CollectionOnDisk cod in parents.Keys)
                        cod.CloseStream();
                }

                ClearStores(true);

                // if no more ongoing transaction, we can safely delete the transaction backup data file
                File.Delete(Server.Path + DataBackupFilename);
            }
            finally
            {
                _inCommit--;
                if (Parent == null)
                    CollectionOnDisk.transaction = null;
                else if (Parent.Children != null)
                    Parent.Children.Remove(this);
                Log.Logger.Instance.Verbose("InternalRollback Ended.");
            }
            CurrentCommitPhase = CommitPhase.Rolledback;
        }

        private void RemoveInMemory(CollectionOnDisk coll, long address)
        {
            bool reload = false;
            CollectionOnDisk p = coll;
            while (p != null)
            {
                if (p.RemoveInMemory(address, this) && !reload)
                    reload = true;
                if (p.Parent is CollectionOnDisk)
                    p = (CollectionOnDisk)p.Parent;
                else
                    return;
            }
        }
        #region Copy Data
        private class CopySegmentParams : IDisposable
        {
            public CopySegmentParams(CopyParams source)
            {
                HeadItem = source;
                Items.Add(source);
            }
            public void Dispose()
            {
                if (Completed != null)
                {
                    Completed.WaitOne();
                    Completed.Dispose();
                    Completed = null;
                    SourceReadAheadBuffer = null;
                    Items = null;
                }
            }
            public ManualResetEvent Completed = new ManualResetEvent(false);
            public CopyParams HeadItem;
            public Sop.OnDisk.DataBlock.DataBlockReadBufferLogic SourceReadAheadBuffer;
            public List<CopyParams> Items = new List<CopyParams>();

            public override string ToString()
            {
                var sb = new StringBuilder();

                foreach(var item in Items)
                {
                    sb.AppendLine(item.ToString());
                }

                return sb.ToString();
            }
        }
        private struct CopyParams
        {
            public string SourceFilename;
            public long SourceAddress;
            public string TargetFilename;
            public long TargetAddress;
            public int DataSize;
            public override string ToString()
            {
                return string.Format("{0}, {1}, {2}", SourceFilename, TargetAddress, DataSize);
            }
        }
        class CompareCopyParams : System.Collections.Generic.IComparer<CopyParams>
        {
            public int Compare(CopyParams x, CopyParams y)
            {
                var r = string.Compare(x.SourceFilename, y.SourceFilename);
                if (r == 0)
                    r = x.SourceAddress < y.SourceAddress ? -1 :
                        x.SourceAddress == y.SourceAddress ? 0 : 1;
                return r;
            }
        }

        private static List<CopySegmentParams> DetectAndMerge(List<CopyParams> copyBlocks)
        {
            var sortedByFilename = new Collections.Generic.SortedDictionary<CopyParams, CopyParams>
                                        (new CompareCopyParams());
            foreach (var itm in copyBlocks)
            {
                sortedByFilename.Add(itm, itm);
            }
            var dataSegments = new Sop.Collections.Generic.SortedDictionary<long, long>();
            KeyValuePair<CopyParams, CopyParams> previousItem = new KeyValuePair<CopyParams, CopyParams>();
            var r = new List<CopySegmentParams>();
            CopySegmentParams currSegment = null;
            foreach (var kvp in sortedByFilename)
            {
                if (r.Count == 0)
                {
                    currSegment = new CopySegmentParams(kvp.Value);
                    r.Add(currSegment);
                    previousItem = kvp;
                    dataSegments.Add(kvp.Key.SourceAddress, kvp.Key.DataSize);
                    continue;
                }
                if (previousItem.Key.SourceFilename == kvp.Key.SourceFilename &&
                    OnDisk.Algorithm.BTree.IndexedBlockRecycler.DetectAndMerge(dataSegments,
                        kvp.Value.SourceAddress, (long)kvp.Value.DataSize))
                {
                    currSegment.Items.Add(kvp.Value);
                    continue;
                }
                if (dataSegments.Count == 1)
                {
                    dataSegments.MoveFirst();
                    currSegment.HeadItem.DataSize = (int)dataSegments.CurrentValue;
                    currSegment = new CopySegmentParams(kvp.Value);
                    r.Add(currSegment);
                    dataSegments.Clear();
                }
                else
                    throw new SopException("There is a bug in DetectAndMerge.");
                previousItem = kvp;
                dataSegments.Add(kvp.Key.SourceAddress, kvp.Key.DataSize);
            }
            if (dataSegments.Count == 1)
            {
                dataSegments.MoveFirst();
                currSegment.HeadItem.DataSize = (int)dataSegments.CurrentValue;
            }
            else
                throw new SopException("There is a bug in DetectAndMerge.");
            return r;
        }
        private static void CopyData(string serverRootPath, List<CopyParams> copyBlocks)
        {
            if (copyBlocks == null)
                throw new ArgumentNullException("copyBlocks");
            Transaction.LogTracer.Verbose("CopyData: Start server root path={0}.", serverRootPath);

            var copySegments = DetectAndMerge(copyBlocks);
            using (var readPool = new ConcurrentIOPoolManager())
            {
                long readerFileSize = 0;
                const int ReadAheadAsyncBatch = 20;
                try
                {
                    for (int i = 0; i < copySegments.Count; i++)
                    {
                        if (readPool.AsyncThreadException != null)
                            throw readPool.AsyncThreadException;

                        // prevent overallocating Async threads for Copying large amounts of data blocks...
                        if (i > ReadAheadAsyncBatch)
                        {
                            copySegments[i - ReadAheadAsyncBatch].Completed.WaitOne();
                            copySegments[i - ReadAheadAsyncBatch - 1].Dispose();
                        }
                        var rb = copySegments[i].HeadItem;
                        int size = rb.DataSize;
                        string systemBackupFilename = string.Format("{0}{1}{2}", serverRootPath, System.IO.Path.DirectorySeparatorChar, rb.SourceFilename);
                        // copy backed up data onto the DB
                        var reader = readPool.GetInstance(systemBackupFilename, null, size);
                        if (readerFileSize == 0)
                            readerFileSize = reader.FileStream.Length;
                        if (rb.SourceAddress < readerFileSize)
                        {
                            Transaction.LogTracer.Verbose("CopyData: inside the loop, i={0}, Address={1}, Size={2}.", i, rb.SourceAddress, size);

                            if (rb.SourceAddress + rb.DataSize > readerFileSize)
                                rb.DataSize = size = (int)(readerFileSize - rb.SourceAddress);
                            reader.FileStream.Seek(rb.SourceAddress, SeekOrigin.Begin, true);
                            reader.FileStream.BeginRead(reader.Buffer, 0, size,
                            #region BeginRead callback
                             (result) =>
                             {
                                 if (!result.IsCompleted)
                                     result.AsyncWaitHandle.WaitOne();
                                 var copySegment = (CopySegmentParams)((object[])result.AsyncState)[0];
                                 var reader2 = (ConcurrentIOData)((object[])result.AsyncState)[1];

                                 try
                                 {
                                     reader2.FileStream.EndRead(result);
                                 }
                                 catch(Exception exc)
                                 {
                                     readPool.AddException(exc);
                                     Transaction.LogTracer.Fatal(exc);
                                     reader2.Event.Set();
                                     throw;
                                 }
                                 try
                                 {
                                     using (var writePool = new ConcurrentIOPoolManager())
                                     {
                                         Transaction.LogTracer.Verbose("CopyData: inside the reader callback, Src Address={0}, Size={1}.",
                                             copySegment.HeadItem.SourceAddress, copySegment.HeadItem.DataSize);

                                         copySegment.SourceReadAheadBuffer = new DataBlockReadBufferLogic(reader2.Buffer,
                                             copySegment.HeadItem.SourceAddress, copySegment.HeadItem.DataSize);

                                         // write to targets...
                                         foreach (var cp in copySegment.Items)
                                         {
                                             // short circuit if IO exception is encountered.
                                             if (writePool.AsyncThreadException != null)
                                                 throw writePool.AsyncThreadException;

                                             var buffer = copySegment.SourceReadAheadBuffer.Get(cp.SourceAddress, cp.DataSize, true);
                                             var targetFile = cp.TargetFilename;
                                             var writer = writePool.GetInstance(targetFile, null);

                                             // delay up to hear reader2 mgmt so as not to inadvertently allow WaitAll on 
                                             // reader pool to return when writers are about to start but not allocated yet.
                                             if (reader2 != null)
                                             {
                                                 reader2.Buffer = null;
                                                 reader2.Event.Set();
                                                 reader2 = null;
                                             }

                                             writer.FileStream.Seek(cp.TargetAddress, SeekOrigin.Begin, true);
                                             writer.FileStream.BeginWrite(buffer, 0, buffer.Length,
                                             #region BeginWrite callback
                                                 (writerResult) =>
                                                 {
                                                     if (!writerResult.IsCompleted)
                                                         writerResult.AsyncWaitHandle.WaitOne();
                                                     var writer2 = (ConcurrentIOData)writerResult.AsyncState;
                                                     try
                                                     {
                                                         writer2.FileStream.EndWrite(writerResult, true);
                                                         Transaction.LogTracer.Verbose("CopyData: inside the writer callback, FileStream Position={0}.",
                                                             writer2.FileStream.Position);
                                                         writer2.FileStream.Flush();
                                                         writer2.Event.Set();
                                                     }
                                                     catch (Exception exc)
                                                     {
                                                         writer2.PoolManager.AddException(exc);
                                                         Transaction.LogTracer.Fatal(exc);
                                                         writer2.Event.Set();
                                                         throw;
                                                     }
                                                 }
                                             #endregion
                                                 , writer);
                                         }
                                     }   // this (dispose) will wait until all writer Async I/O threads are completed...
                                     // free up read ahead buffer to conserve memory...
                                 }
                                 finally
                                 {
                                     Transaction.LogTracer.Verbose("CopyData: reader callback end, setting copySegment.SourceReadAheadBuffer to null.");
                                     copySegment.SourceReadAheadBuffer = null;
                                     copySegment.Completed.Set();
                                 }
                             }
                            #endregion
                            , new object[] { copySegments[i], reader });
                        }
                        else
                        {
                            copySegments[i].Completed.Set();
                            // just signal completion as Segments are beyond file length, 'can only mean they are "file growth" segments
                            // which are handled separately on another method.
                            reader.Event.Set();
                        }
                        // end
                    }
                }
                finally
                {
                    // Dispose: wait until the last copy thread group is completed & free up resources...
                    foreach (var cs in copySegments)
                        cs.Dispose();
                }
            }
            Transaction.LogTracer.Verbose("CopyData: End server root path={0}.", serverRootPath);
        }
        #endregion

        private void RestoreData()
        {
            if (LogCollection == null)
                return;
            LogCollection.Locker.Lock();
            try
            {
                var lc = LogCollection;
                using (var writePool = new ConcurrentIOPoolManager())
                {
                    using (var readPool = new ConcurrentIOPoolManager())
                    {
                        OnDisk.File.IFile targetFile = null;
                        foreach (var de in lc)
                        {
                            // short circuit if IO exception was detected.
                            if (readPool.AsyncThreadException != null)
                                throw readPool.AsyncThreadException;
                            if (writePool.AsyncThreadException != null)
                                throw writePool.AsyncThreadException;

                            var key = de.Key;
                            var value = de.Value;
                            string systemBackupFilename = Server.Path + GetLogBackupFilename(value.BackupFileHandle);

                            int size = value.DataSize;
                            if (targetFile == null || targetFile.Filename != key.SourceFilename)
                                targetFile = (OnDisk.File.IFile)Server.GetFile(key.SourceFilename);

                            if (targetFile == null)
                                continue;

                            ConcurrentIOData reader = readPool.GetInstance(systemBackupFilename, (TransactionRoot)Root, size);
                            ConcurrentIOData writer = writePool.GetInstance(targetFile);
                            reader.FileStream.Seek(value.BackupDataAddress, SeekOrigin.Begin, true);
                            writer.FileStream.Seek(key.SourceDataAddress, SeekOrigin.Begin, true);
                            reader.FileStream.BeginRead(reader.Buffer, 0, size, ReadCallback,
                                                        new object[] { new[] { reader, writer }, false });
                        }
                    }
                }
            }
            finally
            {
                LogCollection.Locker.Unlock();
            }
            ClearLogs();
        }

        protected virtual void ClearStores(bool isRecycleStores)
        {
            CollectionOnDisk.transaction = null;
            if (CollectionOnDisk.Session != null)
                CollectionOnDisk.Session.Transaction = null;
            ModifiedCollections.Clear();
            _addBlocksStore = null;
            _recycledBlocksStore = null;
            _fileGrowthStore = null;
            _recycledSegmentsStore = null;

            RemoveFromLogBackupLookup(DataBackupFilename);
            if (Count > 0 && Interlocked.Decrement(ref Count) == 0)
            {
                if (LogCollection != null)
                {
                    lock (Locker)
                    {
                        if (LogCollection != null)
                        {
                            LogBackupFileHandleLookup.Clear();
                            LogBackupFilenameLookup.Clear();
                            Interlocked.Exchange(ref _logBackupFilenameLookupCounter, 0);
                            ClearBackupStreams();
                            LogCollection = null;
                        }
                    }
                }
            }
            if (_appendLogger != null)
            {
                _appendLogger.Dispose();
                File.Delete(_appendLogger.LogFilename);
                _appendLogger = null;
            }
            if (_updateLogger == null)
                return;
            _updateLogger.Dispose();
            File.Delete(_updateLogger.LogFilename);
            _updateLogger = null;
        }

        protected Collections.Generic.ISortedDictionary<RecordKey, CollectionOnDisk> ModifiedCollections =
            new Collections.Generic.ConcurrentSortedDictionary<RecordKey, CollectionOnDisk>(new RecordKeyComparer2<RecordKey>());

        // Recycled Blocks Store
        private Collections.Generic.ISortedDictionary<RecordKey, long> _recycledBlocksStore;
        private Collections.Generic.ISortedDictionary<RecordKey, long> _addBlocksStore;
        // Recycled Collections Store
        private Collections.Generic.ISortedDictionary<RecordKey, long> _recycledSegmentsStore;
        /// <summary>
        /// Transaction File Growth Store keeps information about Segments
        /// that were created within the transaction. The information is used
        /// during rollback to recycle those segments.
        /// </summary>
        private Collections.Generic.ISortedDictionary<RecordKey, long> _fileGrowthStore;
        private static int _counter = 1;

        internal static Log.Logger LogTracer = Log.Logger.Instance;
        //new Log.Logger(@"C:\MyProjects\CSharp\SopVersion4.7\Package\Samples\bin\App_Data\TransactionLog.txt")
        //{
        //    LogLevel = Log.LogLevels.Verbose
        //};
    }
}
