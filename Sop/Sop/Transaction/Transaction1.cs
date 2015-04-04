// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
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
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.OnDisk.DataBlock;
using Sop.OnDisk.IO;
using FileStream = Sop.OnDisk.File.FileStream;

namespace Sop.Transaction
{
    using OnDisk;

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
        /// SOP merges contiguous blocks as possible to minimize entries.
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
                _addStore =
                    new Collections.Generic.ConcurrentSortedDictionary<RecordKey, long>(
                        (byte)BTreeAlgorithm.DefaultSlotLength, rkc);
                CollectionOnDisk.transaction = this;
                //** file growth store
                var fsc = new FileSegmentComparer<RecordKey>();
                _fileGrowthStore =
                    new Collections.Generic.ConcurrentSortedDictionary<RecordKey, long>(
                        (byte)BTreeAlgorithm.DefaultSlotLength, fsc);
                //** recycled collection store
                _recycledCollectionStore =
                    new Collections.Generic.ConcurrentSortedDictionary<RecordKey, long>(
                        (byte)BTreeAlgorithm.DefaultSlotLength, fsc);

                //** log collection
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
                        LogCollection =
                            new Collections.Generic.ConcurrentSortedDictionary<BackupDataLogKey, BackupDataLogValue>
                                (new BackupDataLogKeyComparer<BackupDataLogKey>());
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
                if (_addStore != null && _addStore is SortedDictionaryOnDisk)
                    ((SortedDictionaryOnDisk)_addStore).ParentTransactionLogger = value;
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
        /// Commit changes to Containers/members of this Transaction
        /// </summary>
        /// <returns></returns>
        public override bool Commit()
        {
            bool r = base.Commit();
            if (r && OwnsRoot)
                return Parent.Commit();
            return r;
        }

        /// <summary>
        /// Rollback changes to Containers/members of this Transaction
        /// </summary>
        public override void Rollback()
        {
            base.Rollback();
            if (OwnsRoot)
            {
                Parent.Rollback();
                Parent = null;
            }
        }

        private int _inCommit = 0;

        /// <summary>
        /// Roll back other transaction(s) that modified one or more blocks
        /// modified also by this transaction
        /// </summary>
        protected virtual void RollbackConflicts()
        {
        }

        /// <summary>
        /// Commit a transaction
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
                            //** save all cached data of each collection
                            var parents =
                                new Dictionary<CollectionOnDisk, object>(ModifiedCollections.Count);
                            var closeColls = new List<RecordKey>();
                            foreach (KeyValuePair<RecordKey, CollectionOnDisk> kvp in ModifiedCollections)
                            {
                                CollectionOnDisk collection = kvp.Value;
                                CollectionOnDisk ct = collection.GetTopParent();
                                if (ct.IsOpen)
                                    parents[ct] = null;
                                else
                                    closeColls.Add(kvp.Key);
                            }
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
                            //** don't clear transaction log so rollback is still possible
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
                            //** mark second phase completed as when it starts, no turning back...
                            CurrentCommitPhase = CommitPhase.SecondPhase;

                            //** preserve the recycled segment so on rollback it can be restored...
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

                            //** delete new (AddStore), updated (LogCollection) and 
                            //** file growth segments (FileGrowthStore) "log entries" 
                            ClearStores(true);

                            //** todo: Record on Trans Log the FileSet Remove action + info needed for
                            //** commit resume "on crash and restart" 11/9/08

                            File.Delete(Server.Path + DataBackupFilename);

                            //** todo: remove from trans Log the FileSet Remove action... 11/09/08

                            return true;
                        }
                        break;
                }
                //** auto roll back this transaction if commit failed above
                if (CurrentCommitPhase != CommitPhase.Rolledback &&
                    CurrentCommitPhase != CommitPhase.SecondPhase)
                    Rollback();
                return false;
            }
            finally
            {
                _inCommit--;
                if (Parent == null)
                    CollectionOnDisk.transaction = null;
                else
                    Parent.Children.Remove(this);
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

        public static Sop.ObjectServerWithTransaction RollbackAll(string serverFilename, Preferences preferences)
        {
            return RollbackAll(serverFilename, preferences, true);
        }

        /// <summary>
        /// Rollback uncomitted transactions.
        /// NOTE: this should be invoked upon restart so uncommited transaction(s)
        /// when program quits in previous run can be rolled back.
        /// </summary>
        /// <param name="serverFilename"> </param>
        /// <param name="serverProfile"> </param>
        /// <param name="createOpenObjectServerIfNoRollbackLog"> </param>
        public static Sop.ObjectServerWithTransaction RollbackAll(string serverFilename, Preferences preferences,
                                                                  bool createOpenObjectServerIfNoRollbackLog)
        {
            if (string.IsNullOrEmpty(serverFilename))
                throw new ArgumentNullException("serverFilename");

            if (!Sop.Utility.Utility.HasRequiredDirectoryAccess(serverFilename))
                throw new InvalidOperationException(
                    string.Format("Not enough rights/access on directory containing file '{0}'.",
                                  serverFilename));

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


            //** NOTE: ProcessUpdateLog needs to be done ahead of RollbackAll as the latter 
            //** removes backup files which are used by the former

            //** rollback all pending transaction updates...
            ProcessUpdateLog(serverRootPath, true);

            //** Rollback (delete) root trans created DB objects...
            if (TransactionRoot.RollbackAll(serverRootPath))
            {
                /** AppendLogxx.txt
                    Grow d:\Sopbin\Sop\File.dta 1050624 2096
                 */
                appendLogs = Directory.GetFiles(serverRootPath,
                                                string.Format("{0}*.txt", AppendLogLiteral));
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
                        //** open the file and do restore for each backed up entry
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
                        //** remove the Backup log file, we're done rolling back and it's no longer needed
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
                        try
                        {
                            fs.Dispose();
                        }
                        catch
                        {
                        }
                    }
                    fs = null;
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
                //** delete the Update log file after processing its contents... (we're done with it)
                if (cleanup)
                    File.Delete(s);
            }

            //** delete the system transaction backup file
            if (cleanup && backupFiles.Count > 0)
            {
                foreach (string s in backupFiles.Keys)
                    File.Delete(string.Format("{0}\\{1}", serverRootPath, s));
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
            var lc = LogCollection;  //).GetLogSequence();
            Sop.IFile targetFile = null;
            var logsForRemoval = new List<BackupDataLogKey>(lc.Count);
            foreach (var de in lc)
            {
                var key = (BackupDataLogKey)de.Key;
                if (targetFile == null || targetFile.Filename != key.SourceFilename)
                    targetFile = Server.GetFile(key.SourceFilename);
                if (targetFile != null)
                    logsForRemoval.Add(key);
            }
            foreach (BackupDataLogKey k in logsForRemoval)
                lc.Remove(k);
        }

        /// <summary>
        /// Rollback a transaction
        /// </summary>
        public override void InternalRollback(bool isDisposing)
        {
            if (_addStore == null && ModifiedCollections == null)
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
                //** Step 1. truncate all newly added blocks beyond eof before transaction began
                //** Step 2. copy all preserved blocks in transaction log onto respective
                //** collection on disk to revert changes. Ensure to mark each reverted block so during
                //** crash while rollback, we can resume where we left off.
                //** Step 3. Clear memory of this transaction objects

                //** Revert from backup
                RestoreData();

                //** Clear memory of this transaction's objects
                var parents = new Dictionary<CollectionOnDisk, object>(ModifiedCollections.Count);
                foreach (KeyValuePair<RecordKey, CollectionOnDisk> de in ModifiedCollections)
                {
                    de.Value.HeaderData.IsModifiedInTransaction = false;
                    //** clear memory of objects belonging to the transaction...
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
                if (!(OwnsRoot && isDisposing))
                {
                    foreach (CollectionOnDisk cod in parents.Keys)
                    {
                        cod.OnRollback();
                        if (cod is BTreeAlgorithm)
                        {
                            var sdod = ((BTreeAlgorithm)cod).Container;
                            if (sdod == null) continue;
                            sdod.Reload();
                        }
                        else
                            cod.Load();
                    }
                }
                //** Truncate all newly added blocks beyond eof
                if (_fileGrowthStore != null)
                {
                    var dbis = new List<KeyValuePair<DeletedBlockInfo, OnDisk.File.IFile>>(_fileGrowthStore.Count);
                    foreach (var de in _fileGrowthStore)
                    {
                        var key = de.Key;
                        var f = (OnDisk.File.IFile)Server.FileSet[key.Filename];
                        if (f == null) continue;
                        //** add to deleted blocks the newly extended blocks!
                        var dbi = new DeletedBlockInfo { StartBlockAddress = de.Key.Address };
                        dbi.EndBlockAddress = dbi.StartBlockAddress + de.Value;
                        dbis.Add(new KeyValuePair<DeletedBlockInfo, OnDisk.File.IFile>(dbi, f));
                    }
                    _addStore.Clear();
                    _fileGrowthStore.Clear();
                    _recycledCollectionStore.Clear();
                    int oldCommit = _inCommit;
                    _inCommit = 0;
                    if (Server.HasTrashBin)
                    {
                        foreach (KeyValuePair<DeletedBlockInfo, OnDisk.File.IFile> itm in dbis)
                        {
                            //** add to deleted blocks the newly extended blocks!
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
                if (OwnsRoot && isDisposing)
                {
                    foreach (CollectionOnDisk cod in parents.Keys)
                        cod.CloseStream();
                }

                ClearStores(true);

                //** if no more ongoing transaction, we can safely delete the transaction backup data file
                File.Delete(Server.Path + DataBackupFilename);
            }
            finally
            {
                _inCommit--;
                if (Parent == null)
                    CollectionOnDisk.transaction = null;
                else if (Parent.Children != null)
                    Parent.Children.Remove(this);
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

        private struct CopyParams
        {
            public string SourceFilename;
            public long SourceAddress;
            public string TargetFilename;
            public long TargetAddress;
            public int DataSize;
        }
        private static void CopyData(string serverRootPath, List<CopyParams> copyBlocks)
        {
            if (copyBlocks == null)
                throw new ArgumentNullException("copyBlocks");
            using (var writePool = new ConcurrentIOPoolManager())
            {
                using (var readPool = new ConcurrentIOPoolManager())
                {
                    string targetFile = null;
                    long readerFileSize = 0;
                    for (int i = 0; i < copyBlocks.Count; i++)
                    {
                        var rb = copyBlocks[i];
                        int size = rb.DataSize;
                        if (targetFile == null || targetFile != rb.TargetFilename)
                            targetFile = rb.TargetFilename;

                        string systemBackupFilename = string.Format("{0}\\{1}", serverRootPath, rb.SourceFilename);

                        // copy backed up data onto the DB
                        var reader = readPool.GetInstance(systemBackupFilename, null, size);
                        var writer = writePool.GetInstance(targetFile, null, size);
                        if (reader == null || writer == null)
                            continue;

                        if (readerFileSize == 0)
                            readerFileSize = reader.FileStream.Length;
                        if (rb.SourceAddress + rb.DataSize <= readerFileSize)
                        {
                            reader.FileStream.Seek(rb.SourceAddress, SeekOrigin.Begin);
                            writer.FileStream.Seek(rb.TargetAddress, SeekOrigin.Begin);
                            reader.FileStream.BeginRead(reader.Buffer, 0, size,
                                                        ReadCallback,
                                                        new object[] { new[] { reader, writer }, false }
                                );
                        }
                        else
                        {
                            reader.Event.Set();
                            writer.Event.Set();
                        }
                        // end
                    }
                }
            }
        }

        private void RestoreData()
        {
            if (LogCollection == null)
                return;
            var lc = LogCollection;
            using (var writePool = new ConcurrentIOPoolManager())
            {
                using (var readPool = new ConcurrentIOPoolManager())
                {
                    OnDisk.File.IFile targetFile = null;
                    foreach (var de in lc)
                    {
                        var key = de.Key;
                        var value = de.Value;
                        string systemBackupFilename = Server.Path + GetLogBackupFilename(value.BackupFileHandle);

                        int size = value.DataSize;
                        if (targetFile == null || targetFile.Filename != key.SourceFilename)
                            targetFile = (OnDisk.File.IFile)Server.GetFile(key.SourceFilename);

                        if (targetFile == null)
                            continue;

                        ConcurrentIOData reader = readPool.GetInstance(systemBackupFilename, (TransactionRoot)Root,
                                                                       size);
                        ConcurrentIOData writer = writePool.GetInstance(targetFile, size);
                        reader.FileStream.Seek(value.BackupDataAddress, SeekOrigin.Begin);
                        writer.FileStream.Seek(key.SourceDataAddress, SeekOrigin.Begin);
                        reader.FileStream.BeginRead(reader.Buffer, 0, size, ReadCallback,
                                                    new object[] { new[] { reader, writer }, false });
                    }
                }
            }
            ClearLogs();
        }

        protected virtual void ClearStores(bool isRecycleStores)
        {
            CollectionOnDisk.transaction = null;
            if (CollectionOnDisk.Session != null)
                CollectionOnDisk.Session.Transaction = null;

            ModifiedCollections.Clear();

            _addStore = null;
            _fileGrowthStore = null;
            _recycledCollectionStore = null;

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
            if (_updateLogger == null) return;
            _updateLogger.Dispose();
            File.Delete(_updateLogger.LogFilename);
            _updateLogger = null;
        }

        protected Collections.Generic.ISortedDictionary<RecordKey, CollectionOnDisk> ModifiedCollections =
            new Collections.Generic.ConcurrentSortedDictionary<RecordKey, CollectionOnDisk>(new RecordKeyComparer2<RecordKey>());

        private Collections.Generic.ISortedDictionary<RecordKey, long> _addStore;
        private Collections.Generic.ISortedDictionary<RecordKey, long> _recycledCollectionStore;

        /// <summary>
        /// Transaction File Growth Store keeps information about Segments
        /// that were created within the transaction. The information is used
        /// during rollback to recycle those segments.
        /// </summary>
        private Collections.Generic.ISortedDictionary<RecordKey, long> _fileGrowthStore;

        private static int _counter = 1;
    }
}