// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.File;
using Sop.OnDisk.Geometry;
using Sop.OnDisk.IO;
using Sop.Utility;

namespace Sop.Transaction
{
    using OnDisk;


    /// <summary>
    /// Create 1st level Transaction using TransactionRoot.
    /// SOP uses TransactionRoot for keeping track of lowest
    /// level transaction activities such as creation of the
    /// main ObjectServer's File object.
    /// It is also used to provide 2nd level transaction
    /// protection such as before writing to the Indexed transaction
    /// Files, it records the activity detail such as File start Offset
    /// and count of bytes to be modified so the activity can be rolled
    /// back if needed.
    /// </summary>
    internal class TransactionRoot : TransactionBase, Sop.Transaction.ITransactionRoot
    {
        private class SingleLineWriter : IDisposable
        {
            public SingleLineWriter(string filename)
            {
                _fs = new System.IO.FileStream(filename, System.IO.FileMode.Create);
                this.Filename = filename;
                _sw = new System.IO.StreamWriter(_fs);
                _sw.AutoFlush = true;
            }

            internal readonly string Filename;

            public void Dispose()
            {
                if (_fs != null)
                {
                    try
                    {
                        _sw.Dispose();
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                    _sw = null;
                    try
                    {
                        _fs.Dispose();
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                    _fs = null;
                }
            }

            public void Log(string text)
            {
                _sw.BaseStream.Seek(0, System.IO.SeekOrigin.Begin);
                _sw.WriteLine(text);
                _sw.BaseStream.SetLength(text.Length + 2);
            }

            private System.IO.FileStream _fs;
            private System.IO.StreamWriter _sw;
        }

        public override bool Commit()
        {
            // Comming the Collection Transaction level
            if (Server != null &&
                Server.SystemFile != null &&
                Server.SystemFile.Store != null &&
                Server.SystemFile.Store.Transaction != null &&
                ((TransactionBase) Server.SystemFile.Store.Transaction).CurrentCommitPhase ==
                CommitPhase.UnCommitted)
            {
                return Server.SystemFile.Store.Transaction.Commit();
            }
            // Commit the Server Root level
            return base.Commit();
        }

        public override void Rollback()
        {
            // Rollback the Collection Transaction level
            if (Server != null &&
                Server.SystemFile != null &&
                Server.SystemFile.Store != null &&
                Server.SystemFile.Store.Transaction != null &&
                ((TransactionBase) Server.SystemFile.Store.Transaction).CurrentCommitPhase ==
                CommitPhase.UnCommitted)
            {
                Server.SystemFile.Store.Transaction.Rollback();
                return;
            }
            // Rollback the Server Root level
            base.Rollback();
        }

        public override void Dispose()
        {
            this._addStore = null;
            this._fileGrowthStore = null;
            _recycledCollectionStore = null;
            this._logger = null;
            this._loggerTransDetails = null;
            this._loggerTransDetailsBuffer = null;
            this._loggerTransDetailsFileStream = null;
            this.Server = null;
            base.Dispose();
        }

        protected TransactionRoot()
        {
        }

        protected TransactionRoot(string logFolder, string filename)
        {
            this.LogFolder = logFolder;
            SetupFilenames(filename);
        }

        private void SetupFilenames(string filename)
        {
            if (!System.IO.Directory.Exists(LogFolder))
                System.IO.Directory.CreateDirectory(LogFolder);

            if (!Sop.Utility.Utility.HasRequiredDirectoryAccess(filename))
                throw new InvalidOperationException(
                    string.Format("Not enough rights/access on directory containing file '{0}'.",
                                  filename));

            _loggerFilename = string.Format("{0}\\{1}.txt", LogFolder, filename);
            _loggerTransDetailsFilename = string.Format("{0}\\{1}{2}.txt", LogFolder, filename, DetailsLiteral);
            _loggerTransDetailsLogFilename = string.Format("{0}\\{1}{2}.log", LogFolder, filename, DetailsLiteral);
            LogRootFilename = filename;
        }

        private string _loggerFilename;

        private string LoggerFilename
        {
            get
            {
                if (_logger != null)
                    return _logger.LogFilename;
                return _loggerFilename;
            }
        }

        private string _loggerTransDetailsFilename;

        private string LoggerTransDetailsFilename
        {
            get
            {
                if (_loggerTransDetails != null)
                    return _loggerTransDetails.Filename;
                return _loggerTransDetailsFilename;
            }
        }

        /// <summary>
        /// Contains the Log root filename.
        /// </summary>
        public string LogRootFilename { get; private set; }

        /// <summary>
        /// Transaction Log folder
        /// </summary>
        public string LogFolder; // = Sop.ObjectServerWithTransaction.RootFolderPath + "\\TransactionLogs";

        private GenericLogger _logger;

        private GenericLogger Logger
        {
            get { return _logger ?? (_logger = new GenericLogger(_loggerFilename)); }
        }

        private SingleLineWriter _loggerTransDetails;

        private SingleLineWriter LoggerTransDetails
        {
            get { return _loggerTransDetails ?? (_loggerTransDetails = new SingleLineWriter(_loggerTransDetailsFilename)); }
        }

        private static int Counter = 0;

        /// <summary>
        /// Begin a root transaction
        /// </summary>
        public static TransactionRoot BeginRoot(string serverRootPath)
        {
            //** clear out the colleciton transaction thread static when a new Root Trans is being created
            CollectionOnDisk.transaction = null;
            if (!serverRootPath.EndsWith("\\"))
                serverRootPath += "\\";
            return new TransactionRoot(serverRootPath + "TransactionLogs",
                                       string.Format("{0}{1}", TransLiteral,
                                                     System.Threading.Interlocked.Increment(ref Counter)));
        }

        private const string TransLiteral = "Trans";

        /// <summary>
        /// Commit this transaction
        /// </summary>
        /// <param name="phase"></param>
        /// <returns></returns>
        public override bool InternalCommit(CommitPhase phase)
        {
            //** don't do anything as if Commit reached this point, it should just be treated as succeeded.
            //** Remember, this is the Root transaction which Commit gets called only when inner trans succeeded.
            if (phase == CommitPhase.SecondPhase)
            {
                //** record completion of this transaction in log file
                CleanupLogger();
                //GC.SuppressFinalize(this);
            }
            else if (CurrentCommitPhase == CommitPhase.UnCommitted)
            {
                if (Server != null)
                {
                    //Server.Flush();
                    CurrentCommitPhase = CommitPhase.SecondPhase;
                    Server.Transaction = null;
                }
                else
                    CurrentCommitPhase = CommitPhase.SecondPhase;
            }
            return true;
        }

        /// <summary>
        /// Rollback this transaction
        /// </summary>
        public override void InternalRollback(bool isDisposing)
        {
            rollback();
            //GC.SuppressFinalize(this);
        }

        //** rollback the action
        private static bool ProcessLines(string loggerFilename,
                                         GenericLogger logger, ObjectServer server)
        {
            //** go to 1st line in transaction log text file
            string processedLinesFilename = string.Format("{0}Processed.dat", loggerFilename);
            var plh = new ProcessedLinesHandler(processedLinesFilename);
            /*
                Create Test.dta
                Create File.dta
                Create Test.dta.log
                Create _SystemTransactionDataBackup
                Remove _SystemTransactionDataBackup
                Create _SystemTransactionDataBackup
                Remove _SystemTransactionDataBackup
             */
            using (plh)
            {
                if (logger != null)
                    logger.Dispose();
                if (Sop.Utility.Utility.FileExists(loggerFilename))
                {
                    System.IO.StreamReader reader = new System.IO.StreamReader(loggerFilename);
                    using (reader)
                    {
                        bool previousIsCreateToken = false;
                        string previousTokenValue = string.Empty;
                        int lineCtr = 0;
                        while (!reader.EndOfStream)
                        {
                            string line = reader.ReadLine();
                            //** process line...
                            if (line.StartsWith("Create "))
                            {
                                if (previousIsCreateToken)
                                {
                                    plh.ProcessedLines = lineCtr;
                                    //DisposeTransRelatedFiles(PreviousTokenValue);
                                    if (server != null &&
                                        server.Filename == previousTokenValue)
                                    {
                                        server.Transaction = null;
                                        server.dispose(true);
                                    }
                                    try
                                    {
                                        System.IO.File.Delete(previousTokenValue);
                                    }
                                    catch
                                    {
                                        return false;
                                    }
                                    plh.MarkLineSuccess();

                                    /* * marking the file entry with "Success" token:
                                        Create Test.dta
                                        Success
                                        Create File.dta
                                        Success
                                        Create Test.dta.log
                                        Success
                                        Create _SystemTransactionDataBackup
                                        Remove _SystemTransactionDataBackup
                                        Create _SystemTransactionDataBackup
                                        Remove _SystemTransactionDataBackup
                                     */
                                }
                                previousIsCreateToken = true;
                            }
                            else if (line.StartsWith("Remove "))
                            {
                                if (previousIsCreateToken)
                                {
                                    previousIsCreateToken = false;
                                }
                            }
                            lineCtr++;
                            previousTokenValue = line.Substring(7);
                        }
                        if (previousIsCreateToken)
                        {
                            plh.ProcessedLines = lineCtr;
                            System.IO.File.Delete(previousTokenValue);
                            plh.MarkLineSuccess();
                        }
                    }
                }
            }
            return true;
        }

        private const string DetailsLiteral = "Details";

        /// <summary>
        /// Rollback all transaction root logs.
        /// NOTE: invoke this upon app restart
        /// </summary>
        public static bool RollbackAll(string serverPath)
        {
            string logFolder = string.Format("{0}\\TransactionLogs", serverPath);
            if (System.IO.Directory.Exists(logFolder))
            {
                //** 1.) open each transaction log file in TransactionLog folder and restore the last line activity...
                string[] logFiles = System.IO.Directory.GetFiles(logFolder, string.Format("{0}*.txt", TransLiteral));
                foreach (string logFile in logFiles)
                {
                    //** open the transaction log file and process contents
                    System.IO.StreamReader reader;
                    try
                    {
                        reader = new System.IO.StreamReader(logFile);
                    }
                    catch
                    {
                        return false;
                    }
                    using (reader)
                    {
                        reader.BaseStream.Seek(0, System.IO.SeekOrigin.End);
                        if (reader.BaseStream.Position == 0)
                        {
                            reader.Close();
                            continue;
                        }
                        long seekCount = -256;
                        if (reader.BaseStream.Position < 256)
                            seekCount = reader.BaseStream.Position*-1;
                        reader.BaseStream.Seek(seekCount, System.IO.SeekOrigin.End);
                        string s = reader.ReadToEnd();
                        s = s.TrimEnd();
                        const string registerSaveLiteral = "RegisterSave ";
                        if (string.IsNullOrEmpty(s) || !s.StartsWith(registerSaveLiteral))
                        {
                            // do nothing...
                        }
                        else
                        {
                            //** roll back...
                            string values = s.Substring(registerSaveLiteral.Length - 1);
                            string[] parts = values.Split(new string[] {", "}, StringSplitOptions.None);
                            if (parts != null && parts.Length == 3)
                            {
                                string targetFilename = parts[0].Trim();
                                int targetBlockAddress;

                                //** TODO: create a restart log file where these exceptions will be logged

                                if (!int.TryParse(parts[1], out targetBlockAddress))
                                    throw new ApplicationException(
                                        string.Format("Invalid Block Address{0} found in log file {1}", parts[1],
                                                      targetFilename));
                                int targetSegmentSize;
                                if (!int.TryParse(parts[2], out targetSegmentSize))
                                    throw new ApplicationException(
                                        string.Format("Invalid _region Size{0} found in log file {1}", parts[2],
                                                      targetFilename));

                                string sourceFilename = string.Format("{0}.log",
                                                                      logFile.Substring(0, logFile.Length - 4));
                                if (!System.IO.File.Exists(sourceFilename))
                                    throw new ApplicationException(
                                        string.Format("Backup filename{0} not found.", sourceFilename));

                                int targetBufferSize;
                                FileStream targetFileStream =
                                    File.UnbufferedOpen(ObjectServer.NormalizePath(serverPath, targetFilename),
                                                        System.IO.FileAccess.ReadWrite,
                                                        targetSegmentSize, out targetBufferSize);

                                if (targetFileStream == null)
                                    throw new ApplicationException(string.Format("Can't open Target File {0}.",
                                                                                 targetFilename));

                                int sourceBufferSize;
                                FileStream sourceFileStream =
                                    File.UnbufferedOpen(
                                        ObjectServer.NormalizePath(serverPath, sourceFilename),
                                        System.IO.FileAccess.ReadWrite,
                                        targetSegmentSize, out sourceBufferSize);
                                var sourceBuffer = new byte[sourceBufferSize];

                                targetFileStream.Seek(targetBlockAddress, System.IO.SeekOrigin.Begin);

                                //** copy asynchronously from source to target log file
                                IAsyncResult iar = sourceFileStream.BeginRead(sourceBuffer, 0,
                                                                              targetSegmentSize, null, null);
                                if (!iar.IsCompleted)
                                    iar.AsyncWaitHandle.WaitOne();
                                sourceFileStream.EndRead(iar);
                                targetFileStream.Seek(0, System.IO.SeekOrigin.Begin);
                                iar = targetFileStream.BeginWrite(sourceBuffer, 0,
                                                                  targetSegmentSize, null, null);
                                if (!iar.IsCompleted)
                                    iar.AsyncWaitHandle.WaitOne();
                                targetFileStream.EndWrite(iar);
                                targetFileStream.Flush();

                                targetFileStream.Dispose();
                                sourceFileStream.Dispose();
                            }
                        }
                    }
                }
                foreach (string logFile in logFiles)
                {
                    ProcessLines(logFile, null, null);
                    Sop.Utility.Utility.FileDelete(logFile);
                }

                //** remove TransactionLog folder
                //System.IO.Directory.Delete(LogFolder, true);
                return true;
            }
            return false;
        }

        private void rollback()
        {
            if (CurrentCommitPhase == CommitPhase.Committed)
                throw new InvalidOperationException(
                    string.Format("Transaction '{0}' is already committed, can't rollback.", Id));
            if (CurrentCommitPhase == CommitPhase.Rolledback)
                throw new InvalidOperationException(
                    string.Format("Transaction '{0}' was rolled back, can't roll it back again.", Id));

            if (Server != null)
                Server.Transaction = null;

            //** do actual roll back...
            /* - open the file if close
             * - process each line that hasn't been marked "committed"
             *      - if created a file, delete it
             *      - if updated a file, copy the 
             */
            if (Server != null && IsDisposing)
            {
                Server.dispose(IsDisposing);
                //Server = null;
            }
            if (ProcessLines(LoggerFilename, _logger, Server))
                CleanupLogger();
            _addStore = null;
            _fileGrowthStore = null;
            _recycledCollectionStore = null;
            CurrentCommitPhase = CommitPhase.Rolledback;
        }

        private ObjectServer _server;

        public ObjectServer Server
        {
            get { return _server; }
            set
            {
                if (value != null && _server != null)
                    throw new InvalidOperationException("This Transaction is already assigned to a Server.");
                _server = value;
            }
        }

        private void CleanupLogger()
        {
            if (_logger != null)
            {
                _logger.Dispose();
                _logger = null;
            }
            if (!string.IsNullOrEmpty(_loggerFilename))
            {
                //** remove the log file
                System.IO.File.Delete(_loggerFilename);
                _loggerFilename = null;
            }
            if (_loggerTransDetails != null)
            {
                _loggerTransDetails.Dispose();
                _loggerTransDetails = null;
            }
            if (!string.IsNullOrEmpty(_loggerTransDetailsFilename))
            {
                System.IO.File.Delete(_loggerTransDetailsFilename);
                _loggerTransDetailsFilename = null;
            }
            //** dispose the text log file
            if (_loggerTransDetailsFileStream != null)
            {
                _loggerTransDetailsFileStream.Dispose();
                _loggerTransDetailsFileStream = null;
                _loggerTransDetailsBuffer = null;
                System.IO.File.Delete(_loggerTransDetailsLogFilename);
            }
            //if (System.IO.Directory.Exists(LogFolder))
            //    System.IO.Directory.Delete(LogFolder, true);
        }

        /// <summary>
        /// If File doesn't exist, records its none existence so on Rollback, it can be deleted.
        /// 'doesn't do anything if file exists.
        /// </summary>
        /// <param name="filename"></param>
        public void RegisterOpenFile(string filename)
        {
            if (!System.IO.File.Exists(filename))
                //** log sample:
                //** Create System.dta
                Logger.LogLine("Create {0}", filename);
        }

        public void RegisterRemoveFile(string filename)
        {
            if (System.IO.File.Exists(filename))
                Logger.LogLine("Remove {0}", filename);
        }

        public void RegisterFailure(TransactionRootFailTypes failType,
                                    params object[] parameters)
        {
            switch (failType)
            {
                case TransactionRootFailTypes.RemoveFileFailure:
                    if (parameters == null || parameters.Length != 1 ||
                        !(parameters[0] is string))
                        throw new ArgumentOutOfRangeException("RemoveFileFailure expects only 1 string parameter.");
                    Logger.LogLine("RemoveFileFailure {0}", parameters[0]);
                    break;
            }
        }

        //** todo: 1/7/09: Copy On Write(COW) each Register block to a backup file that
        //** gets overwritten over and over as only last activity detail data is logged/rolled back
        //** NOTE: older activities are deemed successful if there is a newer activity(ie - the last activity)

        //** idea: 
        // 1. track new Add/Growth segments, don't track Save on said segments as,
        //** they can be stashed away without harm as only added on transaction that will be rolled 
        //** back if failed/program crash
        // 2. track updated segments not in Add/Growth to allow rollback of said segments.
        // 3. last action entry will be rolled back on restart...

        protected internal override void RegisterAdd(CollectionOnDisk collection, long blockAddress, int blockSize)
        {
            //** all transaction collections reside in TransactionFile which will be rolled back and
            //** truncated during restart after program crash...
            Transaction.RegisterAdd(_addStore, _fileGrowthStore, _recycledCollectionStore, collection, blockAddress,
                                    blockSize, true);
        }

        protected internal override void RegisterRemove(CollectionOnDisk collection)
        {
        }

        protected internal override void RegisterFileGrowth(CollectionOnDisk collection, long segmentAddress,
                                                            long segmentSize)
        {
            //** all transaction collections reside in TransactionFile which will be rolled back and
            //** truncated during restart after program crash...
            Transaction.RegisterFileGrowth(_fileGrowthStore, collection, segmentAddress, segmentSize, false);
        }

        private string _lastRegisterSaveFilename;
        private long _lastRegisterSaveBlockAddress;
        private int _lastRegisterSaveSegmentSize;

        private Collections.Generic.ISortedDictionary<Transaction.RecordKey, long> _addStore =
            new Collections.Generic.ConcurrentSortedDictionary<Transaction.RecordKey, long>(new Transaction.RecordKeyComparer<Transaction.RecordKey>());

        private Collections.Generic.ISortedDictionary<Transaction.RecordKey, long> _recycledCollectionStore =
            new Collections.Generic.ConcurrentSortedDictionary<Transaction.RecordKey, long>(new Transaction.FileSegmentComparer<Transaction.RecordKey>());

        private Collections.Generic.ISortedDictionary<Transaction.RecordKey, long> _fileGrowthStore =
            new Collections.Generic.ConcurrentSortedDictionary<Transaction.RecordKey, long>(new Transaction.FileSegmentComparer<Transaction.RecordKey>());

        /// <summary>
        /// RegisterSave is the only one we do COW to be able to roll back from text log file
        /// the last transaction action done.
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="blockAddress"></param>
        /// <param name="segmentSize"></param>
        /// <param name="readPool"> </param>
        /// <param name="writePool"> </param>
        protected internal override bool RegisterSave(CollectionOnDisk collection, long blockAddress, int segmentSize,
            ConcurrentIOPoolManager readPool, ConcurrentIOPoolManager writePool)
        {
            if (!string.IsNullOrEmpty(_lastRegisterSaveFilename))
            {
                LoggerTransDetails.Log(string.Format("Successful RegisterSave {0}, {1}, {2}",
                                                     _lastRegisterSaveFilename, _lastRegisterSaveBlockAddress,
                                                     _lastRegisterSaveSegmentSize));
                _lastRegisterSaveFilename = null;
                _lastRegisterSaveBlockAddress = 0;
                _lastRegisterSaveSegmentSize = 0;
            }

            /* Step 1. Remove Intersections with added/recycled Blocks from region as no need to backup 
                     new Blocks
               Step 2. Copy or backup (any) remaining blocks (the Updated blocks) 
                     onto the Transaction Log file for restore on Rollback
             */
            Transaction.RecordKey key = Transaction.CreateKey(collection, blockAddress);

            Region region = RegionLogic.RemoveIntersections(_fileGrowthStore, key,
                                                                blockAddress, segmentSize);

            if (region == null || region.Count == 0)
                return false;

            bool logOnce = false;
            foreach (KeyValuePair<long, int> area2 in region)
            {
                key.Address = area2.Key;

                Region region2 = RegionLogic.RemoveIntersections(_recycledCollectionStore, key,
                                                                     area2.Key, area2.Value);

                if (region2 != null && region.Count > 0)
                {
                    foreach (KeyValuePair<long, int> area3 in region2)
                    {
                        key.Address = area3.Key;

                        Region region3 = RegionLogic.RemoveIntersections(_addStore, key,
                                                                             area3.Key, area3.Value);

                        //** Step 2: Backup the "modified" portion(s) of data
                        if (region3 != null && region3.Count > 0)
                        {
                            //** foreach disk area in region, copy it to transaction log file
                            foreach (KeyValuePair<long, int> area4 in region3)
                                BackupData(collection, area4.Key, area4.Value);
                            if (!logOnce)
                            {
                                logOnce = true;
                                _lastRegisterSaveFilename = collection.File.Filename;
                                _lastRegisterSaveBlockAddress = blockAddress;
                                _lastRegisterSaveSegmentSize = segmentSize;
                                LoggerTransDetails.Log(
                                    string.Format("RegisterSave {0}, {1}, {2}", collection.File.Filename, blockAddress,
                                                  segmentSize));
                            }
                        }
                    }
                }
            }
            return logOnce;
        }

        protected internal override void RegisterRecycle(CollectionOnDisk collection, long blockAddress,
                                                         int blockSize)
        {
            Transaction.RegisterRecycle(_addStore, collection, blockAddress, blockSize);
        }

        protected internal override void RegisterRecycleCollection(CollectionOnDisk collection, long blockAddress,
                                                                   int blockSize)
        {
            Transaction.RegisterFileGrowth(_recycledCollectionStore, collection, blockAddress, blockSize, true);
        }

        private void BackupData(CollectionOnDisk collection, long blockAddress, int segmentSize)
        {
            if (_loggerTransDetailsFileStream == null)
            {
                int bufferSize;
                //** open with 1 MB custom buffering
                string s = _loggerTransDetailsLogFilename;
                if (Server != null)
                    s = Server.NormalizePath(s);
                _loggerTransDetailsFileStream = File.UnbufferedOpen(s, out bufferSize).RealStream;
                _loggerTransDetailsBuffer = new byte[bufferSize];
            }
            long sourceOffset = collection.FileStream.Position;
            collection.FileStream.Seek(blockAddress, System.IO.SeekOrigin.Begin);
            //** copy asynchronously from source to target log file
            IAsyncResult iar = collection.FileStream.BeginRead(_loggerTransDetailsBuffer, 0, segmentSize, null, null);
            if (!iar.IsCompleted)
                iar.AsyncWaitHandle.WaitOne();
            collection.FileStream.EndRead(iar);
            _loggerTransDetailsFileStream.Seek(0, System.IO.SeekOrigin.Begin);
            iar = _loggerTransDetailsFileStream.BeginWrite(_loggerTransDetailsBuffer, 0, segmentSize, null, null);
            if (!iar.IsCompleted)
                iar.AsyncWaitHandle.WaitOne();
            _loggerTransDetailsFileStream.EndWrite(iar);
            _loggerTransDetailsFileStream.Flush();
            if (_loggerTransDetailsFileStream.Length > segmentSize)
                _loggerTransDetailsFileStream.SetLength(segmentSize);
            collection.FileStream.Seek(sourceOffset, System.IO.SeekOrigin.Begin);
        }

        private string _loggerTransDetailsLogFilename;
        private byte[] _loggerTransDetailsBuffer;
        private System.IO.FileStream _loggerTransDetailsFileStream;
        private RegionLogic RegionLogic = new RegionLogic();
    }
}