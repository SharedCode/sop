using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using Sop.Transaction;

namespace Sop
{
    /// <summary>
    /// Object Server wrapper class.
    /// All methods delegate to the real Object Server received as a parameter in one of the ctors.
    /// </summary>
    public partial class ObjectServer : IObjectServer
    {
        /// <summary>
        /// Defalt Server Data Filename, no extension.
        /// </summary>
        public const string DefaultServerDataFilename = "SopServer.dta";
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="serverFilename"></param>
        /// <param name="commitOnDispose">If there is a pending transaction, true will commit this Transaction when this object gets out of scope, false (default) will rollback.</param>
        /// <param name="preferences"></param>
        /// <param name="readOnly"></param>
        public ObjectServer(string serverFilename = null, bool commitOnDispose = false, Preferences preferences = null, bool readOnly = false)
        {
            Initialize(serverFilename, commitOnDispose, preferences, readOnly);
        }

        private void Initialize(string serverFilename, bool commitOnDispose = false, Preferences preferences = null, bool readOnly = false)
        {
            // handle default paths...
            if (string.IsNullOrWhiteSpace(serverFilename))
            {
                var s = new Uri(System.IO.Path.GetDirectoryName(Assembly.GetAssembly(typeof(ObjectServer)).CodeBase)).LocalPath;
                var dataFolder = string.Format("{0}{1}..{1}App_Data{1}", s, System.IO.Path.DirectorySeparatorChar);
                if (!System.IO.Directory.Exists(dataFolder))
                {
                    dataFolder = string.Format("{0}{1}..{1}SopData{1}", s, System.IO.Path.DirectorySeparatorChar);
                    if (!System.IO.Directory.Exists(dataFolder))
                        System.IO.Directory.CreateDirectory(dataFolder);

                    if (System.IO.Directory.Exists(dataFolder))
                        serverFilename = dataFolder + DefaultServerDataFilename;
                    else
                        throw new SopException("No default SOP data folder can be created/used.");
                }
                else
                    serverFilename = dataFolder + DefaultServerDataFilename;
            }

            // Server ctor...
            if (preferences == null)
                preferences = new Profile();
            if (readOnly)
                RealObjectServer = new OnDisk.ObjectServer(serverFilename, null, preferences, true);
            else
                RealObjectServer =
                    Sop.Transaction.Transaction.BeginOpenServer(serverFilename, preferences);
            CommitOnDispose = commitOnDispose;
        }


        /// <summary>
        /// Begin a new Transaction.
        /// </summary>
        /// <returns></returns>
        public virtual Sop.ITransaction BeginTransaction()
        {
            if (RealObjectServer == null)
                throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
            return RealObjectServer.BeginTransaction();
        }

        public virtual ITransaction CycleTransaction(bool commit = true)
        {
            if (RealObjectServer == null)
                throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
            if (_inCycleTransaction)
                throw new SopException("Sop.ObjectServer CycleTransaction is already ongoing, 'making another call fails.");
            lock (this)
            {
                if (_inCycleTransaction)
                    throw new SopException("Sop.ObjectServer CycleTransaction is already ongoing, 'making another call fails.");
                _inCycleTransaction = true;
                var t = RealObjectServer.CycleTransaction(commit);
                if (t != null && ((Sop.Transaction.Transaction)t).Server != RealObjectServer)
                    RealObjectServer = ((Sop.Transaction.Transaction)t).Server;
                _inCycleTransaction = false;
                return t;
            }
        }
        private volatile bool _inCycleTransaction;

        /// <summary>
        /// Commit Transaction
        /// </summary>
        public virtual void Commit()
        {
            if (RealObjectServer == null)
                throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
            lock (this)
            {
                RealObjectServer.Commit();
            }
        }

        /// <summary>
        /// Rollback the Transaction
        /// </summary>
        public virtual void Rollback()
        {
            if (RealObjectServer == null)
                throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
            lock (this)
            {
                RealObjectServer.Rollback();
            }
        }

        /// <summary>
        /// Returns the default file extension of the Object Server
        /// </summary>
        public const string DefaultFileExtension = "dta";

        public virtual void Close()
        {
            if (RealObjectServer != null)
                RealObjectServer.Close();
        }

        /// <summary>
        /// If there is a pending transaction,
        /// true will commit this transaction on dispose, otherwise will rollback.
        /// </summary>
        public bool? CommitOnDispose
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.CommitOnDispose;
            }
            set
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                RealObjectServer.CommitOnDispose = value;
            }
        }

        public virtual Encoding Encoding
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.Encoding;
            }
        }

        public virtual string Filename
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.Filename;
            }
        }

        public virtual IFileSet FileSet
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.FileSet;
            }
        }
        Sop.Client.IFileSet Sop.Client.IObjectServer.FileSet
        {
            get { return FileSet; }
        }

        public virtual IFile GetFile(string name)
        {
            if (RealObjectServer == null)
                throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
            return RealObjectServer.GetFile(name);
        }
        Sop.Client.IFile Sop.Client.IObjectServer.GetFile(string name)
        {
            return GetFile(name);
        }

        public virtual bool IsDirty
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.IsDirty;
            }
        }

        public virtual bool IsNew
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.IsNew;
            }
        }

        public virtual bool IsOpen
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.IsOpen;
            }
        }

        public virtual bool ReadOnly
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.ReadOnly;
            }
        }

        public virtual string Name
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.Name;
            }
        }

        public virtual void Open()
        {
            if (RealObjectServer == null)
                throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
            RealObjectServer.Open();
        }

        public Profile Profile
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.Profile;
            }
        }

        public Sop.ISortedDictionary<string, string> StoreTypes
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.StoreTypes;
            }
        }

        public virtual string Path
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.Path;
            }
        }

        public virtual StoreNavigator StoreNavigator
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.StoreNavigator;
            }
        }

        public virtual void Flush()
        {
            if (RealObjectServer == null)
                throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
            RealObjectServer.Flush();
        }

        public virtual IFile SystemFile
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.SystemFile;
            }
        }
        Sop.Client.IFile Sop.Client.IObjectServer.SystemFile
        {
            get
            {
                return SystemFile;
            }
        }

        public virtual ITransaction Transaction
        {
            get
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                return RealObjectServer.Transaction;
            }
            set
            {
                if (RealObjectServer == null)
                    throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
                RealObjectServer.Transaction = value;
            }
        }

        public virtual void Dispose()
        {
            if (RealObjectServer != null)
            {
                RealObjectServer.Dispose();
                StoreFactory.RemoveServerStoresInMru(RealObjectServer);
                RealObjectServer = null;
            }
        }

        public override string ToString()
        {
            return string.Format("Server {0}, FileSystem {1}",
                this.Name, this.SystemFile.ToString());
        }

        internal volatile IObjectServer RealObjectServer;
    }
}
