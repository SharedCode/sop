using System;
using System.Collections.Generic;
using System.Text;
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
        /// default ctor.
        /// </summary>
        protected ObjectServer()
        {
        }
        /// <summary>
        /// ctor receiving the real object server as parameter.
        /// </summary>
        /// <param name="server"></param>
        public ObjectServer(IObjectServer server)
        {
            RealObjectServer = server;
        }

        /// <summary>
        /// Begin a new Transaction.
        /// </summary>
        /// <returns></returns>
        public virtual Sop.ITransaction BeginTransaction()
        {
            if (RealObjectServer == null)
                throw new InvalidOperationException("ObjectServer not initialized. RealObjectServer is null.");
            //return RealObjectServer.BeginTransaction();
            return Sop.Transaction.Transaction.BeginWithNewRoot((ObjectServerWithTransaction) RealObjectServer);
        }

        /// <summary>
        /// Commit Transaction
        /// </summary>
        public virtual void Commit()
        {
            if (Transaction != null)
                Transaction.Commit();
        }

        /// <summary>
        /// Rollback the Transaction
        /// </summary>
        public virtual void Rollback()
        {
            if (Transaction != null)
                Transaction.Rollback();
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

        internal IObjectServer RealObjectServer;
    }
}
