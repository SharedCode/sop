using System;

namespace Sop
{
    /// <summary>
    /// Object Server interface.
    /// </summary>
    public interface IObjectServer : Client.IObjectServer
    {
        /// <summary>
        /// Close the entire Server including each File(s) and each File's Object Store.
        /// </summary>
        void Close();

        /// <summary>
        /// Returns the Server profile.
        /// </summary>
        Profile Profile { get; }

        /// <summary>
        /// Returns the Create Store Log table.
        /// </summary>
        Sop.ISortedDictionary<string, string> StoreTypes { get; }

        /// <summary>
        /// Returns the Server's File Set.
        /// </summary>
        new IFileSet FileSet { get; }
        /// <summary>
        /// Return the File with a given name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        new IFile GetFile(string name);
        /// <summary>
        /// true if Server was modified.
        /// </summary>
        bool IsDirty { get; }
        /// <summary>
        /// true if Server was just created.
        /// </summary>
        bool IsNew { get; }
        /// <summary>
        /// true if Server is Open.
        /// </summary>
        bool IsOpen { get; }
        /// <summary>
        /// Open the Server.
        /// </summary>
        void Open();
        /// <summary>
        /// Save changes to the Server.
        /// </summary>
        void Flush();
        /// <summary>
        /// Returns the Server's System File.
        /// </summary>
        new IFile SystemFile { get; }
        /// <summary>
        /// Transaction object.
        /// </summary>
        ITransaction Transaction { get; set; }
        /// <summary>
        /// Begin a Transaction.
        /// </summary>
        /// <returns></returns>
        ITransaction BeginTransaction();
        /// <summary>
        /// Commint all changes to the current transaction.
        /// </summary>
        void Commit();
        /// <summary>
        /// Rollback all changes of the current transaction.
        /// </summary>
        void Rollback();
    }
}