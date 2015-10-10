using Sop.OnDisk.File;

namespace Sop
{
    public partial class ObjectServer
    {
        /// <summary>
        /// License Key.
        /// </summary>
        public static string LicenseKey
        {
            get { return OnDisk.ObjectServer.LicenseKey; }
            set { OnDisk.ObjectServer.LicenseKey = value; }
        }

        /// <summary>
        /// Open Object Server in read only mode.
        /// </summary>
        /// <param name="serverFilename"></param>
        /// <param name="serverProfile"></param>
        /// <returns></returns>
        public static Sop.IObjectServer OpenReadOnly(string serverFilename, Preferences preferences = null)
        {
            return new OnDisk.ObjectServer(serverFilename, null, preferences, true);
        }

        /// <summary>
        /// Open Object Server and begin a low-level, high speed transaction (no ACID properties on this level).
        /// </summary>
        /// <param name="serverFilename"></param>
        /// <param name="serverProfile"></param>
        /// <returns></returns>
        public static Sop.ObjectServer OpenWithTransaction(string serverFilename, Preferences preferences = null)
        {
            return new ObjectServer(serverFilename, true, preferences, false);
        }

        /// <summary>
        /// Maximum number of File Stream Instance count.
        /// 
        /// NOTE: SOP manages File Stream instances so when total number of
        /// Opened File Streams reaches maximum amount, SOP will close down
        /// least recently used File Stream. This management is important for 
        /// scalability as File Streams are expensive resources, OS imposes
        /// maximum limit and .
        /// </summary>
        public static int MaxFileStreamInstanceCount
        {
            get { return FileStream.MaxInstanceCount; }
            set { FileStream.MaxInstanceCount = value; }
        }

        public static string GetFullFolderPath(string serverSystemFilename)
        {
            return System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(serverSystemFilename));
        }

        /// <summary>
        /// Rollback all pending transactions left open by previous Application run.
        /// </summary>
        public static void RollbackAll(string serverRootPath)
        {
            Sop.Transaction.TransactionRoot.RollbackAll(serverRootPath);
        }
    }
}