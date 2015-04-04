using System;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// Object Server with transaction.
    /// </summary>
    internal class ObjectServerWithTransaction : OnDisk.ObjectServer
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public ObjectServerWithTransaction()
        {
        }

        /// <summary>
        /// Constructor expecting Filename and Transaction Logger
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="transLogger"></param>
        /// <param name="profileScheme"> </param>
        /// <param name="readOnly"> </param>
        public ObjectServerWithTransaction(string filename, ITransaction transLogger = null,
            Preferences preferences = null, bool readOnly = false) :
            base(
                                               filename, (Transaction.TransactionRoot)transLogger, preferences,
                                               readOnly)
        {
        }

        /// <summary>
        /// Begin Transaction
        /// </summary>
        public bool BeginTransaction()
        {
            if (Transaction == null ||
                (int)((Sop.Transaction.ITransactionLogger)Transaction).CurrentCommitPhase >=
                (int)Sop.Transaction.CommitPhase.SecondPhase)
            {
                Transaction = Sop.Transaction.TransactionRoot.BeginRoot(this.Path);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Commit Transaction
        /// </summary>
        public void Commit()
        {
            if (Transaction != null)
                Transaction.Commit();
        }

        /// <summary>
        /// Rollback the Transaction
        /// </summary>
        public void Rollback()
        {
            if (Transaction != null)
                Transaction.Rollback();
        }

        /// <summary>
        /// Rollback all pending transactions left open by previous Application run.
        /// </summary>
        /// <param name="serverRootPath"> </param>
        public static void RollbackAll(string serverRootPath)
        {
            Sop.Transaction.TransactionRoot.RollbackAll(serverRootPath);
        }
    }
}