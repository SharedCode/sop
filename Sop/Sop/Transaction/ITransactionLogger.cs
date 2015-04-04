// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;

namespace Sop.Transaction
{
    /// <summary>
    /// Transaction Logger interface
    /// </summary>
    public interface ITransactionLogger : ITransaction
    {
        /// <summary>
        /// true means transaction is in process of being disposed, false otherwise.
        /// </summary>
        bool IsDisposing { get; set; }
        /// <summary>
        /// Returns the parent Transaction object
        /// </summary>
        ITransactionLogger Parent { get; set; }

        /// <summary>
        /// Nested or Children Transactions 
        /// </summary>
        List<ITransactionLogger> Children { get; set; }

        /// <summary>
        /// Returns the Root transaction
        /// </summary>
        ITransactionLogger Root { get; }

        /// <summary>
        /// Create Collection helper method.
        /// </summary>
        /// <param name="file"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"></param>
        /// <returns></returns>
        Sop.ISortedDictionaryOnDisk CreateCollection(IFile file, IComparer comparer, string name,
                                                     bool isDataInKeySegment);

        /// <summary>
        /// Commit this Transaction's Children's Transaction(s) then Commit this Transaction.
        /// </summary>
        /// <param name="phase"></param>
        /// <returns></returns>
        bool Commit(CommitPhase phase);

        /// <summary>
        /// Implement to Commit this transaction object only.
        /// NOTE: don't commit children, they are managed by "Commit"
        /// function, commit just this object.
        /// </summary>
        /// <param name="phase"></param>
        /// <returns></returns>
        bool InternalCommit(CommitPhase phase);

        /// <summary>
        /// Returns the non-root, transaction logger (TransactionBase class derived).
        /// </summary>
        /// <returns></returns>
        ITransactionLogger GetOuterChild();

        /// <summary>
        /// Implement to Rollback this transaction object only.
        /// NOTE: don't rollback children, they are managed by "Rollback"
        /// function, rollback just this object.
        /// </summary>
        void InternalRollback(bool isDisposing);

        /// <summary>
        /// Commit Phase
        /// </summary>
        CommitPhase CurrentCommitPhase { get; set; }
    }
}
