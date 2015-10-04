// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// Transaction interface.
    /// </summary>
    public interface ITransaction : IDisposable
    {
        /// <summary>
        /// Begin the transaction.
        /// </summary>
        /// <returns></returns>
        ITransaction Begin();

        /// <summary>
        /// Commit the transaction.
        /// </summary>
        /// <returns></returns>
        bool Commit();

        /// <summary>
        /// Rollback the transaction.
        /// </summary>
        void Rollback();

        /// <summary>
        /// Recycle transaction will commit an ongoing transaction
        /// and begin a new one. It will hold lock on the Stores
        /// in between this recycle process and relinguish the locks
        /// only when the new Transaction successfully got started.
        /// 
        /// This method is useful for threaded Transactions where
        /// Store locks need to be held until new Transaction is created
        /// to ensure no data changes will occur in between Commit
        /// and Begin of a new Transaction, which can occur in heavily
        /// threaded Store use environment.
        /// </summary>
        /// <param name="commit">true will commit ongioing transaction, false will rollback. Either way, a new Transactio will be started right after.</param>
        /// <returns></returns>
        ITransaction Cycle(bool commit = true);
    }
}
