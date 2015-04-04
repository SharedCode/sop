// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

namespace Sop.Transaction
{
    /// <summary>
    /// Transaction Commit Phase
    /// </summary>
    public enum CommitPhase
    {
        /// <summary>
        /// Transaction is ongoing and uncommitted
        /// </summary>
        UnCommitted,

        /// <summary>
        /// Transaction is in Phase 1 commit
        /// </summary>
        FirstPhase,

        /// <summary>
        /// Transaction is in Phase 2 or committed state
        /// </summary>
        SecondPhase,

        /// <summary>
        /// Committed = Phase 2, Transaction was committed
        /// </summary>
        Committed = SecondPhase,

        /// <summary>
        /// Transaction was rolled back, changes were undone
        /// </summary>
        Rolledback
    }
}