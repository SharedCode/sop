// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.IO;

namespace Sop.Transaction
{
    /// <summary>
    /// A blank Transaction Logger. Function overrides are meant to do nothing.
    /// </summary>
    internal class NoTransactionLogger : TransactionBase, ITransactionLogger
    {
        protected override void Flush() { }
        public override void Dispose() { }

        public override ITransactionLogger Begin(bool ownsRoot) { return this; }

        public override bool Commit(CommitPhase phase)
        {
            CurrentCommitPhase = CommitPhase.Committed;
            return true; 
        }
        public override void Rollback()
        {
            CurrentCommitPhase = CommitPhase.Rolledback;
        }
        public override bool InternalCommit(CommitPhase phase)
        {
            CurrentCommitPhase = phase;
            return true; 
        }
        public override void InternalRollback(bool isDisposing)
        {
            CurrentCommitPhase = CommitPhase.Rolledback;
        }

        protected internal override void RegisterAdd(CollectionOnDisk collection, long blockAddress, int blockSize) { }
        protected internal override void RegisterRemove(CollectionOnDisk collection, long blockAddress, int blockSize) { }
        protected internal override void RegisterFileGrowth(CollectionOnDisk collection, long segmentAddress, long segmentSize) { }
        protected internal override bool RegisterRecycle(CollectionOnDisk collection, long blockAddress, int blockSize) { return false; }
        protected internal override void RegisterRecycleCollection(CollectionOnDisk collection, long blockAddress, int blockSize) { }
        protected internal override bool RegisterSave(CollectionOnDisk collection, long blockAddress, int segmentSize,
                                         ConcurrentIOPoolManager readPool, ConcurrentIOPoolManager writePool) { return true; }
        protected internal override void TrackModification(CollectionOnDisk collection, bool untrack) { }
    }
}
