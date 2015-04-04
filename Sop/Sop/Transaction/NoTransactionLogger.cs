// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
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
        public override bool Commit(CommitPhase phase) { return true; }
        public override void Rollback() { }
        public override bool InternalCommit(CommitPhase phase) { return true; }
        public override void InternalRollback(bool isDisposing) { }

        protected internal override void RegisterAdd(CollectionOnDisk collection, long blockAddress, int blockSize) { }
        protected internal override void RegisterRemove(CollectionOnDisk collection) { }
        protected internal override void RegisterFileGrowth(CollectionOnDisk collection, long segmentAddress, long segmentSize) { }
        protected internal override void RegisterRecycle(CollectionOnDisk collection, long blockAddress, int blockSize) { }
        protected internal override void RegisterRecycleCollection(CollectionOnDisk collection, long blockAddress, int blockSize) { }
        protected internal override bool RegisterSave(CollectionOnDisk collection, long blockAddress, int segmentSize,
                                         ConcurrentIOPoolManager readPool, ConcurrentIOPoolManager writePool) { return true; }
    }
}
