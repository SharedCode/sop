// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using Sop.Persistence;

namespace Sop.Transaction
{
    using OnDisk;

    internal partial class Transaction : ITransactionLogger
    {
        /// <summary>
        /// Transaction "Action" Record Key
        /// </summary>
        internal class RecordKey : DataReference
        {
            public RecordKey()
            {
            }
        }

        /// <summary>
        /// Transaction "Action" Record contains information about a management 
        /// action done to an ObjectTable.
        /// </summary>
        internal class Record : InternalPersistent
        {
            public Record()
            {
            }

            public Record(RecordKey Key)
            {
                this.Key = Key;
            }

            /// <summary>
            /// Record Key
            /// </summary>
            public RecordKey Key = null;

            /// <summary>
            /// RecordData contains the block's data before it got updated.
            /// During rollback, this data is applied to the block to restore or undo changes.
            /// </summary>
            public byte[] Data = null;

            public override void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
            {
            }

            public override void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
            {
            }
        }
    }
}