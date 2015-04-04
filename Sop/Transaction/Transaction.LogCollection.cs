// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using Sop.Persistence;

namespace Sop.Transaction
{
    using OnDisk;

    internal partial class Transaction
    {
        internal class BackupDataLogKeyComparer<T> : IComparer<T> where T : BackupDataLogKey
        {
            public int Compare(T x, T y)
            {
                BackupDataLogKey xKey = x;
                BackupDataLogKey yKey = y;
                int r = String.CompareOrdinal(xKey.SourceFilename, yKey.SourceFilename);
                return r == 0 ? xKey.SourceDataAddress.CompareTo(yKey.SourceDataAddress) : r;
            }
        }

        internal class BackupDataLogKeyComparer : IComparer
        {
            public int Compare(object x, object y)
            {
                BackupDataLogKey xKey = (BackupDataLogKey) x;
                BackupDataLogKey yKey = (BackupDataLogKey) y;
                int r = System.String.CompareOrdinal(xKey.SourceFilename, yKey.SourceFilename);
                return r == 0 ? xKey.SourceDataAddress.CompareTo(yKey.SourceDataAddress) : r;
            }
        }

        internal class BackupDataLogKey : InternalPersistent
        {
            public string SourceFilename = string.Empty;
            public long SourceDataAddress = -1;
            //public int Sequence;
            public override void Pack(IInternalPersistent parent, BinaryWriter writer)
            {
                writer.Write(SourceFilename);
                writer.Write(SourceDataAddress);
            }

            public override void Unpack(IInternalPersistent parent, BinaryReader reader)
            {
                SourceFilename = reader.ReadString();
                SourceDataAddress = reader.ReadInt64();
            }

            public override string ToString()
            {
                return string.Format("{0}.{1}", SourceFilename, SourceDataAddress);
            }
        }

        internal class BackupDataLogValue : InternalPersistent
        {
            private int _dataSize;

            public int DataSize
            {
                get { return _dataSize; }
                set
                {
                    if (value != _dataSize)
                    {
                        _isDirty = true;
                        _dataSize = value;
                    }
                }
            }

            private long _backupDataAddress = -1;

            public long BackupDataAddress
            {
                get { return _backupDataAddress; }
                set
                {
                    if (value != _backupDataAddress)
                    {
                        _isDirty = true;
                        _backupDataAddress = value;
                    }
                }
            }

            public override bool IsDirty
            {
                get { return _isDirty; }
                set { _isDirty = value; }
            }

            public int BackupFileHandle;

            /// <summary>
            /// ID of "winning" transaction that caused data to get backed up
            /// </summary>
            public int TransactionId = -1;

            public override void Pack(IInternalPersistent parent, BinaryWriter writer)
            {
                writer.Write(DataSize);
                writer.Write(BackupDataAddress);
            }

            public override void Unpack(IInternalPersistent parent, BinaryReader reader)
            {
                DataSize = reader.ReadInt32();
                BackupDataAddress = reader.ReadInt64();
            }

            public override string ToString()
            {
                return string.Format("{0}.{1}.{2}", BackupFileHandle, BackupDataAddress, DataSize);
            }
        }
    }
}