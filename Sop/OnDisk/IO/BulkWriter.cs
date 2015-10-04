// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System.Collections.Generic;
using System.IO;
using Sop.OnDisk.Algorithm.Collection;
using Sop.Transaction;
using System.Threading;

namespace Sop.OnDisk.IO
{
    /// <summary>
    /// Bulk Writer does a multi-threaded, asynchronous read/write operations
    /// for writing/reading big amount of data to/from target locations on disk.
    /// </summary>
    internal class BulkWriter
    {
        /// <summary>
        /// Signifies the data chunk from in-memory source (byte[]) that needs to be written to disk.
        /// </summary>
        internal class DataChunk
        {
            /// <summary>
            /// Target file offset where start of data chunk needs to be written.
            /// </summary>
            public long TargetDataAddress;
            /// <summary>
            /// Start of data chunk (from source) that needs to get written to disk.
            /// </summary>
            public int Index;
            /// <summary>
            /// Size of the data chunk that needs to get written to disk.
            /// </summary>
            public int Size;
        }

        /// <summary>
        /// Backup the target blocks on disk.
        /// </summary>
        /// <param name="readPool"></param>
        /// <param name="writePool"></param>
        /// <param name="parent"></param>
        /// <param name="source"></param>
        /// <param name="dataChunks"></param>
        public void Backup(ConcurrentIOPoolManager readPool, ConcurrentIOPoolManager writePool,
            Algorithm.Collection.ICollectionOnDisk parent, byte[] source, List<DataChunk> dataChunks)
        {
            ITransactionLogger trans = parent.Transaction;
            if (trans != null)
            {
                Sop.Transaction.Transaction.LogTracer.Verbose("BulkWriter.Backup: Start for Thread {0}.", Thread.CurrentThread.ManagedThreadId);
                foreach (var chunk in dataChunks)
                {
                    Sop.Transaction.Transaction.LogTracer.Verbose("BulkWriter.Backup: inside foreach chunk {0} Thread {1}.", chunk.TargetDataAddress, Thread.CurrentThread.ManagedThreadId);
                    // Identify regions that were not backed up and overwritten yet then back them up...
                    ((TransactionBase)trans).RegisterSave((CollectionOnDisk)parent, chunk.TargetDataAddress,
                                                           chunk.Size, readPool, writePool);
                }
            }
        }

        /// <summary>
        /// Write data to disk in bulk mode.
        /// This method can spin off multiple threads part of doing Asynchronous operations
        /// to accomplish following processes:
        /// - back up existing target data segments that will be overwritten to respective transaction log file.
        /// - overwrite target data segments with data from in-memory source provided (source parameter).
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="source"></param>
        /// <param name="dataChunks"></param>
        public void Write(ConcurrentIOPoolManager writePool,
            Algorithm.Collection.ICollectionOnDisk parent,
            byte[] source, List<DataChunk> dataChunks)
        {
            Log.Logger.Instance.Log(Log.LogLevels.Information, "BulkWriter.Write begin.");
            byte[] data = source;

            #region Async write data segments from source onto respective target regions on disk...
#if (_WIN32)
            if (dataChunks.Count == 1 && dataChunks[0].Size <= (int)512)    //DataBlockSize.Minimum)
            {
                var chunk = dataChunks[0];
                long dataAddress = chunk.TargetDataAddress;
                int dataIndex = chunk.Index;
                int dataSize = chunk.Size;
                var writer = parent.FileStream;
                if (dataAddress != writer.GetPosition(true))
                    writer.Seek(dataAddress, SeekOrigin.Begin, true);
                writer.Write(data, dataIndex, dataSize);
                Log.Logger.Instance.Log(Log.LogLevels.Information, "BulkWriter.Write end (Size <= {0}).", (int)512);    //DataBlockSize.Minimum);
                return;
            }
#endif
            bool initial = true;
            foreach (var chunk in dataChunks)
            {
                // short circuit if IO exception is encountered.
                if (writePool.AsyncThreadException != null)
                    throw writePool.AsyncThreadException;

                long dataAddress = chunk.TargetDataAddress;
                int dataIndex = chunk.Index;
                int dataSize = chunk.Size;
                var writer = writePool.GetInstance(parent.File.Filename, null);

                // extend file if needed on initial step of the loop.
                if (initial)
                {
                    initial = false;
                    long targetLastByteOnFileOffset = dataChunks[dataChunks.Count - 1].TargetDataAddress +
                                                      dataChunks[dataChunks.Count - 1].Size;
                    if (writer.FileStream.Length < targetLastByteOnFileOffset)
                    {
                        writer.FileStream.Seek(targetLastByteOnFileOffset, SeekOrigin.Begin, true);
                    }
                }

                writer.FileStream.InUse = true;
                if (dataAddress != writer.FileStream.GetPosition(true))
                    writer.FileStream.Seek(dataAddress, SeekOrigin.Begin, true);
                var param = new[] { null, writer };
                writer.FileStream.BeginWrite(data, dataIndex, dataSize, Transaction.Transaction.WriteCallback, param);
            }

            #endregion

            Log.Logger.Instance.Log(Log.LogLevels.Information, "BulkWriter.Write end.");
        }
    }
}
