// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System.Collections.Generic;
using System.IO;
using Sop.OnDisk.Algorithm.Collection;
using Sop.Transaction;

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
        /// Write data to disk in bulk mode.
        /// This method can spin off multiple threads part of doing Asynchronous operations
        /// to accomplish following processes:
        /// - back up existing target data segments that will be overwritten to respective transaction log file.
        /// - overwrite target data segments with data from in-memory source provided (source parameter).
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="source"></param>
        /// <param name="dataChunks"></param>
        public void Write(Algorithm.Collection.ICollectionOnDisk parent, byte[] source, List<DataChunk> dataChunks)
        {
            Log.Logger.Instance.Log(Log.LogLevels.Information, "BulkWriter.Write begin.");

            byte[] data = source;
            ITransactionLogger trans = parent.Transaction;
            if (trans != null)
            {
                #region Async Backup target disk regions for update...

                using (var writePool = new ConcurrentIOPoolManager())
                {
                    using (var readPool = new ConcurrentIOPoolManager())
                    {
                        foreach (var chunk in dataChunks)
                        {
                            // Identify regions that were not backed up and overwritten yet then back them up...
                            ((TransactionBase) trans).RegisterSave((CollectionOnDisk) parent, chunk.TargetDataAddress,
                                                                   chunk.Size, readPool, writePool);
                        }
                    }
                }

                #endregion
            }

            #region Async write data segments from source onto respective target regions on disk...

            if (dataChunks.Count == 1 && dataChunks[0].Size <= (int)DataBlockSize.FiveTwelve)
            {
                var chunk = dataChunks[0];
                long dataAddress = chunk.TargetDataAddress;
                int dataIndex = chunk.Index;
                int dataSize = chunk.Size;
                var writer = parent.FileStream;
                if (dataAddress != writer.Position)
                    writer.Seek(dataAddress, SeekOrigin.Begin);
                writer.Write(data, dataIndex, dataSize);

                Log.Logger.Instance.Log(Log.LogLevels.Information, "BulkWriter.Write end (Size <= 512).");
                return;
            }
            using (var writePool2 = new ConcurrentIOPoolManager())
            {
                bool initial = true;
                foreach (var chunk in dataChunks)
                {
                    long dataAddress = chunk.TargetDataAddress;
                    int dataIndex = chunk.Index;
                    int dataSize = chunk.Size;
                    var writer = writePool2.GetInstance(parent.File.Filename, null, dataSize);

                    // extend file if needed on initial step of the loop.
                    long targetLastByteOnFileOffset = dataChunks[dataChunks.Count - 1].TargetDataAddress +
                                                      dataChunks[dataChunks.Count - 1].Size;
                    if (initial && writer.FileStream.Length < targetLastByteOnFileOffset)
                    {
                        initial = false;
                        writer.FileStream.Seek(targetLastByteOnFileOffset, SeekOrigin.Begin);
                    }

                    if (dataAddress != writer.FileStream.Position)
                        writer.FileStream.Seek(dataAddress, SeekOrigin.Begin);
                    var param = new[] {null, writer};
                    writer.FileStream.BeginWrite(data, dataIndex, dataSize, Transaction.Transaction.WriteCallback, param);
                }
            }

            #endregion

            Log.Logger.Instance.Log(Log.LogLevels.Information, "BulkWriter.Write end.");
        }
    }
}
