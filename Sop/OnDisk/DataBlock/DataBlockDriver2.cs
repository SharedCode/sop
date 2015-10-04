// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

﻿using System;
using System.IO;
using System.Collections.Generic;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.Algorithm.LinkedList;
using Sop.OnDisk.IO;
using FileStream = Sop.OnDisk.File.FileStream;
using System.Threading.Tasks;
using System.Threading;

namespace Sop.OnDisk.DataBlock
{
    /// <summary>
    /// DataBlockDriver is a data block driver implementation for 
    /// writing/reading data blocks to/from a local file.
    /// </summary>
    internal partial class DataBlockDriver
    {
        /// Header:
        /// OccupiedBlock Head
        /// OccupiedBlock Tail
        /// DeletedBlock Head
        /// DeletedBlock Tail
        /// 
        /// Layout in Disk:
        /// Byte 0: Available or Occupied flag
        /// Byte 1 to 8: Next Item Address (64 bit long int)
        /// Byte 9 to 10: Size Occupied
        /// Byte 11 to 11 + Size Occupied: USER DATA
        /// Disk Layout:
        /// Block 1 -> Block 2 -> Block 3 -> Block n
        private void AddBlockToDisk(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block,
                                    bool isHead, bool isCollectionBlock, bool addToMru)
        {
            parent.IsDirty = true;
            if (block == null)
                throw new ArgumentNullException("block");
            Sop.DataBlock currentBlock = block, prevBlock = null;

            // save linked Blocks...
            do
            {
                if (AllocateNextBlock(parent, currentBlock, isHead, isCollectionBlock, addToMru))
                {
                    SetIsDirty(currentBlock, true); //ensure block will be written by MRUManager
                    Sop.DataBlock db = currentBlock.Next;
                    while (db != null)
                    {
                        SetIsDirty(db, true); //ensure block will be written by MRUManager
                        if (addToMru && db.SizeOccupied > 0)
                            ((CollectionOnDisk)parent).Blocks.Add(db.DataAddress, db);
                        db = db.Next;
                    }
                    return;
                }
                SetIsDirty(currentBlock, true); //ensure block will be written by MRUManager
                if (prevBlock != null)
                {
                    long prevId = GetId(prevBlock);
                    Sop.DataBlock db;
                    if (((CollectionOnDisk)parent).Blocks.TryGetValue(prevId, out db))
                    {
                        if (db.InternalNextBlockAddress != -1)
                            prevBlock.InternalNextBlockAddress = db.InternalNextBlockAddress;
                        ((CollectionOnDisk)parent).Blocks[prevId] = prevBlock;
                    }
                    prevBlock.NextItemAddress = currentBlock.DataAddress;
                    SetDiskAddress(parent, prevBlock, addToMru);
                }
                prevBlock = currentBlock;
                currentBlock = currentBlock.Next;
                isHead = false;
            } while (currentBlock != null && currentBlock.DataAddress == -1);
            SetDiskAddress(parent, prevBlock, addToMru);
        }

        /// <summary>
        /// Assign block(s) of space on disk to the Data Block(s).
        /// The assigned disk blocks' file offsets will be set as the blocks' DataAddresses.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="block">Data Block which will be assigned a block of space on disk</param>
        /// <param name="isCollectionBlock"> </param>
        /// <param name="addToMru">true will put the block into MRU, else not</param>
        public void SetDiskBlock(Algorithm.Collection.ICollectionOnDisk parent,
                                 Sop.DataBlock block,
                                 bool isCollectionBlock,
                                 bool addToMru = true)
        {
            Sop.DataBlock prevBlock = null;
            bool isHead = true;
            bool blockExtended = false;
            while (block != null)
            {
                if (block.DataAddress == -1)
                {
                    if (prevBlock != null)
                        blockExtended = true;
                    AddBlockToDisk(parent, block, isHead, isCollectionBlock, addToMru);
                }
                if (prevBlock != null)
                {
                    var db = ((CollectionOnDisk)parent).Blocks[GetId(prevBlock)];
                    if (db != null)
                    {
                        if (db.InternalNextBlockAddress != -1)
                            prevBlock.InternalNextBlockAddress = db.InternalNextBlockAddress;
                        ((CollectionOnDisk)parent).Blocks[GetId(prevBlock)] = prevBlock;
                    }
                    if (prevBlock.NextItemAddress != block.DataAddress)
                    {
                        if (blockExtended)
                        {
                            Log.Logger.Instance.Log(Log.LogLevels.Verbose, "DataBlockDriver.SetDiskBlock: block({0}) got extended.",
                                prevBlock.DataAddress);
                        }
                        prevBlock.NextItemAddress = block.DataAddress;
                        SetDiskAddress(parent, prevBlock, addToMru);
                    }
                    else
                    {
                        //ensure blocks will be written by MRUManager
                        SetIsDirty(prevBlock, true);
                        SetIsDirty(block, true);
                    }
                }
                prevBlock = block;
                block = block.Next;
                isHead = false;
            }
        }

        private void SetDiskAddress(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block, bool addToMru)
        {
            if (block.DataAddress < 0)
            {
                // reserve the block chunk of space on disk to the block and put it in MRU,
                // MRU Manager will take care of making another call to physically write the 
                // block when it's appropriate (during MRU fault).
                block.DataAddress = parent.FileStream.Position;
                parent.FileStream.Seek(block.Length, SeekOrigin.Current);
            }
            SetIsDirty(block, true); //ensure block will be written by MRUManager
        }

        internal MemoryStream BufferStream;
        internal BinaryWriter BinaryWriter;
        private byte[] _writeBuffer;

        /// <summary>
        /// Write a group of Blocks onto Disk. NOTE: it will be more optimal if Blocks
        /// are sorted by its Data Address so this function can write contiguous blocks
        /// in one async write.
        /// </summary>
        public int WriteBlocksToDisk(Algorithm.Collection.ICollectionOnDisk parent,
                                     IDictionary<long, Sop.DataBlock> blocks, bool clear)
        {
            if (!parent.IsOpen)
                return 0;

            var blockSize = (int)parent.DataBlockSize;
            int chunkSize = MaxSegmentSize / 2;
            if (chunkSize > blocks.Count * blockSize)
                chunkSize = blocks.Count * blockSize;

            if (_writeBuffer == null || _writeBuffer.Length < chunkSize)
                _writeBuffer = new byte[chunkSize];

            Sop.Transaction.Transaction.LogTracer.Verbose("WriteBlocksToDisk: Start for Thread {0}.", Thread.CurrentThread.ManagedThreadId);

            using (var backupWritePool = new ConcurrentIOPoolManager())
            {
                // Backup the target blocks on disk
                using (var backupReadPool = new ConcurrentIOPoolManager())
                {
                    WriteBlocksToDisk(backupReadPool, backupWritePool, parent, blocks);
                }
            }
            // overwrite the target blocks on disk with source blocks.
            using (var writePool = new ConcurrentIOPoolManager())
            {
                WriteBlocksToDisk(null, writePool, parent, blocks);
            }

            Sop.Transaction.Transaction.LogTracer.Verbose("WriteBlocksToDisk: End for Thread {0}.", Thread.CurrentThread.ManagedThreadId);

            return blocks.Count;
        }

        private void WriteBlocksToDisk(ConcurrentIOPoolManager readPool, ConcurrentIOPoolManager writePool,
            Algorithm.Collection.ICollectionOnDisk parent, IDictionary<long, Sop.DataBlock> blocks)
        {
            if (BinaryWriter == null)
            {
                if (BufferStream == null)
                    BufferStream = new MemoryStream();
                BinaryWriter = new BinaryWriter(BufferStream, parent.File.Server.Encoding);
            }
            else
                BinaryWriter.Seek(0, SeekOrigin.Begin);

            const int sizeOfNumerics = Sop.DataBlock.OverheadSize;
            var writeBuffer = _writeBuffer;

            int bufferIndex = 0, currentTargetBufferIndex = 0;
            long runningAddress = -1, startBlockAddress = -1;
            var bulkWriter = new BulkWriter();
            var dataChunks = new List<BulkWriter.DataChunk>(4);

            #region resize data file before appending to it...
            if (readPool == null || writePool == null)
            {
                if (((Collections.Generic.SortedDictionary<long, Sop.DataBlock>)blocks).MoveLast())
                {
                    // thread safe increase File Size to accomodate data to be appended...
                    var FileSize = ((Collections.Generic.SortedDictionary<long, Sop.DataBlock>)blocks).CurrentKey +
                        (int)parent.DataBlockSize;
                    if (parent.FileStream.Length < FileSize)
                    {
                        if (parent.Transaction != null)
                        {
                            lock (parent.Transaction)
                            {
                                if (parent.FileStream.Length < FileSize)
                                    parent.FileStream.SetLength(FileSize);
                            }
                        }
                        else
                            parent.FileStream.SetLength(FileSize);
                    }
                }
            }
            #endregion
            foreach (Sop.DataBlock block in blocks.Values)
            {
                SetIsDirty(block, false);
                if (block.DataAddress >= 0)
                {
                    #region Process special states, e.g. - buffer is full, current block Address is fragmented from previous block's
                    if (startBlockAddress == -1)
                        startBlockAddress = runningAddress = block.DataAddress;
                    else
                    {
                        bool bufferIsFull = bufferIndex + sizeOfNumerics + block.Data.Length +
                                            currentTargetBufferIndex > writeBuffer.Length - block.Length;
                        if (block.DataAddress != runningAddress || bufferIsFull)
                        {
                            dataChunks.Add(new BulkWriter.DataChunk
                            {
                                TargetDataAddress = startBlockAddress,
                                Index = currentTargetBufferIndex,   // Index in the buffer of 1st byte of this segment
                                Size = bufferIndex                  // size of the segment
                            });
                            if (bufferIsFull)
                            {
                                //** write to disk
                                if (readPool != null && writePool != null)
                                {
                                    bulkWriter.Backup(readPool, writePool, parent, writeBuffer, dataChunks);
                                    if (writePool.AsyncThreadException != null)
                                        throw writePool.AsyncThreadException;
                                    else if (readPool.AsyncThreadException != null)
                                        throw readPool.AsyncThreadException;
                                }
                                else if (writePool != null)
                                {
                                    bulkWriter.Write(writePool, parent, writeBuffer, dataChunks);
                                    if (writePool.AsyncThreadException != null)
                                        throw writePool.AsyncThreadException;
                                }
                                else
                                    throw new SopException("WriteBlocksToDisk has a bug!");

                                // create new buffer for succeeding chunks...
                                dataChunks = new List<BulkWriter.DataChunk>(4);
                                writeBuffer = new byte[writeBuffer.Length];
                                currentTargetBufferIndex = 0;
                            }
                            else
                            {
                                currentTargetBufferIndex += bufferIndex;
                            }
                            bufferIndex = 0;
                            runningAddress = startBlockAddress = block.DataAddress;
                        }
                    }
                    #endregion
                }
                else
                    throw new InvalidOperationException("Invalid (-) Block.DataAddress detected.");

                //**** write Block Header and Data to disk
                BinaryWriter.Seek(0, SeekOrigin.Begin);
                // Byte 0 to 7: Next Item Address (64 bit long int) = 0 (no next item)
                BinaryWriter.Write(block.NextItemAddress);
                // Byte 8 to 11: Size Occupied
                BinaryWriter.Write(block.SizeOccupied);
                // Byte 12 to 19: Low-level next datablock address
                BinaryWriter.Write(block.InternalNextBlockAddress);
                // Byte 20: count of member blocks, max is 65535.
                ushort memberCount = 0;
                if (block.IsHead)
                {
                    int cm = block.CountMembers(true);
                    memberCount = cm > Sop.DataBlock.MaxChainMemberCount ? Sop.DataBlock.MaxChainMemberCount : (ushort)cm;
                }
                BinaryWriter.Write(memberCount);

                byte[] b2 = BufferStream.GetBuffer();
                Array.Copy(b2, 0, writeBuffer, currentTargetBufferIndex + bufferIndex, sizeOfNumerics);
                bufferIndex += sizeOfNumerics;

                //** Byte 20 to 20 + Data Length: USER DATA
                int cs = block.Data.Length;
                //if (currentTargetBufferIndex + cs + bufferIndex > writeBuffer.Length - block.Length)
                //    cs = writeBuffer.Length - (currentTargetBufferIndex + bufferIndex);
                Array.Copy(block.Data, 0, writeBuffer, currentTargetBufferIndex + bufferIndex, cs);

                bufferIndex += block.Data.Length;
                runningAddress += block.Length;
            }

            // write the last chunk set to disk...
            if (startBlockAddress != -1)
            {
                //** write to disk
                dataChunks.Add(new BulkWriter.DataChunk
                {
                    TargetDataAddress = startBlockAddress,
                    Index = currentTargetBufferIndex,
                    Size = bufferIndex
                });
            }
            if (dataChunks.Count > 0)
            {
                if (readPool != null && writePool != null)
                    bulkWriter.Backup(readPool, writePool, parent, writeBuffer, dataChunks);
                else if (writePool != null)
                    bulkWriter.Write(writePool, parent, writeBuffer, dataChunks);
                else
                    throw new SopException("WriteBlocksToDisk has a bug!");
            }
        }

        public Sop.DataBlock ReadBlockFromDisk(Algorithm.Collection.ICollectionOnDisk parent, bool getForRemoval)
        {
            var r = CreateBlock(parent.DataBlockSize);
            return ReadBlockFromDisk(parent, parent.CurrentEntryDataAddress, getForRemoval, r);
        }

        public void ReadBlockFromDisk(Algorithm.Collection.ICollectionOnDisk parent,
            List<Algorithm.BTree.BTreeItemOnDisk> items, System.Func<int, bool> readCallback)
        {
            var sortedBlocks = new Sop.Collections.Generic.SortedDictionary<long, int>();
            var dataSegments = new Sop.Collections.Generic.SortedDictionary<long, long>();

            for (int i = 0; i < items.Count; i++)
            {
                var address = GetId(items[i].Value.DiskBuffer);
                sortedBlocks.Add(address, i);
            }
            dataSegments.Clear();

            //detect contiguous blocks & read these data blocks as a bigger segment for optimal reading.
            KeyValuePair<long, int> lastEntry;
            List<KeyValuePair<long, int>> blockAddresses = new List<KeyValuePair<long, int>>();
            foreach (var entry in sortedBlocks)
            {
                lastEntry = entry;
                var address = entry.Key;
                blockAddresses.Add(entry);
                if (!Algorithm.BTree.IndexedBlockRecycler.DetectAndMerge(dataSegments,
                    address, items[entry.Value].Value.DiskBuffer.contiguousBlockCount * (int)parent.DataBlockSize, MaxSegmentSize))
                {
                    _readAheadBuffer.Clear();
                    dataSegments.MoveFirst();
                    _readAheadBuffer.Read(parent.FileStream, dataSegments.CurrentKey, (int)dataSegments.CurrentValue);

                    foreach (var addr in blockAddresses)
                    {
                        var rab = new DataBlockReadBufferLogic(_readAheadBuffer);
                        var block = ReadBlockFromDisk(parent, addr.Key, false);
                        items[addr.Value].Value.DiskBuffer = block;
                        // process(deserialize the Object) the read blocks...
                        readCallback(addr.Value);
                        _readAheadBuffer = rab;
                    }
                    blockAddresses.Clear();
                    dataSegments.Clear();
                    dataSegments.Add(address, items[entry.Value].Value.DiskBuffer.contiguousBlockCount * (int)parent.DataBlockSize);
                }
            }
            // process last data segment...
            if (dataSegments.Count > 0)
            {
                _readAheadBuffer.Clear();
                dataSegments.MoveFirst();
                _readAheadBuffer.Read(parent.FileStream, dataSegments.CurrentKey, (int)dataSegments.CurrentValue);
                foreach (var addr in blockAddresses)
                {
                    var rab = new DataBlockReadBufferLogic(_readAheadBuffer);
                    var block = ReadBlockFromDisk(parent, addr.Key, false);
                    items[addr.Value].Value.DiskBuffer = block;
                    // process(deserialize the Object) the read blocks...
                    readCallback(addr.Value);
                    _readAheadBuffer = rab;
                }
            }
        }

        internal const int MaxSegmentSize = 524288;

        public Sop.DataBlock ReadBlockFromDisk(Algorithm.Collection.ICollectionOnDisk parent, long address, bool getForRemoval)
        {
            if (address >= 0)
            {
                var o = ((CollectionOnDisk)parent).Blocks[address];
                if (o != null)
                {
                    getForRemoval = true;
                    if (o.SizeOccupied > 0)
                    {
                        if (o.NextItemAddress >= 0 && o.Next == null)
                            o.Next = ReadBlockFromDisk(parent, o.NextItemAddress, getForRemoval);
                        return o;
                    }
                }
            }
            if (parent.FileStream.Length > address)
            {
                if (parent is LinkedListOnDisk)
                    ((LinkedListOnDisk)parent).MoveTo(address);
                Sop.DataBlock d = CreateBlock(parent.DataBlockSize);
                d = ReadBlockFromDisk(parent, address, getForRemoval, getForRemoval, d);
                if (!d.IsEmpty())
                    ReadBlock(parent, d, getForRemoval);
                _readAheadBuffer.Clear();
                return d;
            }
            Sop.DataBlock r = CreateBlock(parent.DataBlockSize);
            r.DataAddress = address;
            return r;
        }

        private Sop.DataBlock ReadBlock(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block, bool getForRemoval)
        {
            // Read the block
            if (block.SizeAvailable == 0 || (block.Next == null && block.NextItemAddress >= 0))
            {
                Sop.DataBlock d = block;
                //** read rest of blocks
                while (d.NextItemAddress >= 0)
                {
                    d = ReadNextBlock(parent, d, getForRemoval);
                }
            }
            return block;
        }

        internal Sop.DataBlock ReadBlockFromDisk(Algorithm.Collection.ICollectionOnDisk parent, long dataAddress, bool getForRemoval,
                                             Sop.DataBlock block)
        {
            Sop.DataBlock d = block;
            ReadBlockFromDiskOrInitializeIfEmpty(parent, dataAddress, getForRemoval, d);
            return ReadBlock(parent, d, getForRemoval);
        }

        private Sop.DataBlock ReadNextBlock(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock d, bool getForRemoval)
        {
            // read Next block
            if (d.NextItemAddress != -1)
            {
                if (d.Next == null)
                    d.Next = CreateBlock((DataBlockSize)d.Length);
                d.Next.DataAddress = d.NextItemAddress;
            }
            d = d.Next;
            if (d != null && d.DataAddress >= 0)
                ReadBlockFromDiskOrInitializeIfEmpty(parent, d.DataAddress, getForRemoval, d);
            return d;
        }


        public List<Sop.DataBlock.Info> ReadBlockInfoFromDisk(Algorithm.Collection.ICollectionOnDisk parent, long dataAddress)
        {
            // todo: support reading from Transaction backup segments...
            //byte[] backedupData = Transaction.Transaction.ReadBlockFromBackup(parent,
            //                                                                  dataAddress, getForRemoval,
            //                                                                  readMetaInfoOnly);

            FileStream fileStream = parent.FileStream;
            if (fileStream.Position != dataAddress)
                fileStream.Seek(dataAddress, SeekOrigin.Begin);
            List<Sop.DataBlock.Info> r = new List<Sop.DataBlock.Info>();
            if (fileStream.Length > 0)
            {
                var metaData = new byte[Sop.DataBlock.OverheadSize];
                var countRead = fileStream.Read(metaData, 0, metaData.Length);
                if (countRead < metaData.Length)
                {
                    // check if data is not on disk, just return empty array to mean data not found...
                    if (countRead == 0)
                        return r;
                    throw new SopException(string.Format("Expected to read {0} bytes but read {1} bytes instead.", metaData.Length, countRead));
                }
                var da = dataAddress;
                while (true)
                {
                    using (var ms = new MemoryStream(metaData))
                    {
                        using (var br = new BinaryReader(ms, parent.File.Server.Encoding))
                        {
                            Sop.DataBlock.Info blockInfo = new Sop.DataBlock.Info()
                            {
                                Address = da
                            };
                            // Byte 0 to 7: Next Item Address (64 bit long int) = 0 (no next item)
                            var nextItemAddress = br.ReadInt64();
                            // Byte 8 to 11: Size Occupied
                            var sizeOccupied = br.ReadInt32();
                            // Byte 12 to 19: Low-level next block address
                            var internalNextBlockAddress = br.ReadInt64();
                            blockInfo.BlockCount = br.ReadUInt16();
                            r.Add(blockInfo);

                            // read next block segment info...
                            if (blockInfo.BlockCount > 1)
                            {
                                var addr = blockInfo.Address + (blockInfo.BlockCount - 1) * (int)parent.DataBlockSize;
                                fileStream.Seek(blockInfo.Address + (blockInfo.BlockCount - 1) * (int)parent.DataBlockSize, SeekOrigin.Begin);
                                countRead = fileStream.Read(metaData, 0, sizeof(long));
                                if (countRead < sizeof(long))
                                    throw new SopException(string.Format("Expected to read {0} bytes but read {1} bytes instead.", sizeof(long), countRead));
                                using (var ms2 = new MemoryStream(metaData))
                                {
                                    using (var br2 = new BinaryReader(ms2, parent.File.Server.Encoding))
                                    {
                                        nextItemAddress = br2.ReadInt64();
                                    }
                                }
                            }
                            if (nextItemAddress != -1)
                            {
                                fileStream.Seek(nextItemAddress, SeekOrigin.Begin);
                                da = fileStream.Position;
                            }
                            else
                                break;
                        }
                    }
                    countRead = fileStream.Read(metaData, 0, metaData.Length);
                    if (countRead < metaData.Length)
                        throw new SopException(string.Format("Expected to read {0} bytes but read {1} bytes instead.", metaData.Length, countRead));
                }
            }
            return r;
        }

        private Sop.DataBlock ReadBlockFromDisk(Algorithm.Collection.ICollectionOnDisk parent,
                                            long dataAddress, bool getForRemoval, bool readMetaInfoOnly,
                                            Sop.DataBlock target)
        {
            byte[] backedupData = Transaction.Transaction.ReadBlockFromBackup(parent,
                                                                              dataAddress, getForRemoval,
                                                                              readMetaInfoOnly);
            Sop.DataBlock block = target;
            if (backedupData == null && !_readAheadBuffer.IsEmpty)
            {
                backedupData = _readAheadBuffer.Get(dataAddress, block.Length);
            }
            FileStream fileStream = parent.FileStream;
            if (backedupData == null && fileStream.Position != dataAddress)
                fileStream.Seek(dataAddress, SeekOrigin.Begin);
            if (backedupData != null || fileStream.Length > 0)
            {
                if (block.DataAddress < 0)
                    block.DataAddress = dataAddress;

                BinaryReader br;
                if (backedupData == null)
                {
                    if (fileStream.Position != block.DataAddress)
                        fileStream.Seek(block.DataAddress, SeekOrigin.Begin);
                    br = fileStream.CreateBinaryReader(parent.File.Server.Encoding);
                    try
                    {
                        backedupData = new byte[readMetaInfoOnly ? Sop.DataBlock.OverheadSize : block.Length];
                        var countRead = fileStream.Read(backedupData, 0, backedupData.Length);
                        if (countRead < backedupData.Length)
                            throw new SopException(string.Format("Expected to read {0} bytes but read {1} bytes instead.", backedupData.Length, countRead));
                    }
                    finally
                    {
                        br.Close();
                    }
                }

                #region read data from byte array!
                using (var ms = new MemoryStream(backedupData))
                {
                    using (br = new BinaryReader(ms, parent.File.Server.Encoding))
                    {
                        //**** read Block Header and Data to disk
                        // Byte 0 to 7: Next Item Address (64 bit long int) = 0 (no next item)
                        block.NextItemAddress = br.ReadInt64();
                        // Byte 8 to 11: Size Occupied
                        block.SizeOccupied = br.ReadInt32();
                        // Byte 12 to 19: Low-level next block address
                        block.InternalNextBlockAddress = br.ReadInt64();

                        var memberCount = br.ReadUInt16();

                        if (block.SizeOccupied == 0 && block.NextItemAddress <= 0 && block.InternalNextBlockAddress <= 0)
                        {
                            block.InternalNextBlockAddress = block.NextItemAddress = -1;
                        }

                        if (!readMetaInfoOnly)
                        {
                            if (block.SizeOccupied > 0)
                                Array.Copy(backedupData, br.BaseStream.Position, block.Data, 0, block.SizeOccupied);

                            if (!getForRemoval)
                            {
                                SetIsDirty(block, false);
                            }
                            if (!block.IsHead && memberCount > 0)
                                block.IsHead = true;
                            // encache rest of blocks' data as a read ahead buffer...
                            if (memberCount > 1)
                            {
                                _readAheadBuffer.Read(fileStream, block.NextItemAddress, (memberCount - 1) * block.Length);
                            }
                        }
                    }
                }
                #endregion
            }
            return block;
        }
        private DataBlockReadBufferLogic _readAheadBuffer = new DataBlockReadBufferLogic();

        /// <summary>
        /// Read Block from Disk
        /// </summary>
        /// <returns></returns>
        private Sop.DataBlock ReadBlockFromDiskOrInitializeIfEmpty(
            Algorithm.Collection.ICollectionOnDisk parent,
            long dataAddress, bool getForRemoval, Sop.DataBlock block)
        {
            if (parent.FileStream.Length > 0)
            {
                block = ReadBlockFromDisk(parent, dataAddress, getForRemoval, getForRemoval, block);
            }
            else
                block.Initialize();
            return block;
        }

        /// <summary>
        /// Remove Block from the Collection.
        /// NOTE: this is used by BTreeAlgorithm when deleting a node's and/or 
        /// a node's item's data block(s) on disk.
        /// </summary>
        public bool Remove(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block, bool clearBlock = true)
        {
            if (!parent.File.Server.HasTrashBin)
                return false;
            // remove block and linked blocks from MRU
            Sop.DataBlock d = block;
            long contiguousBlockStartAddress = -1;
            long currentBlockAddress = -1;
            int contiguousBlockCount = 0;

            var deletedBlocks = ((CollectionOnDisk)parent).DeletedBlocks;
            IDictionary<long, Sop.DataBlock> blocks = ((CollectionOnDisk)parent).Blocks;

            if (!d.IsFolded)
                d.Fold();

            if (d.IsFolded)
            {
                if (clearBlock && d.SizeOccupied > 0)
                    d.SizeOccupied = 0;
                foreach (var segment in d.foldedDataAddresses)
                {
                    if (deletedBlocks != null)
                    {
                        RegisterRemovedBlock((CollectionOnDisk)parent, segment.Address, segment.BlockCount * (int)parent.DataBlockSize);
                        // add removed block to the recycle Store for reuse later on Add operation
                        deletedBlocks.AddAvailableBlock(segment.Address, segment.BlockCount * (int)parent.DataBlockSize);
                    }
                }
            }
            else
            {
                #region Legacy code for removing unfolded Blocks
                do
                {
                    if (d.DataAddress >= 0)
                    {
                        blocks.Remove(d.DataAddress);
                    }
                    if (currentBlockAddress >= 0 && currentBlockAddress + (int)parent.DataBlockSize == d.DataAddress)
                    {
                        // if current block is part of a single contiguous block
                        contiguousBlockCount++;
                        currentBlockAddress = d.DataAddress;
                    }
                    else
                    {
                        if (currentBlockAddress != -1)
                        {
                            // recycle segment
                            if (deletedBlocks != null)
                            {
                                RegisterRemovedBlock((CollectionOnDisk)parent, contiguousBlockStartAddress, contiguousBlockCount * (int)parent.DataBlockSize);
                                // add removed block to the recycle Store for reuse later on Add operation
                                deletedBlocks.AddAvailableBlock(contiguousBlockStartAddress,
                                                                contiguousBlockCount * (int)parent.DataBlockSize);
                            }
                        }
                        currentBlockAddress = d.DataAddress;
                        contiguousBlockStartAddress = d.DataAddress;
                        contiguousBlockCount = 1;
                    }

                    // clear block's data
                    if (clearBlock && d.SizeOccupied > 0)
                        d.SizeOccupied = 0;

                    if (d.Next == null && d.NextItemAddress >= 0)
                        //load the next datablock to get its next block address...
                        d.Next = ReadBlockFromDisk(parent, parent.FileStream.Position, true, true,
                                                   CreateBlock(parent.DataBlockSize));

                    d = d.Next;
                } while (d != null);

                // add to deleted blocks store any deleted block haven't added yet
                if (contiguousBlockStartAddress != -1)
                {
                    if (deletedBlocks != null)
                    {
                        RegisterRemovedBlock((CollectionOnDisk)parent, contiguousBlockStartAddress, contiguousBlockCount * (int)parent.DataBlockSize);
                        // add removed block to the recycle Store for reuse later on Add operation
                        deletedBlocks.AddAvailableBlock(contiguousBlockStartAddress,
                                                        contiguousBlockCount * (int)parent.DataBlockSize);
                    }
                }
                #endregion
            }
            parent.RegisterChange(true);
            return true;
        }

        private void RegisterRemovedBlock(CollectionOnDisk container, long blockAddress, int blockSize)
        {
            // register removed block
            if (container.Transaction != null)
                ((Transaction.TransactionBase)container.Transaction).Register(
                    Sop.Transaction.ActionType.Remove, (CollectionOnDisk)container, blockAddress, blockSize);
        }

        /// <summary>
        /// Delete the Collection for recycling
        /// </summary>
        public void Delete(Algorithm.Collection.ICollectionOnDisk parent)
        {
            if (HeaderData != null && HeaderData.OccupiedBlocksHead != null)
            {
                //** add deleted collection start and end block info to the deleted blocks collection of the File
                var dbi = new DeletedBlockInfo
                {
                    StartBlockAddress = HeaderData.OccupiedBlocksHead.DataAddress,
                    EndBlockAddress = HeaderData.EndAllocatableAddress
                };
                bool oc = false;
                if (parent.File.DeletedCollections != null)
                {
                    oc = ((Algorithm.BTree.IBTreeAlgorithm)parent.File.DeletedCollections).ChangeRegistry;
                    ((Algorithm.BTree.IBTreeAlgorithm)parent.File.DeletedCollections).ChangeRegistry =
                        ((Algorithm.BTree.IBTreeAlgorithm)parent).ChangeRegistry;
                    parent.File.DeletedCollections.Add(dbi);
                }

                //** Reset count to 0 and save the header to disk
                HeaderData.Count = 0;
                if (!parent.IsTransactionStore)
                    parent.RegisterChange(true);
                if (parent.File.DeletedCollections != null)
                    ((Algorithm.BTree.IBTreeAlgorithm)parent.File.DeletedCollections).ChangeRegistry = oc;
                HeaderData.Clear();
            }
        }
    }
}
