// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

﻿using System;
using System.IO;
using System.Collections.Generic;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.Algorithm.LinkedList;
using Sop.OnDisk.IO;
using FileStream = Sop.OnDisk.File.FileStream;

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

            //** save linked Blocks...
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
                        //MruManager.Add(db.DataAddress, db);
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
                        SetIsDirty(prevBlock, true); //ensure block will be written by MRUManager
                        SetIsDirty(block, true); //ensure block will be written by MRUManager
                        // add to Blocks?   //90;
                        //if (addToMru)
                        //    MruManager.Add(prevBlock.DataAddress, prevBlock);
                    }
                }
                prevBlock = block;
                block = block.Next;
                isHead = false;
            }
            // Check if Blocks is full? //90;
            //if (MruManager == null || !MruManager.GeneratePruneEvent) return;
            //var m = MruManager;
            //if (m.CheckIfFull())
            //{
            //    var cod = (CollectionOnDisk) MruManager.Collection;
            //    if (cod != null)
            //        cod.OnMaxCapacity(m.Count - m.MinCapacity);
            //}
        }

        private void SetDiskAddress(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block, bool addToMru)
        {
            if (block.DataAddress < 0)
            {
                //** reserve the block chunk of space on disk to the block and put it in MRU,
                //** MRU Manager will take care of making another call to physically write the 
                //** block when it's appropriate (during MRU fault).
                block.DataAddress = parent.FileStream.Position;
                parent.FileStream.Seek(block.Length, SeekOrigin.Current);
            }
            SetIsDirty(block, true); //ensure block will be written by MRUManager
            //if (addToMru && block.SizeOccupied > 0)
            //    MruManager.Add(block.DataAddress, block);
        }

        internal MemoryStream BufferStream;
        internal BinaryWriter BinaryWriter;
        private byte[] _writeBuffer;

        /// <summary>
        /// Write a group of Blocks into Disk. NOTE: it will be more optimal if Blocks
        /// are sorted by its Data Address so this function can write contiguous blocks
        /// in one async write.
        /// </summary>
        public int WriteBlocksToDisk(Algorithm.Collection.ICollectionOnDisk parent,
                                     IDictionary<long, Sop.DataBlock> blocks, bool clear)
        {
            if (!parent.IsOpen)
                return 0;

            var blockSize = (int)parent.DataBlockSize;
            int r = blocks.Count;

            if (BinaryWriter == null)
            {
                if (BufferStream == null)
                    BufferStream = new MemoryStream();
                BinaryWriter = new BinaryWriter(BufferStream, parent.File.Server.Encoding);
            }
            else
                BinaryWriter.Seek(0, SeekOrigin.Begin);

            const int sizeOfNumerics = Sop.DataBlock.OverheadSize;
            int chunkSize = (int)DataBlockSize.FiveTwentyFourTwoEightyEight * 4;
            if (chunkSize > blocks.Count * blockSize)
                chunkSize = blocks.Count * blockSize;

            if (_writeBuffer == null || _writeBuffer.Length < chunkSize)
                _writeBuffer = new byte[chunkSize];
            int bufferIndex = 0, startIndex = 0, currentTargetBufferIndex = 0;
            long runningAddress = -1, startBlockAddress = -1;
            var bulkWriter = new BulkWriter();
            var dataChunks = new List<BulkWriter.DataChunk>(4);

            foreach (Sop.DataBlock block in blocks.Values)
            {
                SetIsDirty(block, false);
                if (block.DataAddress >= 0)
                {
                    #region Process special states, e.g. - buffer is full
                    if (startBlockAddress == -1)
                        startBlockAddress = runningAddress = block.DataAddress;
                    else
                    {
                        bool bufferIsFull = (bufferIndex - startIndex) + sizeOfNumerics + block.Data.Length +
                                            currentTargetBufferIndex > _writeBuffer.Length - block.Length;
                        if (block.DataAddress != runningAddress || bufferIsFull)
                        {
                            dataChunks.Add(new BulkWriter.DataChunk
                                               {
                                                   TargetDataAddress =
                                                       startBlockAddress == -1
                                                           ? block.DataAddress
                                                           : startBlockAddress,
                                                   Index = currentTargetBufferIndex + startIndex,
                                                   Size = bufferIndex - startIndex
                                               });
                            if (bufferIsFull)
                            {
                                //** write to disk
                                bulkWriter.Write(parent, _writeBuffer, dataChunks);
                                //** reset buffer
                                dataChunks.Clear();
                                currentTargetBufferIndex = 0;
                            }
                            else
                            {
                                currentTargetBufferIndex += (bufferIndex - startIndex);
                            }
                            startIndex = bufferIndex = 0;
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
                // Byte 20: count of member blocks, max is 255.
                byte memberCount = 0;
                if (block.IsHead)
                {
                    int cm = block.CountMembers(true);
                    memberCount = cm > byte.MaxValue ? byte.MaxValue : (byte)cm;
                }
                BinaryWriter.Write(memberCount);

                byte[] b2 = BufferStream.GetBuffer();
                Array.Copy(b2, 0, _writeBuffer, currentTargetBufferIndex + bufferIndex, sizeOfNumerics);
                bufferIndex += sizeOfNumerics;

                //** Byte 20 to 20 + Data Length: USER DATA
                int cs = block.Data.Length;
                if (currentTargetBufferIndex + cs + bufferIndex > _writeBuffer.Length - block.Length)
                    cs = _writeBuffer.Length - (currentTargetBufferIndex + bufferIndex);
                Array.Copy(block.Data, 0, _writeBuffer, currentTargetBufferIndex + bufferIndex, cs);

                bufferIndex += block.Data.Length;
                runningAddress += block.Length;
            }
            if (startBlockAddress != -1)
            {
                //** write to disk
                dataChunks.Add(new BulkWriter.DataChunk
                                   {
                                       TargetDataAddress = startBlockAddress,
                                       Index = currentTargetBufferIndex + startIndex,
                                       Size = bufferIndex - startIndex
                                   });
            }
            if (dataChunks.Count > 0)
                bulkWriter.Write(parent, _writeBuffer, dataChunks);

            return r;
        }

        public Sop.DataBlock ReadBlockFromDisk(Algorithm.Collection.ICollectionOnDisk parent, bool getForRemoval)
        {
            //var r = MruManager[parent.CurrentEntryDataAddress];
            //if (r == null)
            var r = CreateBlock(parent.DataBlockSize);
            return ReadBlockFromDisk(parent, parent.CurrentEntryDataAddress, getForRemoval, r);
        }

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
                    backedupData = new byte[readMetaInfoOnly ? Sop.DataBlock.OverheadSize : block.Length];
                    if (fileStream.Read(backedupData, 0, backedupData.Length) < backedupData.Length)
                        throw new SopException(string.Format("Expected to read {0} bytes but read {1} bytes instead.", backedupData.Length));
                    br.Close();
                }

                //** read data from byte array!
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

                        byte memberCount = br.ReadByte();

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
        /// Remove Block from the Collection
        /// </summary>
        public void Remove(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block, bool clearBlock = true)
        {
            if (!parent.File.Server.HasTrashBin)
                return;
            //** remove block and linked blocks from MRU
            Sop.DataBlock d = block;
            long contiguousBlockStartAddress = -1;
            long currentBlockAddress = -1;
            int contiguousBlockCount = 0;

            var deletedBlocks = ((CollectionOnDisk)parent).DeletedBlocks;
            IDictionary<long, Sop.DataBlock> blocks = ((CollectionOnDisk)parent).Blocks;

            do
            {
                if (d.DataAddress >= 0)
                {
                    blocks.Remove(d.DataAddress);
                }
                if (currentBlockAddress >= 0 && currentBlockAddress + (int)parent.DataBlockSize == d.DataAddress)
                {
                    //** if current block is part of a single contiguous block
                    contiguousBlockCount++;
                    currentBlockAddress = d.DataAddress;
                }
                else
                {
                    if (currentBlockAddress != -1)
                    {
                        //** recycle segment
                        if (deletedBlocks != null)
                            deletedBlocks.AddAvailableBlock(contiguousBlockStartAddress,
                                                            contiguousBlockCount * (int)parent.DataBlockSize);
                    }
                    currentBlockAddress = d.DataAddress;
                    contiguousBlockStartAddress = d.DataAddress;
                    contiguousBlockCount = 1;
                }

                //** clear block's data
                if (clearBlock && d.SizeOccupied > 0)
                    d.SizeOccupied = 0;

                if (d.Next == null && d.NextItemAddress >= 0)
                    //** load the next datablock to get its next block address...
                    d.Next = ReadBlockFromDisk(parent, parent.FileStream.Position, true, true,
                                               CreateBlock(parent.DataBlockSize));

                d = d.Next;
            } while (d != null);

            //** add to deleted blocks store any deleted block haven't added yet
            if (contiguousBlockStartAddress != -1)
            {
                if (deletedBlocks != null)
                    deletedBlocks.AddAvailableBlock(contiguousBlockStartAddress,
                                                    contiguousBlockCount * (int)parent.DataBlockSize);
            }
            parent.RegisterChange(true);
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
