// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.IO;
using Sop.OnDisk.Algorithm.Collection;
using FileStream = Sop.OnDisk.File.FileStream;

namespace Sop.OnDisk.DataBlock
{
    using Mru;

    /// <summary>
    /// DataBlockDriver is a data block driver implementation for 
    /// writing/reading data blocks to/from a local file.
    /// </summary>
    internal partial class DataBlockDriver : IDataBlockDriver
    {
        public DataBlockDriver()
        {
        }

        protected internal DataBlockDriver(Algorithm.Collection.ICollectionOnDisk parent, HeaderData hd = null)
        {
            Initialize(parent, hd);
        }

        public void Dispose()
        {
            if (BufferStream != null)
            {
                BufferStream.Dispose();
                BufferStream = null;
            }
            if (BinaryWriter != null)
            {
                BinaryWriter.Close();
                BinaryWriter = null;
            }
            BlockRecycler = null;
            _readAheadBuffer = null;
            _writeBuffer = null;
            //_logger = null;
            HeaderData = null;
        }

        internal void Initialize(Algorithm.Collection.ICollectionOnDisk parent, HeaderData hd)
        {
            HeaderData = hd;
            if (HeaderData != null) return;
            HeaderData = new HeaderData
            {
                DiskBuffer = CreateBlock(parent.DataBlockSize)
            };
        }

        /// <summary>
        /// Create a block of Data
        /// </summary>
        /// <param name="blockSize"></param>
        /// <returns></returns>
        public Sop.DataBlock CreateBlock(DataBlockSize blockSize)
        {
            Sop.DataBlock db = null;
            if (BlockRecycler != null)
                db = BlockRecycler.GetRecycledObject();
            if (db == null)
                db = new Sop.DataBlock(blockSize);
            return db;
        }

        /// <summary>
        /// Block Recycler.
        /// </summary>
        public Recycling.IRecycler<Sop.DataBlock> BlockRecycler { get; internal set; }

        public long GetId(Sop.DataBlock block)
        {
            if (block == null)
                return -1;
            return block.DataAddress;
        }

        public void SetId(Sop.DataBlock block, long value)
        {
            block.DataAddress = value;
        }

        public bool GetIsDirty(Sop.DataBlock block)
        {
            return block.IsDirty;
        }

        public void SetIsDirty(Sop.DataBlock targetBlock, bool newValue)
        {
            targetBlock.IsDirty = newValue;
        }

        public static long DefaultMruMaxCapacity = 50000;
        public static long DefaultMruMinCapacity = (long)Math.Ceiling(DefaultMruMaxCapacity * .75);

        /// <summary>
        /// MoveNext makes the next entry the current one
        /// </summary>
        public bool MoveNext(Algorithm.Collection.ICollectionOnDisk parent)
        {
            return MoveTo(parent, ((CollectionOnDisk)parent).GetCurrentDataBlock().Next);
        }

        /// <summary>
        /// MoveFirst makes the first entry in the Collection the current one
        /// </summary>
        public bool MoveFirst(Algorithm.Collection.ICollectionOnDisk parent)
        {
            return MoveTo(parent, this.HeaderData.OccupiedBlocksHead);
        }

        /// <summary>
        /// MoveLast makes the last entry in the Collection the current one
        /// </summary>
        public bool MoveLast(Algorithm.Collection.ICollectionOnDisk parent)
        {
            return MoveTo(parent, this.HeaderData.OccupiedBlocksTail);
        }

        /// <summary>
        /// Move parent Collection's file pointer to 'Address'.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="address"></param>
        /// <returns></returns>
        public bool MoveTo(Algorithm.Collection.ICollectionOnDisk parent, long address)
        {
            if (address >= 0)
            {
                FileStream fs = parent.FileStream;
                if (address != fs.Position)
                    fs.Seek(address, SeekOrigin.Begin);
                if (((CollectionOnDisk)parent).CurrentEntryDataAddress != address)
                {
                    if (((CollectionOnDisk)parent).currentDataBlock != null &&
                        ((CollectionOnDisk)parent).currentDataBlock.DataAddress > -1
                        )
                        ((CollectionOnDisk)parent).currentDataBlock = null;
                    ((CollectionOnDisk)parent).currentEntry = null;
                    ((CollectionOnDisk)parent).CurrentEntryDataAddress = address;
                }
                return true;
            }
            return false;
        }

        public bool MoveTo(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock dataBlock)
        {
            return dataBlock != null && MoveTo(parent, dataBlock.DataAddress);
        }

        private DeletedBlockInfo RecycleBlock(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block,
                                              bool isCollectionBlock, out int totalBlockSize, out bool fromCollection)
        {
            int blockLength = block.Length;
            totalBlockSize = block.CountMembers() * blockLength;
            var dbi = ((CollectionOnDisk)parent).GetDeletedBlock(totalBlockSize, isCollectionBlock, out fromCollection);
            if (dbi == null)
            {
                totalBlockSize = -1;
                return dbi;
            }
            if (!fromCollection)
            {
                return dbi;
            }
            if (dbi.IsContiguousBlock)
            {
                #region NOT VISITED BLOCK
                //** from Collection.DeletedBlocks (IsContiguousBlock = true)
                if (dbi.Count * (int)parent.DataBlockSize < totalBlockSize)
                {
                    string errMsg = "RecycleBlock: Total Requested Block Size > recycled Deleted Block(s).";
                    Log.Logger.Instance.Log(Log.LogLevels.Fatal, errMsg);
                    throw new InvalidOperationException(errMsg);
                }
                #endregion

                // Register recycled blocks so they can get handled properly, e.g. - will not get backed up during Store Flush.
                if (parent.Transaction != null)
                    ((Transaction.TransactionBase)parent.Transaction).Register(
                        Sop.Transaction.ActionType.Recycle,
                        (CollectionOnDisk)parent, dbi.StartBlockAddress, dbi.Count * blockLength);

                // set Block to the block addresses referenced in dbi
                int count = dbi.Count;
                long address = dbi.StartBlockAddress;
                Sop.DataBlock db = block;
                Transaction.ITransactionLogger trans = parent.Transaction;
                Sop.DataBlock dbPrev = null;
                while (db != null)
                {
                    dbi.Count--;
                    db.DataAddress = address;
                    if (dbPrev != null)
                    {
                        dbPrev.NextItemAddress = address;
                        dbPrev.Next = db;
                        SetIsDirty(dbPrev, true);
                    }
                    address += blockLength;
                    dbPrev = db;
                    db = db.Next;
                }
                SetIsDirty(dbPrev, true);

                var codParent = (CollectionOnDisk)parent;
                IDataBlockRecycler delBlocks = codParent.DeletedBlocks;

                if (HeaderData.RecycledSegment != dbi ||
                    HeaderData.RecycledSegment == null)
                {
                    if (delBlocks != null)
                        delBlocks.RemoveAvailableBlock(dbi.StartBlockAddress);
                }

                if (dbi.Count == 0)
                {
                    dbi.Count = count;
                    HeaderData.RecycledSegment = null;
                }
                else
                {
                    HeaderData.RecycledSegment = (DeletedBlockInfo)dbi.Clone();
                    HeaderData.RecycledSegment.StartBlockAddress = address;
                    dbi.Count = count;
                }
            }
            return dbi;
        }

        internal bool ResurfaceDeletedBlockNextSegment(CollectionOnDisk parent,
                                                       DeletedBlockInfo dbi, long segmentEnd)
        {
            //** read next segment of deleted collection
            Sop.DataBlock db = CreateBlock(parent.DataBlockSize);
            long address = segmentEnd - (int)parent.DataBlockSize;
            db = ReadBlockFromDisk(parent, address, true, true, db);
            if (db.InternalNextBlockAddress >= 0)
            {
                dbi.StartBlockAddress = db.InternalNextBlockAddress;
                if (parent.File.DeletedCollections != null)
                    parent.File.DeletedCollections.SetTop(dbi);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Allocate next available block on disk.
        /// Returns true if all blocks had been allocated, false otherwise.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="block"></param>
        /// <param name="isHead"></param>
        /// <param name="isCollectionBlock"></param>
        /// <param name="addToMru"> </param>
        /// <returns></returns>
        private bool AllocateNextBlock(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block,
                                       bool isHead, bool isCollectionBlock, bool addToMru)
        {
            bool isBlockLocallyRecycled = false;
            HeaderData hd = HeaderData;
            if (hd == null && parent.Parent is CollectionOnDisk)
                hd = ((CollectionOnDisk)parent.Parent).HeaderData;

            bool willRecycle = hd != null && hd.RecycledSegment != null && hd.RecycledSegment.Count > 0;
            if (!willRecycle && hd.NextAllocatableAddress + block.Length <= hd.EndAllocatableAddress)
                AllocateAvailableBlock((CollectionOnDisk)parent, hd, block);
            else
            {
                if (parent.File.Server.HasTrashBin && !willRecycle)
                {
                    if (isCollectionBlock)
                        willRecycle = parent.File.DeletedCollections != null &&
                                      parent.File.DeletedCollections.Count > 0;
                    //** prioritize block recycling if there are plenty of deleted blocks
                    if (!willRecycle && ((CollectionOnDisk)parent).DeletedBlocks != null)
                    {
                        willRecycle = ((CollectionOnDisk)parent).DeletedBlocks.Count > 0 ||
                                      parent.File.DeletedCollections.Count > 0;
                    }
                    if (!willRecycle && isHead)
                        willRecycle = parent.File.DeletedCollections != null &&
                                      parent.File.DeletedCollections.Count > 0;
                }

                DeletedBlockInfo dbi = null;

                if (willRecycle)
                {
                    int totalBlockSize;
                    bool fromCollection;
                    if (isCollectionBlock)
                    {
                        if (parent.File.DeletedCollections != null && parent.File.DeletedCollections.Count > 0)
                            dbi = RecycleBlock(parent, block, true, out totalBlockSize, out fromCollection);
                    }
                    if (
                        dbi != null ||
                        ((((CollectionOnDisk)parent).DeletedBlocksCount) > 0 &&
                          (dbi =
                           RecycleBlock(parent, block, isCollectionBlock, out totalBlockSize, out fromCollection)) !=
                          null)
                        )
                    {
                        #region Recycle Deleted Block
                        if (dbi.IsContiguousBlock)
                        {
                            #region Contiguous blocks can only be from Collection.DeletedBlocks
                            if (block.DataAddress == -1)
                            {
                                long address = dbi.StartBlockAddress;
                                Sop.DataBlock db = block;
                                int blockLength = (int)parent.DataBlockSize;
                                while (db != null)
                                {
                                    dbi.Count--;
                                    db.DataAddress = address;
                                    address += blockLength;
                                    db = db.Next;
                                }
                            }
                            isBlockLocallyRecycled = true;
                            #endregion
                        }
                        else
                        {
                            #region Recycled block is from a deleted collection of the File
                            int growthSizeInNob = parent.File.StoreGrowthSizeInNob;
                            block.DataAddress = dbi.StartBlockAddress;
                            hd.StartAllocatableAddress = dbi.StartBlockAddress;
                            hd.EndAllocatableAddress = hd.StartAllocatableAddress +
                                                       (short)parent.File.DataBlockSize * growthSizeInNob;

                            Log.Logger.Instance.Log(Log.LogLevels.Verbose,
                                "Recycled region {0}, Size {1} from {2} DeletedCollections",
                                dbi.StartBlockAddress, hd.EndAllocatableAddress - hd.StartAllocatableAddress,
                                parent.File.Filename);

                            if (parent.Transaction != null)
                                ((Transaction.TransactionBase)parent.Transaction).Register(
                                    Sop.Transaction.ActionType.RecycleCollection,
                                    (CollectionOnDisk)parent, dbi.StartBlockAddress,
                                    hd.EndAllocatableAddress - hd.StartAllocatableAddress);

                            if (dbi.EndBlockAddress == hd.EndAllocatableAddress)
                            {
                                if (parent.File.DeletedCollections != null)
                                    parent.File.DeletedCollections.RemoveTop();
                            }
                            else
                            {
                                //** read next segment of deleted collection
                                if (
                                    !ResurfaceDeletedBlockNextSegment((CollectionOnDisk)parent, dbi,
                                                                      hd.EndAllocatableAddress))
                                {
                                    if (parent.File.DeletedCollections != null)
                                        parent.File.DeletedCollections.Remove(dbi.StartBlockAddress);
                                }
                            }
                            hd.NextAllocatableAddress = hd.StartAllocatableAddress + block.Length;
                            hd.DiskBuffer.IsDirty = true;
                            hd.IsModifiedInTransaction = true;
                            #endregion
                        }
                        #endregion
                    }
                }
                if (dbi == null)
                {
                    if (hd.NextAllocatableAddress + block.Length <= hd.EndAllocatableAddress)
                        AllocateAvailableBlock((CollectionOnDisk)parent, hd, block);
                    else
                        AllocateOnNextSegment((CollectionOnDisk)parent, hd, block);
                }
            }

            #region Add Block to MRU

            if (hd.OccupiedBlocksHead == null)
            {
                hd.OccupiedBlocksHead = CreateBlock((DataBlockSize)block.Length);
                hd.OccupiedBlocksHead.DataAddress = block.DataAddress;
                if (hd.OccupiedBlocksTail == null)
                    hd.OccupiedBlocksTail = CreateBlock((DataBlockSize)block.Length);
            }
            else if (isBlockLocallyRecycled)
            {
                //** reload & update the InternalNextBlockAddress to keep ("low-level") segments' link intact...
                Sop.DataBlock db = block;
                while (db != null)
                {
                    if ((db.DataAddress + (int)parent.DataBlockSize) %
                        (parent.File.Profile.StoreGrowthSizeInNob * (int)parent.DataBlockSize) == 0)
                    {
                        Sop.DataBlock db2 = CreateBlock(parent.DataBlockSize);
                        db2 = ReadBlockFromDisk(parent, db.DataAddress, true, true, db2);
                        db.InternalNextBlockAddress = db2.InternalNextBlockAddress;
                    }
                    db = db.Next;
                }
                //** add to MRU cache if requested...



                return true;
            }
            else
            {
                if ((hd.OccupiedBlocksTail.DataAddress + (int)parent.DataBlockSize) %
                    (parent.File.Profile.StoreGrowthSizeInNob * (int)parent.DataBlockSize) == 0)
                {
                    Sop.DataBlock db = ReadBlockFromDisk(parent, hd.OccupiedBlocksTail.DataAddress, false);
                    db.InternalNextBlockAddress = block.DataAddress;
                    this.SetDiskAddress(parent, db, addToMru);
                    long dbId = GetId(db);
                    ((CollectionOnDisk)parent).Blocks[dbId] = db;
                }
            }
            hd.OccupiedBlocksTail.DataAddress = block.DataAddress;

            #endregion

            return false;
        }

        private void AllocateAvailableBlock(CollectionOnDisk parent, HeaderData hd, Sop.DataBlock block)
        {
            //** Allocate block from the "available" segment
            block.DataAddress = hd.NextAllocatableAddress;
            hd.NextAllocatableAddress += block.Length;
            hd.DiskBuffer.IsDirty = true;
            hd.IsModifiedInTransaction = true;
            //** register the add of new block to transaction log
            Transaction.ITransactionLogger trans = parent.Transaction;
            if (trans == null) return;
            ((Transaction.TransactionBase)trans).RegisterAdd(parent, block.DataAddress, block.Length);
        }

        private void AllocateOnNextSegment(CollectionOnDisk parent, HeaderData hd, Sop.DataBlock block)
        {
            long g;
            long segmentSize = parent.Grow(out g);
            if (segmentSize > 0)
            {
                Transaction.ITransactionLogger trans = parent.Transaction;
                if (trans != null)
                {
                    ((Transaction.TransactionBase)trans).RegisterFileGrowth(parent, g, segmentSize);
                }
            }
            else
            {
                string s = string.Format("File '{0}' failed to grow.", parent.File.Filename);
                //_logger.LogLine(s);
                throw new InvalidOperationException(s);
            }
            hd.OnDiskLeftoverSegmentSize = (int)(hd.EndAllocatableAddress - hd.NextAllocatableAddress);
            hd.StartAllocatableAddress = g;
            hd.EndAllocatableAddress = hd.StartAllocatableAddress + segmentSize;
            hd.NextAllocatableAddress = hd.StartAllocatableAddress;

            block.DataAddress = hd.NextAllocatableAddress;
            hd.NextAllocatableAddress += block.Length;
            hd.DiskBuffer.IsDirty = true;
            hd.IsModifiedInTransaction = true;
        }
        public HeaderData HeaderData { get; set; }
    }
}