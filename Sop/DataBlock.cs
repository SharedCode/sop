
// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)


using System;
using System.Collections.Generic;
using Sop.OnDisk.Algorithm.BTree;

namespace Sop
{
    /// <summary>
    /// DataBlock represents the smallest unit of data structure stored on disk.
    /// Each data block is linked to another data block within the same File
    /// forming a huge chain of structures on disk. Yes, a File in its simplest form,
    /// is mainly a series of linked data blocks in SOP. These structures may contain one or many
    /// Dictionaries on Disk (a.k.a. - data store).
    /// 
    /// DataBlock is used in SOP for POCO persistence. i.e. - each POCO, internal or user defined,
    /// is serialized and data stored in a set of DataBlock objects which are then used by 
    /// file stream writers/readers in actual write/read of data to/from file on disk.
    /// 
    /// Layout of a single data block on Disk:
    /// Byte 0 to 7: Logical level Next Item Address (64 bit long int)
    /// Byte 8 to 11: Size Occupied (32 bit)
    /// Byte 12 to 19: Physical or Low level next item address. (64 bit)
    /// Byt2 20: Count of member blocks if block is head.
    /// Byte 21 to Size Occupied: USER DATA
    /// </summary>
    public class DataBlock : ICloneable
    {
        /// <summary>
        /// DataBlock information used in Block Folding function.
        /// </summary>
        public class Info
        {
            public long Address;
            public int BlockCount;
        }
        /// <summary>
        /// Constructor expecting DataBlockSize.
        /// </summary>
        /// <param name="dataBlockSize"></param>
        public DataBlock(DataBlockSize dataBlockSize, List<Info> metaBlocks = null)
        {
            if ((int)dataBlockSize > 0)
                Data = new byte[(int)dataBlockSize - OverheadSize];
            if (metaBlocks != null)
            {
                foldedDataAddresses = metaBlocks.ToArray();
                contiguousBlockCount = metaBlocks[0].BlockCount;
            }
        }

        /// <summary>
        /// true if this block is the head block, false otherwise.
        /// </summary>
        public bool IsHead { get; set; }

        /// <summary>
        /// true means this is candidate for recycling as there is no
        /// container that has reference on this block, false otherwise.
        /// </summary>
        internal bool Orphaned { get; set; }

        /// <summary>
        /// Check if block is empty
        /// </summary>
        /// <returns></returns>
        public bool IsEmpty()
        {
            return SizeOccupied == 0 && NextItemAddress <= 0 && InternalNextBlockAddress <= 0;
        }

        #region Block Folding/Unfolding
        internal void RemoveFromCache(BTreeAlgorithm container)
        {
            if (IsFolded)
            {
                foreach (var info in foldedDataAddresses)
                {
                    var addresses = Convert(info);
                    int i = 0;
                    while (i < addresses.Length)
                    {
                        container.Blocks.Remove(addresses[i++]);
                    }
                }
                return;
            }
            var db = this;
            while (db != null)
            {
                long ID = container.GetId(db);
                if (ID < 0)
                    break;
                container.Blocks.Remove(ID);
                db = db.Next;
            }
        }

        internal bool IsFolded
        {
            get
            {
                return foldedDataAddresses != null;
            }
        }
        /// <summary>
        /// Fold will deallocate this block chain and keep only
        /// the DataAddresses for use in Unfold to recreate the chain.
        /// </summary>
        internal void Fold()
        {
            if (Next == null || NextItemAddress == -1) return;
            List<Info> addresses = new List<Info>();
            ProcessHeadSets(addresses, true);
            foldedDataAddresses = addresses.ToArray();
            if (foldedDataAddresses.Length > 2)
            {
                Log.Logger.Instance.Verbose("DataBlock with Folded Addresses count of {0} was detected.", foldedDataAddresses.Length);
            }
        }
        internal Info[] foldedDataAddresses;
        private int foldedMemberCount;

        /// <summary>
        /// Returns true if Folded info is available on disk, false otherwise.
        /// false means this block is standalone block and there is no need
        /// to read the folded info from disk.
        /// </summary>
        internal bool IsFoldedInfoOnDisk
        {
            get
            {
                return SizeAvailable == 0 && (foldedMemberCount > 1 || NextItemAddress >= 0);
            }
        }

        /// <summary>
        /// Unfold will recreate this block chain.
        /// </summary>
        /// <param name="container"></param>
        internal void Unfold(BTreeAlgorithm container)
        {
            if (foldedDataAddresses == null) return;
            var db = container.Blocks[NextItemAddress];
            if (db != null)
            {
                db.ClearData();
                var prev2 = this;
                foreach (var info in foldedDataAddresses)
                {
                    var addresses = Convert(info);
                    int i2 = 0;
                    if (DataAddress == addresses[0])
                        i2++;
                    while (i2 < addresses.Length)
                    {
                        var _na = addresses[i2];
                        db = container.Blocks[_na];
                        if (db == null)
                        {
                            db = new DataBlock((DataBlockSize)Length);
                            db.DataAddress = _na;
                        }
                        else
                            db.ClearData();
                        prev2.Next = db;
                        prev2.NextItemAddress = db.DataAddress;
                        prev2 = db;
                        i2++;
                    }
                }
                foldedDataAddresses = null;
                return;
            }
            // recreate the buffers if not found in Blocks cache...
            var prev = this;
            foreach (var info in foldedDataAddresses)
            {
                var addresses = Convert(info);
                int i = 0;
                if (DataAddress == addresses[0])
                    i++;
                while (i < addresses.Length)
                {
                    var _na = addresses[i];
                    db = new DataBlock((DataBlockSize)Length);
                    db.DataAddress = _na;
                    prev.Next = db;
                    prev.NextItemAddress = db.DataAddress;
                    prev = db;
                    i++;
                }
            }
            foldedDataAddresses = null;
        }
        private long[] Convert(Info foldedBlocks)
        {
            var count = foldedBlocks.BlockCount;
            if (count == 0)
                count++;
            var blockLength = Length;
            long[] r = new long[count];
            r[0] = foldedBlocks.Address;
            for (int i = 1; i < r.Length; i++)
            {
                r[i] = r[i - 1] + blockLength;
            }
            return r;
        }
        #endregion

        #region for removal
        ///// <summary>
        ///// Returns index of a given block in relative to this block instance.
        ///// </summary>
        ///// <param name="block"></param>
        ///// <returns></returns>
        //public int GetIndexOf(DataBlock block)
        //{
        //    DataBlock db = this;
        //    int i = 0;
        //    while (db != null)
        //    {
        //        if (db == block) return i;
        //        db = db.Next;
        //        i++;
        //    }
        //    return -1;
        //}
        ///// <summary>
        ///// Returns an Enumerated DataBlockSize value given its int equivalent
        ///// </summary>
        ///// <param name="sizeOccupied"></param>
        ///// <returns></returns>
        //public static DataBlockSize GetEnumeratedSize(int sizeOccupied)
        //{
        //    int i;
        //    for (i = (int) DataBlockSize.Minimum; i <= (int) DataBlockSize.Maximum; i <<= 1)
        //    {
        //        if (i >= sizeOccupied) break;
        //    }
        //    return (DataBlockSize) i;
        //}
        ///// <summary>
        ///// true means block set occupies contiguous space on disk, otherwise false
        ///// </summary>
        ///// <returns></returns>
        //public bool IsContiguous()
        //{
        //    DataBlock db = this;
        //    while (db != null)
        //    {
        //        if (db.NextItemAddress >= 0 && db.DataAddress + Length != db.NextItemAddress)
        //            return false;
        //        db = db.Next;
        //    }
        //    return true;
        //}
        #endregion

        /// <summary>
        /// Is DataAddress one of this Sop.DataBlock's Blocks
        /// </summary>
        /// <param name="dataAddress"></param>
        /// <returns></returns>
        public bool IsBlockOfThis(long dataAddress)
        {
            DataBlock db = this;
            while (db != null)
            {
                if (db.DataAddress == dataAddress || NextItemAddress == dataAddress)
                    return true;
                db = db.Next;
            }
            return false;
        }

        /// <summary>
        /// Set the data block size and create the Data byte array at same time
        /// </summary>
        public DataBlockSize DataBlockSize
        {
            set
            {
                int ns = (int)value - OverheadSize;
                if (Data == null || Data.Length != ns)
                    Data = new byte[ns];
            }
        }

        /// <summary>
        /// Size Occupied
        /// </summary>
        public int SizeOccupied { get; set; }

        /// <summary>
        /// Offset on Disk where start byte of Data is stored.
        /// NOTE: Internal use only.
        /// </summary>
        public long DataAddress
        {
            get { return _dataAddress; }
            set { _dataAddress = value; }
        }
        private long _dataAddress = -1;


        #region payload support (for removal)
        ///// <summary>
        ///// true if next block is a payload type or false if not
        ///// </summary>
        ///// <returns></returns>
        //public bool IsNextPayload()
        //{
        //    return PassThroughBlock != null;
        //}
        ///// <summary>
        ///// Returns the last block in the chain
        ///// </summary>
        ///// <param name="db"></param>
        ///// <returns></returns>
        //public static DataBlock GetLast(DataBlock db)
        //{
        //    if (db == null)
        //        return null;
        //    DataBlock r = db;
        //    while (r.Next != null)
        //        r = r.Next;
        //    return r;
        //}
        ///// <summary>
        ///// Is block extended(true) since its last persistence to disk/virtual store or not(false)
        ///// </summary>
        ///// <returns></returns>
        //public virtual bool WasExtended()
        //{
        //    DataBlock db = this;
        //    while (db != null)
        //    {
        //        if (db.DataAddress < 0)
        //            return true;
        //        db = db.Next;
        //    }
        //    return false;
        //}
        ///// <summary>
        ///// Append byte array to the end of the block's data array,
        ///// extending the chain to more block(s) if needed
        ///// </summary>
        ///// <param name="value"></param>
        //public void AppendValue(byte[] value)
        //{
        //    AppendValue(value, 0);
        //}
        ///// <summary>
        ///// Append byte array starting from a given Index
        ///// </summary>
        ///// <param name="value"></param>
        ///// <param name="startIndex"></param>
        //public void AppendValue(byte[] value, int startIndex)
        //{
        //    if (value.Length < this.SizeAvailable)
        //    {
        //        long sz = SizeOccupied;
        //        int length = value.Length - startIndex;
        //        Array.Copy(value, startIndex, this.Data, sz, length);
        //        SizeOccupied += length;
        //    }
        //    else
        //        throw new ArgumentOutOfRangeException("value", value,
        //                                              "Available size is smaller than needed space to store Value.");
        //}
        //public void SetIsDirty(bool dirty)
        //{
        //    Sop.DataBlock db = this;
        //    while (db != null)
        //    {
        //        db.IsDirty = dirty;
        //        db = db.Next;
        //    }
        //}

        ///// <summary>
        ///// PassThroughBlock
        ///// </summary>
        //public DataBlock PassThroughBlock;
        #endregion

        /// <summary>
        /// Low-level Next block address, for internal use.
        /// </summary>
        public long InternalNextBlockAddress
        {
            get { return _internalNextBlockAddress; }
            set { _internalNextBlockAddress = value; }
        }
        private long _internalNextBlockAddress = -1;

        /// <summary>
        /// NextBlockAddress is used to link Blocks in order to accomodate Data bigger than
        /// what a Block can store (e.g., Data > 512 bytes).
        /// NOTE: Internal use only.
        /// </summary>
        public long NextItemAddress
        {
            get { return _nextItemAddress; }
            set { _nextItemAddress = value; }
        }
        private long _nextItemAddress = -1;

        /// <summary>
        /// Data to be persisted/read to/from store
        /// </summary>
        public byte[] Data { get; set; }

        /// <summary>
        /// Data is modified(true) or not(false).
        /// </summary>
        public bool IsDirty
        {
            get { return _isDirty; }
            set { _isDirty = value; }
        }
        private bool _isDirty = true;

        /// <summary>
        /// Returns next block.
        /// NOTE: Internal use only.
        /// </summary>
        public DataBlock Next { get; set; }

        /// <summary>
        /// Clear the data contents without resetting the data address. This is useful
        /// for marking blocks of data on disk as cleared. Call "ClearData" then
        /// save the block to disk to set to 0 each data byte of the block(s) on disk.
        /// </summary>
        public void ClearData(int size = -1)
        {
            if (SizeOccupied == 0)
                return;

            if (size == -1)
            {
                //** clear entire chain of blocks
                DataBlock d = this;
                do
                {
                    if (d.SizeOccupied > 0)
                    {
                        d.SizeOccupied = 0;
                        if (d.Data != null)
                            Array.Clear(d.Data, 0, d.Data.Length);
                    }
                    //if (d.IsNextPayload())
                    //    break;
                    d = d.Next;
                } while (d != null);
            }
            else if (size > 0)
            {
                //** clear Data from current and linked block(s) until bytes cleared = Size
                DataBlock d = this;
                int size2 = size;
                do
                {
                    if (d.Data.Length < size2)
                    {
                        Array.Clear(d.Data, 0, d.Data.Length);
                        size2 -= d.Data.Length;
                    }
                    else
                    {
                        Array.Clear(d.Data, 0, size2);
                        break;
                    }
                    //if (d.IsNextPayload())
                    //    break;
                    d = d.Next;
                } while (d != null && size2 > 0);
            }
        }

        /// <summary>
        /// Override of ToString to write the DataBlock's
        /// address (file offset) on disk.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return DataAddress.ToString();
        }

        /// <summary>
        /// Initialize this Data Block to its initial state
        /// </summary>
        public void Initialize()
        {
            contiguousBlockCount = 0;
            foldedDataAddresses = null;
            foldedMemberCount = 0;
            DataAddress = -1;
            NextItemAddress = -1;
            InternalNextBlockAddress = -1;
            IsDirty = true;
            IsHead = false;
            Orphaned = false;
            if (Data != null)
                Array.Clear(Data, 0, Data.Length);
            SizeOccupied = 0;
            if (Next != null)
                Next.Initialize();
        }

        /// <summary>
        /// Returns the Block's Size in bytes
        /// </summary>
        public int Length
        {
            get { return OverheadSize + Data.Length; }
        }

        /// <summary>
        /// returns how much data in bytes can still be appended to this datablock
        /// </summary>
        public int SizeAvailable
        {
            get
            {
                if (Data != null)
                    return Data.Length - SizeOccupied;
                return 0;
            }
        }

        /// <summary>
        /// Returns the number of bytes of overhead (non-user data or bytes used for mngmt) of a Block
        /// </summary>
        public const int OverheadSize = sizeof(long) * 2 + sizeof(int) + sizeof(ushort);

        /// <summary>
        /// Create Sop.DataBlock
        /// </summary>
        /// <returns></returns>
        protected internal virtual DataBlock Create()
        {
            return new DataBlock((DataBlockSize)Length);
        }

        /// <summary>
        /// Copy this object to Destination
        /// </summary>
        /// <param name="destination"></param>
        public virtual void Copy(DataBlock destination)
        {
            if (this != destination)
            {
                this.Data.CopyTo(destination.Data, 0);
                destination.IsDirty = IsDirty;
                destination.DataAddress = this.DataAddress;
                destination.NextItemAddress = this.NextItemAddress;
                destination.SizeOccupied = this.SizeOccupied;
                destination.InternalNextBlockAddress = this.InternalNextBlockAddress;
                //if (IsNextPayload())
                //    destination.Next = destination.PassThroughBlock = PassThroughBlock; //.Clone();
                //else
                //{
                if (NextItemAddress >= 0)
                {
                    if (Next != null)
                    {
                        if (destination.Next == null)
                            destination.Next = Create();
                        Next.Copy(destination.Next);
                    }
                }
                else
                {
                    if (destination.Next != null)
                    {
                        if (Next != null)
                            Next.Copy(destination.Next);
                    }
                    if (Next != null)
                    {
                        destination.Next = Create();
                        Next.Copy(destination.Next);
                    }
                }
                //}
            }
        }

        /// <summary>
        /// Clone this datablock
        /// </summary>
        /// <returns></returns>
        public object Clone()
        {
            DataBlock d = Create();
            Copy(d);
            return d;
        }

        /// <summary>
        /// Extend the block
        /// </summary>
        /// <returns></returns>
        public virtual DataBlock Extend()
        {
            if (Next == null)
                Next = Create();
            //else if (IsNextPayload())
            //{
            //    Sop.DataBlock n = Create();
            //    n.Next = n.PassThroughBlock = PassThroughBlock;
            //    n.NextItemAddress = NextItemAddress;
            //    PassThroughBlock = null;
            //    Next = n;
            //    NextItemAddress = -1;
            //}
            else
            {
                if (Next.SizeAvailable == 0)
                    return Next.Extend();
            }
            return Next;
        }

        /// <summary>
        /// Get Total Size occupied by the chained Sop.DataBlock.
        /// </summary>
        /// <returns></returns>
        public int GetSizeOccupied(int offset = 0)
        {
            int totalSize = 0;
            Sop.DataBlock h = this;
            do
            {
                if (h.Data != null && h.SizeOccupied > 0)
                {
                    totalSize += h.SizeOccupied;
                    if (h == this && offset > 0)
                        totalSize -= offset;
                    h = h.Next;
                }
                else
                    break;
            } while (h != null);
            return totalSize;
        }

        /// <summary>
        /// Retrieves a DataBlock from the chain.
        /// </summary>
        /// <param name="chainIndex">index or chain sequence # of the block to get from the chain.</param>
        /// <returns></returns>
        public DataBlock GetBlock(int chainIndex)
        {
            DataBlock db = this;
            int i;
            for (i = 0; i < chainIndex && db.Next != null; i++)
            {
                db = db.Next;
            }
            if (i == chainIndex)
                return db;
            return null;
        }

        /// <summary>
        /// Returns the entire blocks' data
        /// </summary>
        /// <returns></returns>
        public byte[] GetData()
        {
            int totalSize = GetSizeOccupied();
            byte[] bytes = new byte[totalSize];
            int currentPosition = 0;
            DataBlock h = this;
            do
            {
                if (h.Data != null && h.SizeOccupied > 0)
                {
                    Array.Copy(h.Data, 0, bytes, currentPosition, h.SizeOccupied);
                    currentPosition += h.SizeOccupied;
                    h = h.Next;
                }
                else
                    break;
            } while (h != null);
            return bytes;
        }

        /// <summary>
        /// Count number of in-memory members of the linked blocks.
        /// </summary>
        /// <param name="contiguousOnly">true will count contiguous blocks only, otherwise will count all member blocks.</param>
        /// <returns></returns>
        public int CountMembers(bool contiguousOnly = false)
        {
            if (contiguousOnly && contiguousBlockCount > 0)
                return contiguousBlockCount;
            if (IsFolded)
            {
                if (!contiguousOnly && foldedMemberCount > 0)
                    return foldedMemberCount;

                int ctr = 0;
                foreach (var a in foldedDataAddresses)
                {
                    ctr += a.BlockCount;
                    if (contiguousOnly)
                    {
                        contiguousBlockCount = ctr;
                        break;
                    }
                }
                if (!contiguousOnly)
                    foldedMemberCount = ctr;
                return ctr;
            }
            DataBlock d = this;
            int c = 0;
            while (d != null)
            {
                c++;
                if (contiguousOnly)
                {
                    if (d.NextItemAddress >= 0 && d.DataAddress + Length != d.NextItemAddress)
                        return c;
                }
                d = d.Next;
            }
            return c;
        }

        /// <summary>
        /// Maximum number of DataBlock members in a chain.
        /// This value dictates how many 512 data block size can be grouped together
        /// for the bulk data read/write I/O operations.
        /// </summary>
        internal const ushort MaxChainMemberCount = ushort.MaxValue;
        //(ushort)((double)ushort.MaxValue * .75);

        /// <summary>
        /// Mark block as Head (IsHead=true) for each contiguous chain
        /// of 255 blocks. This allows mapping to disk to get optimized
        /// allowing bulk write/read of entire 255 chained block contents
        /// in one Read/Write IO async operation.
        /// </summary>
        internal int ProcessHeadSets(List<Info> target = null, bool unlink = false)
        {
            IsHead = true;
            contiguousBlockCount = 1;
            var d = this;
            DataBlock headBlock = this;
            int c = 0;
            int ctr = 0;
            while (d != null)
            {
                ctr++;
                if (c > 0)
                {
                    d.IsHead = false;
                    headBlock.contiguousBlockCount++;
                }
                c++;
                if (d.NextItemAddress >= 0 && d.DataAddress + Length == d.NextItemAddress)
                {
                    // next block is contiguous
                    if (c == MaxChainMemberCount)
                    {
                        if (d.Next != null)
                        {
                            d.Next.IsHead = true;
                            d.Next.contiguousBlockCount = 1;
                            AddAddess(target, headBlock);
                            headBlock = d.Next;
                        }
                        c = 0;
                    }
                }
                else
                {
                    // process non-contiguous next block
                    if (d.Next != null)
                    {
                        if (c == 1)
                            d.IsHead = false;
                        d.Next.IsHead = true;
                        d.Next.contiguousBlockCount = 1;
                        AddAddess(target, headBlock);
                        headBlock = d.Next;
                    }
                    c = 0;
                }
                var prev = d;
                d = d.Next;
                if (unlink)
                {
                    prev.Next = null;
                }
            }
            AddAddess(target, headBlock);
            foldedMemberCount = ctr;
            return ctr;
        }
        private void AddAddess(List<Info> target, DataBlock headBlock)
        {
            if (target != null)
            {
                target.Add(new Info { Address = headBlock._dataAddress, BlockCount = headBlock.contiguousBlockCount });
            }
        }
        internal int contiguousBlockCount;
    }
}
