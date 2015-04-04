// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.IO;
using Sop.OnDisk.DataBlock;
using Sop.Persistence;

namespace Sop.OnDisk
{
    /// <summary>
    /// Header Data contains the Collection's Header information
    /// such as start, end blocks, next allocatable block, count
    /// of items...
    /// </summary>
    internal class HeaderData : InternalPersistent
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public HeaderData()
        {
        }

        /// <summary>
        /// Constructor expecting DataBlockSize
        /// </summary>
        /// <param name="dataBlockSize"></param>
        public HeaderData(DataBlockSize dataBlockSize)
        {
            this.diskBuffer = new Sop.DataBlock(dataBlockSize);
        }

        /// <summary>
        /// Recycled _region
        /// </summary>
        public DeletedBlockInfo RecycledSegment;

        public DeletedBlockInfo RecycledSegmentBeforeTransaction;

        /// <summary>
        /// Occupied Blocks Head
        /// </summary>
        public Sop.DataBlock OccupiedBlocksHead { get; set; }

        /// <summary>
        /// Occupied Blocks Tail
        /// </summary>
        public Sop.DataBlock OccupiedBlocksTail { get; set; }

        /// <summary>
        /// Clear the HeaderData member fields
        /// </summary>
        public void Clear()
        {
            StartAllocatableAddress = 0;
            NextAllocatableAddress = -1;
            EndAllocatableAddress = -1;
            OccupiedBlocksHead = null;
            OccupiedBlocksTail = null;
            Count = 0;
            if (diskBuffer != null)
                diskBuffer.Initialize();
        }

        /// <summary>
        /// Returns the count of items stored in the Collection
        /// </summary>
        public int Count { get; set; }

        internal bool IsModifiedInTransaction;

        /// <summary>
        /// Start of allocatable address
        /// </summary>
        internal long StartAllocatableAddress;

        /// <summary>
        /// Next allocatable address
        /// </summary>
        internal long NextAllocatableAddress = -1;

        /// <summary>
        /// End of allocatable address. When reached, a new block
        /// segment will be allocated by the Collection
        /// </summary>
        internal long EndAllocatableAddress;

        /// <summary>
        /// Next available Sop.DataBlock address on Disk.
        /// NOTE: this is for internal use of the Transaction managers
        /// to keep tab of the next available block address
        /// </summary>
        public long OnDiskNextAvailableBlockAddress = -1;

        /// <summary>
        /// On Disk Left over segment size.
        /// NOTE: for internal use only by the trans managers
        /// </summary>
        public int OnDiskLeftoverSegmentSize;

        /// <summary>
        /// Serialize
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="writer"></param>
        public override void Pack(IInternalPersistent parent, BinaryWriter writer)
        {
            System.IO.BinaryWriter binaryWriter = writer;
            binaryWriter.Write(DiskBuffer.DataAddress);
            binaryWriter.Write(Count);
            binaryWriter.Write(this.StartAllocatableAddress);
            binaryWriter.Write(this.EndAllocatableAddress);
            binaryWriter.Write(this.NextAllocatableAddress);
            long l = -1;
            if (this.OccupiedBlocksHead != null)
                l = this.OccupiedBlocksHead.DataAddress;
            binaryWriter.Write(l);
            l = -1;
            if (OccupiedBlocksTail != null)
                l = OccupiedBlocksTail.DataAddress;
            binaryWriter.Write(l);
            binaryWriter.Write(RecycledSegment != null);
            if (RecycledSegment != null)
                RecycledSegment.Pack(parent, writer);
        }

        /// <summary>
        /// DeSerialize
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        public override void Unpack(IInternalPersistent parent,
                                    BinaryReader reader)
        {
            System.IO.BinaryReader binaryReader = reader;
            long l = binaryReader.ReadInt64();
            if (l >= 0)
                DiskBuffer.DataAddress = l;
            int cnt = binaryReader.ReadInt32();
            long saa = binaryReader.ReadInt64();

            if ((Count > 0 && cnt == 0) ||
                StartAllocatableAddress > 0 && saa == 0)
            {
                binaryReader.ReadInt64();
                binaryReader.ReadInt64();
                binaryReader.ReadInt64();
                binaryReader.ReadInt64();
                if (reader.ReadBoolean())
                {
                    var rs = new DeletedBlockInfo();
                    rs.Unpack(parent, reader);
                }
                return;
            }
            Count = cnt; //BinaryReader.ReadInt32();
            StartAllocatableAddress = saa; // BinaryReader.ReadInt64();
            EndAllocatableAddress = binaryReader.ReadInt64();
            NextAllocatableAddress = binaryReader.ReadInt64();
            long obh = binaryReader.ReadInt64();
            long obt = binaryReader.ReadInt64();
            DataBlockSize dataBlockSize;
            if (parent != null)
            {
                File.File f = (File.File) InternalPersistent.GetParent(parent, typeof (File.File), true);
                dataBlockSize = f.DataBlockSize;
            }
            else
                dataBlockSize = (DataBlockSize) DiskBuffer.Length;
            if (obh >= 0)
            {
                OccupiedBlocksHead = new Sop.DataBlock(dataBlockSize);
                OccupiedBlocksHead.DataAddress = obh;
            }
            else if (OccupiedBlocksHead != null)
                OccupiedBlocksHead = null;
            if (obt >= 0)
            {
                OccupiedBlocksTail = new Sop.DataBlock(dataBlockSize);
                OccupiedBlocksTail.DataAddress = obt;
            }
            else if (OccupiedBlocksTail != null)
                OccupiedBlocksTail = null;

            if (reader.ReadBoolean())
            {
                RecycledSegment = new DeletedBlockInfo();
                RecycledSegment.Unpack(parent, reader);
                RecycledSegmentBeforeTransaction = (DeletedBlockInfo) RecycledSegment.Clone();
            }
        }

        /// <summary>
        /// HeaderData DiskBuffer override just set/get to disk buffer.
        /// </summary>
        public override Sop.DataBlock DiskBuffer
        {
            get { return diskBuffer; }
            set { diskBuffer = value; }
        }
    }
}