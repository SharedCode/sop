// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using Sop.Persistence;

namespace Sop.OnDisk.DataBlock
{
    /// <summary>
    /// Deleted Block Info can contain two types of data:
    /// 1) Selective Delete blocks: Start Address and Count of deleted contiguous blocks
    /// 2) Entire Collection blocks: Start and End Blocks of the deleted collection
    /// </summary>
    internal class DeletedBlockInfo : IInternalPersistent, ICloneable
    {
        /// <summary>
        /// true if contiguous block (ie - Count contains number of reusable blocks),
        /// otherwise, contains linked blocks. EndBlockAddress contains last deleted block.
        /// </summary>
        public bool IsContiguousBlock
        {
            get { return EndBlockAddress < 0; }
        }

        public object Clone()
        {
            DeletedBlockInfo r = new DeletedBlockInfo();
            r.Count = Count;
            r.StartBlockAddress = StartBlockAddress;
            r.IsDirty = IsDirty;
            r.HintSizeOnDisk = HintSizeOnDisk;
            r.EndBlockAddress = EndBlockAddress;
            if (DiskBuffer != null)
                r.DiskBuffer = (Sop.DataBlock) DiskBuffer.Clone();
            return r;
        }

        /// <summary>
        /// 1.) Start Address of deleted contiguous blocks, OR
        /// 2.) Start block Address of deleted collection
        /// </summary>
        public long StartBlockAddress = -1;

        /// <summary>
        /// 1.) NA
        /// 2.) End Address of deleted collection
        /// </summary>
        public long EndBlockAddress = -1;

        /// <summary>
        /// 1.) Count of deleted Blocks
        /// 2.) NA
        /// </summary>
        public int Count = 0;

        /// <summary>
        /// Return the size on disk(in bytes) of this object
        /// </summary>
        public int HintSizeOnDisk { get; private set; }

        /// <summary>
        /// Serialize
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="writer"></param>
        public void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            writer.Write(StartBlockAddress);
            writer.Write(EndBlockAddress);
            writer.Write(Count);
        }

        /// <summary>
        /// DeSerialize
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        public void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            if (reader == null)
                throw new ArgumentNullException("reader");
            StartBlockAddress = reader.ReadInt64();
            EndBlockAddress = reader.ReadInt64();
            Count = reader.ReadInt32();
        }

        /// <summary>
        /// DiskBuffer of this DeletedBlockInfo
        /// </summary>
        public Sop.DataBlock DiskBuffer { get; set; }

        /// <summary>
        /// true means this DeletedBlockInfo was modified, otherwise false
        /// </summary>
        public bool IsDirty { get; set; }

        /// <summary>
        /// Return information contained in this DeletedBlockInfo. Information is useful
        /// for debugging purposes.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format(@"Start: {0}, End: {1}, Contiguous: {2}
                                   Count: {3}, HintSizeOnDisk: {4}, DiskBuffer.DataAddress: {5}",
                                 StartBlockAddress, EndBlockAddress, IsContiguousBlock,
                                 Count, HintSizeOnDisk, DiskBuffer != null ? DiskBuffer.DataAddress : -1);
        }
    }
}