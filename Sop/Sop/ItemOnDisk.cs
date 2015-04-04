// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using Sop.OnDisk.Algorithm.Collection;
using Sop.Persistence;

namespace Sop
{
    /// <summary>
    /// Item On Disk.
    /// </summary>
    public class ItemOnDisk : InternalPersistent, ICloneable
    {
        #region Constructors
        /// <summary>
        /// Default Constructor
        /// </summary>
        public ItemOnDisk()
        {
        }
        /// <summary>
        /// Constructor expecting block size
        /// </summary>
        /// <param name="dataBlockSize"></param>
        public ItemOnDisk(DataBlockSize dataBlockSize) : this(dataBlockSize, null)
        {
        }
        /// <summary>
        /// Constructor expecting block size and Data to be persisted to disk
        /// </summary>
        /// <param name="dataBlockSize"></param>
        /// <param name="data"></param>
        public ItemOnDisk(DataBlockSize dataBlockSize, object data)
            : base(dataBlockSize)
        {
            Data = data;
        }

        #endregion

        /// <summary>
        /// Copy contents of this Item On Disk and returns the new copy.
        /// </summary>
        /// <returns></returns>
        public object Clone()
        {
            return new ItemOnDisk(DataBlockSize, Data)
                        {
                            DiskBuffer = diskBuffer != null ? (DataBlock)diskBuffer.Clone() : null,
                            IsDirty = IsDirty,
                            DataIsUserDefined = DataIsUserDefined,
                            HintSizeOnDisk = HintSizeOnDisk
                        };
        }

        /// <summary>
        /// true means this item on disk was modified since last load from disk,
        /// false otherwise.
        /// </summary>
        public override bool IsDirty
        {
            get
            {
                return base.IsDirty ||
                       (Data is IInternalPersistent &&
                        ((IInternalPersistent) Data).IsDirty);
            }
            set { base.IsDirty = value; }
        }

        /// <summary>
        /// Data to save/read from file
        /// </summary>
        internal object Data;

        /// <summary>
        /// For internal use only.
        /// true means data is user defined, false otherwise.
        /// </summary>
        internal bool DataIsUserDefined { get; set; }

        /// <summary>
        /// Write the contents of this object to the stream bound to a Binary Writer.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="writer"></param>
        public override void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            CollectionOnDisk.WritePersistentData(parent, Data, writer, Collections.BTree.ItemType.Value);
        }
        /// <summary>
        /// Unpack or read the contents of this object from a Binary Reader.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        public override void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            DataIsUserDefined = false;
            bool? r = CollectionOnDisk.ReadPersistentData(parent, reader, ref Data);
            if (r == null)
                DataIsUserDefined = true;
            else if (!r.Value)
                Data = null;
        }
        /// <summary>
        /// Override ToString to write the address on disk of this Item.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (DiskBuffer != null)
                return DiskBuffer.DataAddress.ToString();
            return "-1";
        }
    }
}
