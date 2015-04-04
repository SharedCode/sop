// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.DataBlock;
using Sop.Persistence;

namespace Sop.OnDisk.Algorithm.BTree
{
    /// <summary>
    /// B-Tree Item on Disk.
    /// </summary>
    internal class BTreeItemOnDisk : IInternalPersistent, ICloneable, IBTreeItemOnDisk
    {
        public BTreeItemOnDisk(){}
        public BTreeItemOnDisk(DataBlockSize dataBlockSize, object key, object value)
        {
            Key = key;
            Value = new ItemOnDisk(dataBlockSize, value);
        }

        /// <summary>
        /// Create a shallow copy of this object
        /// </summary>
        /// <returns></returns>
        public object Clone()
        {
            return new BTreeItemOnDisk((DataBlockSize) Value.DiskBuffer.Length, Key, Value);
        }

        /// <summary>
        /// IsDirty tells BTree whether this object needs to be rewritten to disk(dirty) or not
        /// </summary>
        public bool IsDirty
        {
            get
            {
                return _isDirty || Value.IsDirty ||
                       (ValueLoaded && Value.Data is IInternalPersistent &&
                        ((IInternalPersistent) Value.Data).IsDirty);
            }
            set { _isDirty = value; }
        }
        private bool _isDirty = true;

        /// <summary>
        /// Key
        /// </summary>
        public object Key;

        /// <summary>
        /// Version Number of the Item
        /// </summary>
        public long VersionNumber;

        ///// <summary>
        ///// For Value stored in Key Segment, this contains the
        ///// index of data block loaded in memory where Value is stored.
        ///// </summary>
        //public int ValueBlockIndex = -1;
        ///// <summary>
        ///// For Value stored in Key Segment, this contains the
        ///// byte index of where 1st byte of Value is stored within a Data Block.
        ///// </summary>
        //public int ValueIndexInBlock = -1;

        /// <summary>
        /// Value. When Value is updated, IsDirty is set to true.
        /// </summary>
        public ItemOnDisk Value;

        /// <summary>
        /// true means Value was loaded from Disk, false otherwise. Defaults to true.
        /// </summary>
        public bool ValueLoaded
        {
            get
            {
                return _valueLoaded && (Value == null ||
                                       (Value.Data != null || !Value.DataIsUserDefined));
            }
            set
            {
                _valueLoaded = value;
                if (!value) _isDirty = false;
            }
        }
        private bool _valueLoaded = true;

        /// <summary>
        /// Return the size on disk(in bytes) of this object
        /// </summary>
        public int HintSizeOnDisk { get; internal set; }

        /// <summary>
        /// Pack this Item for serialization to disk
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="writer"></param>
        public virtual void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            bool isReference = Value.DiskBuffer.DataAddress > -1;
            writer.Write(isReference);
            if (isReference)
                writer.Write(Value.DiskBuffer.DataAddress);
            else
                CollectionOnDisk.WritePersistentData(parent, Value, writer);
        }

        /// <summary>
        /// Unpack this item for DeSerialization from Stream
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        public virtual void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            bool? r = CollectionOnDisk.ReadPersistentData(parent, reader, ref Key);
            if (r == null)
            {
                if (((BTreeAlgorithm) parent).onKeyUnpack != null)
                    Key = ((BTreeAlgorithm) parent).onKeyUnpack(reader);
                else
                    throw new SopException("Can't Deserialize Custom persisted 'Key'.");
            }
            if (Value == null)
            {
                var f = (File.File) InternalPersistent.GetParent(parent, typeof (File.File));
                Value = new ItemOnDisk(f.DataBlockSize);
            }
            bool valueIsReference = reader.ReadBoolean();
            if (valueIsReference)
                Value.DiskBuffer.DataAddress = reader.ReadInt64();
            else
            {
                object o = Value;
                CollectionOnDisk.ReadPersistentData(parent, reader, ref o);
                Value = (ItemOnDisk) o;
                if (Value.DataIsUserDefined && ((BTreeAlgorithm) parent).onValueUnpack != null)
                    Value.Data = ((BTreeAlgorithm) parent).onValueUnpack(reader);
            }
            IsDirty = false;
        }

        Sop.DataBlock IInternalPersistent.DiskBuffer
        {
            get { return null; }
            set { }
        }
    }
}
