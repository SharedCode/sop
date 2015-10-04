// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.File;

namespace Sop.Persistence
{
    /// <summary>
    /// For internal use only. InternalPersistent Object.
    /// </summary>
    public abstract class InternalPersistent : IInternalPersistent
    {
        protected InternalPersistent()
        {
        }

        /// <summary>
        /// Initialize InternalPersistent Object's DiskBuffer.
        /// </summary>
        /// <param name="dataBlockSize">Data Block of 'DataBlockSize' will be created and assigned as DiskBuffer</param>
        protected InternalPersistent(DataBlockSize dataBlockSize)
        {
            this.DataBlockSize = dataBlockSize;
            //this.DiskBuffer = new Sop.DataBlock(DataBlockSize);
        }

        public override string ToString()
        {
            if (diskBuffer != null)
                return diskBuffer.ToString();
            return "-1";
        }

        /// <summary>
        /// Implement to Pack Persisted Data on a byte array for Serialization.
        /// </summary>
        /// <param name="parent">Collection this object is a member of</param>
        /// <param name="writer">Pack your data using this Writer</param>
        public abstract void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer);

        /// <summary>
        /// Given byte array read from stream, read the bytes needed to de-serialize this object by assigning
        /// to appropriate fields of this object the read data.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        /// <returns>0 if object completely Unpacked or the size of the Data waiting to be read on the stream.</returns>
        public abstract void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader);

        /// <summary>
        /// Traverse the Parent hierarchy and look for a Parent of a given Type.
        /// Example, one can look for the "File" container of a Collection or a Parent
        /// Collection of a Collection and so on and so forth..
        /// </summary>
        /// <param name="parent"> </param>
        /// <param name="findParentOfType"> </param>
        /// <param name="throwIfNotFound"> </param>
        /// <returns></returns>
        public static IInternalPersistent GetParent(IInternalPersistent parent, Type findParentOfType, bool throwIfNotFound = false)
        {
            if (findParentOfType == null)
                throw new ArgumentNullException("findParentOfType");
            if (parent != null)
            {
                Type t = parent.GetType();
                if (t == findParentOfType || t.IsSubclassOf(findParentOfType))
                    return parent;
                if (findParentOfType == typeof (File))
                {
                    if (parent is OnDisk.Algorithm.Collection.ICollectionOnDisk)
                        return ((OnDisk.Algorithm.Collection.ICollectionOnDisk) parent).File;
                }
                if (parent is OnDisk.Algorithm.Collection.ICollectionOnDisk)
                    return ((OnDisk.Algorithm.Collection.ICollectionOnDisk) parent).GetParent(findParentOfType);
                if (parent is File)
                    return ((File) parent).GetParent(findParentOfType);
            }
            if (throwIfNotFound)
                throw new ArgumentException(string.Format("Parent of type '{0}' not found.", findParentOfType.ToString()));
            return null;
        }

        internal DataBlock diskBuffer;

        /// <summary>
        /// Default implementation is to retrieve the disk buffer from MRU,
        /// override if needed to get data from Disk if not found in MRU.
        /// </summary>
        public virtual DataBlock DiskBuffer
        {
            get
            {
                Sop.DataBlock d = null;
                if (this is OnDisk.IInternalPersistentRef)
                {
                    var This = (OnDisk.IInternalPersistentRef) this;
                    if (This.DataAddress > -1 && This.MruManager != null)
                    {
                        d = (DataBlock) This.MruManager[This.DataAddress];
                        if (d == null)
                        {
                            if (This.MruManager.GetParent() != null)
                                d = ((CollectionOnDisk)This.MruManager.GetParent()).
                                    DataBlockDriver.
                                    CreateBlock(DataBlockSize);
                            else
                                d = new Sop.DataBlock(DataBlockSize);
                            d.DataAddress = This.DataAddress;
                            This.MruManager[This.DataAddress] = d;
                        }
                    }
                }
                else
                {
                    d = diskBuffer;
                    if (d == null && DataBlockSize > 0)
                    {
                        d = new Sop.DataBlock(DataBlockSize);
                        diskBuffer = d;
                    }
                }
                return d;
            }
            set
            {
                if (value == null)
                    throw new ArgumentNullException("value");
                if (this is OnDisk.IInternalPersistentRef)
                {
                    var This = (OnDisk.IInternalPersistentRef) this;
                    This.DataAddress = value.DataAddress;
                    if (value.DataAddress > -1 && This.MruManager != null)
                        This.MruManager[This.DataAddress] = value;
                }
                else
                    diskBuffer = value;
            }
        }

        /// <summary>
        /// Implement to return the size on disk(in bytes) of this object
        /// </summary>
        public int HintSizeOnDisk { get; internal set; }

        /// <summary>
        /// IsDirty tells BTree whether this object needs to be rewritten to disk(dirty) or not
        /// </summary>
        public virtual bool IsDirty
        {
            get
            {
                return diskBuffer == null ? _isDirty : DiskBuffer.IsDirty;
            }
            set
            {
                if (diskBuffer != null)
                    DiskBuffer.IsDirty = value;
                else
                    _isDirty = value;
            }
        }

        internal bool _isDirty = true;
        protected internal DataBlockSize DataBlockSize;
    }
}