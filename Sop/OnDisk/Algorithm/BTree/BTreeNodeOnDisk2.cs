// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using Sop.Collections.BTree;
using Sop.OnDisk.Algorithm.Collection;
using Sop.Persistence;
using Sop.Recycling;

namespace Sop.OnDisk.Algorithm.BTree
{
    /// <summary>
    /// 2nd part of the BTreeNodeOnDisk class
    /// </summary>
    internal partial class BTreeNodeOnDisk
    {
        /// <summary>
        /// A BTreeAlgorithm's item address is composed of the node's address + the item's index in the Slots.
        /// </summary>
        internal class ItemAddress
        {
            public long NodeAddress = -1;
            public short NodeItemIndex = -1;

            public bool IsEqual(ItemAddress other)
            {
                return NodeAddress == other.NodeAddress && NodeItemIndex == other.NodeItemIndex;
            }
            public BTreeNodeOnDisk GetNode(BTree.BTreeAlgorithm bTree)
            {
                BTreeNodeOnDisk r = null;
                if (NodeAddress != -1)
                    r = BTreeNodeOnDisk.GetNode(bTree, NodeAddress);
                return r;
            }
        }

        #region Constructors
        public BTreeNodeOnDisk() { }
        public BTreeNodeOnDisk(BTree.BTreeAlgorithm bTree)
        {
            Initialize(bTree, -1);
        }

        /// <summary>
        /// Constructor expecting ParentTree and ParentNode params.
        ///	This form is invoked from another instance of this class when node 
        ///	splitting occurs. Normally, node split occurs to accomodate new items that
        ///	could not be loaded to the node since the node is already full. 
        ///	Calls <see cref="Initialize"/> for member initialization.
        /// </summary>
        /// <param name="bTree">Parent B-Tree instance</param>
        /// <param name="parentNodeAddress">Parent Node instance</param>
        protected BTreeNodeOnDisk(BTree.BTreeAlgorithm bTree, long parentNodeAddress)
        {
            Initialize(bTree, parentNodeAddress);
        }
        #endregion

        internal bool IsRoot()
        {
            return ParentAddress == -1;
        }

        /// <summary>
        /// IsDirty tells BTree whether this object needs to be rewritten to disk(dirty) or not
        /// </summary>
        public bool IsDirty
        {
            get
            {
                if (!_isDirty && Count > 0)
                {
                    if (SynchronizeCount())
                    {
                        for (int i = 0; i < Count; i++)
                        {
                            if (Slots[i].Value.IsDirty ||
                                (Slots[i].ValueLoaded && Slots[i].Value.Data is IInternalPersistent &&
                                 ((IInternalPersistent)Slots[i].Value.Data).IsDirty))
                            {
                                _isDirty = true;
                                break;
                            }
                        }
                    }
                }
                return _isDirty;
            }
            set
            {
                _isDirty = value;
            }
        }
        private bool _isDirty = true;

        /// <summary>
        /// Node disk buffer.
        /// </summary>
        public Sop.DataBlock DiskBuffer
        {
            get { return _diskBuffer; }
            set
            {
                _diskBuffer = value;
                if (_diskBuffer != null)
                    _diskBuffer.IsHead = true;
            }
        }
        private Sop.DataBlock _diskBuffer;

        /// <summary>
        /// Return the size on disk(in bytes) of this object
        /// </summary>
        public int HintSizeOnDisk { get; private set; }

        #region Packing related
        /// <summary>
        /// Pack serializes this node to the Stream.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="writer"></param>
        public void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            //** Pack the Node data
            writer.Write(Count);
            writer.Write(HintSizeOnDisk);
            writer.Write(ParentAddress);
            #region a.) Write children node addresses...
            short ii = 0;
            if (ChildrenAddresses != null)
            {
                for (; ii <= Count; ii++)
                    writer.Write(ChildrenAddresses[ii]);
            }
            #endregion
            #region b.) Write fillers for future children...
            for (; ii <= Slots.Length; ii++)
                writer.Write(-1L);
            #endregion

            bool? isKeySimpleType = null;
            object simpleKey = null;
            var parentTree = (BTreeAlgorithm)parent;
            byte[] keyOfEmptySlots = null;

            int keySizeOnDisk = parentTree.HintKeySizeOnDisk;
            if (keySizeOnDisk > 0)
                keyOfEmptySlots = new byte[keySizeOnDisk];
            //** Pack each Item's Key & Value if in Key region...
            for (short i = 0; i < Slots.Length; i++)
            {
                if (isKeySimpleType == null)
                {
                    isKeySimpleType = Slots[i] != null &&
                                      (CollectionOnDisk.IsSimpleType(Slots[i].Key) || Slots[i].Key is string);
                    if (isKeySimpleType.Value)
                        simpleKey = Slots[i].Key;
                }
                if (i < Count)
                {
                    // Pack Node item key and/or value (if value is in key segment)
                    PackSlotItem(parentTree, writer, Slots[i], ref keySizeOnDisk, isKeySimpleType);
                    if (keyOfEmptySlots == null || keySizeOnDisk > keyOfEmptySlots.Length)
                    {
                        parentTree.HintKeySizeOnDisk = keySizeOnDisk;
                        keyOfEmptySlots = new byte[keySizeOnDisk];
                    }
                }
                else
                    // Pack fillers so a Node can be kept in a contiguous space on disk... (fragmentation is costly)
                    PackSlotFillers(parentTree, writer, simpleKey, ref keySizeOnDisk, ref keyOfEmptySlots);
            }
        }
        private void PackSlotItem(BTreeAlgorithm parent, System.IO.BinaryWriter writer,
            BTreeItemOnDisk slot, ref int keySizeOnDisk, bool? isKeySimpleType)
        {
            if (slot != null)
            {
                IO.OnDiskBinaryWriter _writer = (IO.OnDiskBinaryWriter)writer;
                long streamPos = _writer.LogicalPosition;
                if (slot.Value.IsDirty)
                    slot.VersionNumber++;

                writer.Write(slot.VersionNumber);
                CollectionOnDisk.WritePersistentData(parent, slot.Key, writer, ItemType.Key);
                if (parent.IsDataInKeySegment)
                {
                    if (parent.IsDataLongInt)
                        writer.Write((long)slot.Value.Data);
                    else
                    {
                        if (parent.PersistenceType == PersistenceType.Unknown)
                        {
                            parent.PersistenceType =
                                CollectionOnDisk.GetPersistenceType(parent, slot.Value.Data, ItemType.Value);
                            parent.IsDirty = true;
                        }
                        //** write the value and keep track of its data size and location in the disk buffer.
                        long startPos = _writer.LogicalPosition;
                        CollectionOnDisk.WritePersistentData(parent, slot.Value.Data, writer, ItemType.Value);
                        slot.HintSizeOnDisk = (int)(_writer.LogicalPosition - startPos);
                    }
                    slot.Value.IsDirty = false;
                }
                else
                {
                    if (slot.Value.diskBuffer == null)
                        slot.Value.DiskBuffer = parent.CreateBlock();
                    writer.Write(parent.GetId(slot.Value.DiskBuffer));
                    writer.Write((ushort)slot.Value.DiskBuffer.CountMembers(true));
                }
                int keyDataSize = (int)(_writer.LogicalPosition - streamPos);
                if (keyDataSize > keySizeOnDisk)
                {
                    keySizeOnDisk = keyDataSize;
                    parent.HintKeySizeOnDisk = keyDataSize;
                }
            }
        }
        private void PackSlotFillers(BTreeAlgorithm parent, System.IO.BinaryWriter writer,
            object simpleKey, ref int keySizeOnDisk, ref byte[] keyOfEmptySlots)
        {
            // write filler space same size as VersionNumber
            CollectionOnDisk.WritePersistentData(parent, 0L, writer);

            // write filler space same size as Key
            if (keySizeOnDisk > 0 || simpleKey == null)
            {
                if (keySizeOnDisk == 0)
                {
                    keySizeOnDisk = 20;
                    keySizeOnDisk += (sizeof(long) * 2);
                }
                if (keyOfEmptySlots == null)
                    keyOfEmptySlots = new byte[keySizeOnDisk];
                CollectionOnDisk.WritePersistentData(parent, keyOfEmptySlots, writer, ItemType.Key);
            }
            else
                CollectionOnDisk.WritePersistentData(parent, simpleKey, writer, ItemType.Key);

            // write filler space same size as Value's Address on disk
            //CollectionOnDisk.WritePersistentData(parent, -1L, writer);
        }
        #endregion

        /// <summary>
        /// Unpack Deserializes Node from Stream
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        public void Unpack(IInternalPersistent parent,
                           System.IO.BinaryReader reader)
        {
            Count = reader.ReadInt16();
            HintSizeOnDisk = reader.ReadInt32();
            ParentAddress = reader.ReadInt64();
            if (Slots == null)
                Slots = new BTreeItemOnDisk[((BTreeAlgorithm)parent).SlotLength];
            if (ChildrenAddresses == null)
            {
                ChildrenAddresses = new long[Slots.Length + 1];
                ResetArray(ChildrenAddresses, -1);
            }
            short newCount = 0;
            for (int i = 0; i <= Slots.Length; i++)
            {
                ChildrenAddresses[i] = reader.ReadInt64();
                if (ChildrenAddresses[i] != -1)
                    newCount++;
            }
            if (ChildrenAddresses[0] == -1L)
                ChildrenAddresses = null;
            else if (newCount > 0 && Count != newCount - 1)
                Count = (short)(newCount - 1);
            for (int i = 0; i < Count; i++)
            {
                Slots[i] = new BTreeItemOnDisk();
                object key = null;

                int vn = reader.ReadInt32();
                Slots[i].VersionNumber = vn;
                if (Slots[i].Key is IPersistentVersioned)
                    ((IPersistentVersioned)Slots[i].Key).VersionNumber = Slots[i].VersionNumber;
                // read key from disk
                CollectionOnDisk.ReadPersistentData(parent, reader, ref key);

                if (key == null)
                {
                    if (((BTreeAlgorithm)parent).onKeyUnpack != null)
                        key = ((BTreeAlgorithm)parent).onKeyUnpack(reader);
                    if (key == null)
                    {
                        if (i == 0)
                        {
                            ((BTreeAlgorithm)parent).RootNeedsReload = true;
                            return;
                        }
                        throw new InvalidOperationException(
                            "Can't DeSerialize Key, ensure there is a TypeStore Entry for this data type.");
                    }
                }

                Slots[i].Key = key;
                Slots[i].Value = new ItemOnDisk();
                if (((BTreeAlgorithm)parent).IsDataInKeySegment)
                {
                    if (((BTreeAlgorithm)parent).IsDataLongInt)
                    {
                        long l = reader.ReadInt64();
                        Slots[i].Value.Data = l;
                    }
                    else
                    {
                        if (((BTreeAlgorithm)parent).PersistenceType == PersistenceType.Unknown)
                            throw new InvalidOperationException("Parent BTreeAlgorithm PersistenceType is unknown.");
                        // write the value and keep track of its data size and location in the disk buffer.
                        long startPos = reader.BaseStream.Position;
                        if (CollectionOnDisk.ReadPersistentData(parent, reader, ref Slots[i].Value.Data, ItemType.Value) == null)
                            ((BTreeAlgorithm)parent).ValueUnpack(reader, Slots[i]);
                        Slots[i].HintSizeOnDisk = (int)(reader.BaseStream.Position - startPos);
                    }
                    Slots[i].ValueLoaded = true;
                    Slots[i].Value.IsDirty = false;
                }
                else
                {
                    // read Address of Value in Data Segment
                    long l = reader.ReadInt64();
                    Slots[i].Value.DiskBuffer = ((BTreeAlgorithm)parent).CreateBlock();
                    // new Sop.DataBlock((DataBlockSize) parent.DiskBuffer.Length);
                    ((CollectionOnDisk)parent).SetIsDirty(Slots[i].Value.DiskBuffer, false);
                    Slots[i].ValueLoaded = false;
                    ((BTreeAlgorithm)parent).DataBlockDriver.SetId(Slots[i].Value.DiskBuffer, l);
                    Slots[i].Value.DiskBuffer.contiguousBlockCount = reader.ReadUInt16();
                }
            }
        }

        public override string ToString()
        {
            return DiskBuffer.ToString();
        }


        void IRecyclable.Initialize()
        {
            _diskBuffer = null;
            ParentAddress = -1;
            ResetArray(Slots, null);
            ChildrenAddresses = null;
            Count = 0;
            IsDirty = true;
        }

        /// <summary>
        /// Do class variable/object initialization. Usually invoked from this class' constructor.
        /// </summary>
        /// <param name="bTree">Parent BTree</param>
        /// <param name="parentNodeAddress">Parent Node</param>
        protected internal void Initialize(BTree.BTreeAlgorithm bTree, long parentNodeAddress)
        {
            DiskBuffer = bTree.CreateBlock();    //new Sop.DataBlock(bTree.IndexBlockSize);
            Slots = new BTreeItemOnDisk[bTree.SlotLength];
            ParentAddress = parentNodeAddress;
        }

        /// <summary>
        /// Returns true if slots are all occupied, else false
        /// </summary>
        /// <param name="slotLength">Number of slots per node</param>
        /// <returns>true if full, else false</returns>
        private bool IsFull(short slotLength)
        {
            return Count == slotLength;
        }

        #region Array utility functions
        /// <summary>
        /// "CopyArrayElements" copies elements of an array (Source) to destination array (Destination).
        /// </summary>
        /// <param name="source">Array to copy elements from</param>
        /// <param name="srcIndex">Index of the 1st element to copy</param>
        /// <param name="destination">Array to copy elements to</param>
        /// <param name="destIndex">Index of the 1st element to copy to</param>
        /// <param name="count">Number of elements to copy</param>
        private static void CopyArrayElements<T>(T[] source, int srcIndex, T[] destination, int destIndex, int count)
        {
            if (source != null && destination != null)
            {
                for (short i = 0; i < count; i++)
                    destination[destIndex + i] = source[srcIndex + i];
            }
        }

        // Utility methods...
        /// <summary>
        /// "Shallow" move elements of an array. 
        /// "MoveArrayElements" moves a group (Count) of elements of an array from
        /// source index to destination index.
        /// </summary>
        /// <param name="array">Array whose elements will be moved</param>
        /// <param name="srcIndex">Source index of the 1st element to move</param>
        /// <param name="destIndex">Target index of the 1st element to move to</param>
        /// <param name="count">Number of elements to move</param>
        internal static void MoveArrayElements<T>(T[] array, int srcIndex, int destIndex, int count)
        {
            sbyte addValue = -1;
            int srcStartIndex = srcIndex + count - 1, destStartIndex = destIndex + count - 1;
            if (destIndex < srcIndex)
            {
                srcStartIndex = srcIndex;
                destStartIndex = destIndex;
                addValue = 1;
            }
            if (array != null)
            {
                for (int i = 0; i < count; i++)
                {
                    if (destStartIndex < array.Length) // only process if w/in array range
                    {
                        array[destStartIndex] = array[srcStartIndex];
                        destStartIndex = destStartIndex + addValue;
                        srcStartIndex = srcStartIndex + addValue;
                    }
                }
            }
        }

        protected internal static void ResetArray<T>(T[] array, T value)
        {
            ResetArray(array, value, array.Length);
        }

        /// <summary>
        /// Reset all elements of the array to Value
        /// </summary>
        /// <param name="array">Array to reset all elements of</param>
        /// <param name="value">Value to assign to each element of the array</param>
        /// <param name="itemCount"> </param>
        protected internal static void ResetArray<T>(T[] array, T value, int itemCount)
        {
            if (array != null)
            {
                for (ushort i = 0; i < itemCount; i++)
                    array[i] = value;
            }
        }

        /// <summary>
        /// Skud over one slot all items to the right.
        /// The 1st element moved will then be vacated ready for an occupant.
        /// </summary>
        /// <param name="slots">"Slots" to skud over its contents</param>
        /// <param name="position">1st element index to skud over</param>
        /// <param name="noOfOccupiedSlots">Number of occupied slots</param>
        private static void ShiftSlots<T>(T[] slots, int position, int noOfOccupiedSlots)
        {
            if (position < noOfOccupiedSlots)
                // create a vacant slot by shifting node contents one slot
                MoveArrayElements(slots, position, (short)(position + 1), (short)(noOfOccupiedSlots - position));
        }

        // private modified binary search that facilitates Search of a key and if duplicates were found, 
        // positions the current record pointer to the 1st key instance.
        private static int BinarySearch(System.Array array, int index, int length, object value,
                                        System.Collections.IComparer comparer)
        {
            int r;
            if (comparer != null && index != -1 && length != -1)
                r = Array.BinarySearch(array, index, length, value, comparer);
#if !DEVICE
            else if (index != -1 && length != -1)
                r = Array.BinarySearch(array, index, length, value);
#endif
            else
                r = Array.BinarySearch(array, value);
            if (r >= 0)
            {
                if (r >= 1)
                {
                    int rr = BinarySearch(array, 0, r, value, comparer);
                    if (rr >= 0)
                        return rr;
                }
            }
            return r;
        }
        #endregion

        /// <summary>
        /// Count of items in this B-Tree
        /// </summary>
        public short Count { get; set; }
    }
}