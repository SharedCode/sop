// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
namespace Sop.OnDisk.Algorithm.BTree
{
    /// <summary>
    /// BTree Algorithm interface.
    /// </summary>
    interface IBTreeAlgorithm : Collection.ICollectionOnDisk
    {
        bool AddIfNotExist(BTreeItemOnDisk item);
        void Add(BTreeItemOnDisk item);
        bool ChangeRegistry { get; set; }
        void Clear();
        object Clone();
        System.Collections.IComparer Comparer { get; set; }
        object CurrentKey { get; }
        BTreeNodeOnDisk CurrentNode { get; }
        object CurrentValue { get; set; }
        long DataAddress { get; set; }
        void Delete();
        long GetNextSequence();
        int HintBatchCount { get; set; }
        int HintKeySizeOnDisk { get; set; }
        bool HintSequentialRead { get; set; }
        int HintValueSizeOnDisk { get; set; }
        Sop.DataBlockSize IndexBlockSize { get; set; }
        bool IsDataInKeySegment { get; set; }
        bool IsOnPackEventHandlerSet { get; }
        event Sop.OnObjectPack OnInnerMemberKeyPack;
        event Sop.OnObjectUnpack OnInnerMemberKeyUnpack;
        event Sop.OnObjectPack OnInnerMemberValuePack;
        event Sop.OnObjectUnpack OnInnerMemberValueUnpack;
        event Sop.OnObjectPack OnKeyPack;
        event Sop.OnObjectUnpack OnKeyUnpack;
        void OnMaxCapacity();
        int OnMaxCapacity(System.Collections.IEnumerable nodes);
        event Sop.OnObjectPack OnValuePack;
        event Sop.OnObjectUnpack OnValueUnpack;
        bool Query(Sop.QueryExpression[] items, out Sop.QueryResult[] results);
        void Remove();
        bool Remove(Sop.QueryExpression[] items, bool removeAllOccurrence, out Sop.QueryResult[] results);
        bool Remove(object item);
        bool Remove(object item, bool removeAllOccurrence);
        BTreeNodeOnDisk RootNode { get; }
        bool Search(object item);
        bool Search(object item, bool goToFirstInstance);
        void SetDiskBlock(Sop.DataBlock headBlock);
        short SlotLength { get; set; }
    }
}
