// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;

namespace Sop.OnDisk
{
    /// <summary>
    /// Enumerates SOP Built-in types. Data Block Driver uses this
    /// list to be able to provide low-level deserialization for built-in
    /// types such as ArrayListOnDisk, LinkedListOnDisk, etc...
    /// Being able to deserialize low-level types is important, for example,
    /// for "bootup" types serialization.
    /// </summary>
    internal enum BuiltinTypes
    {
        MinType,
        LinkedListOnDisk = MinType,
        LinkedListOnDiskItemOnDisk,
        SharedBlockOnDiskList,
        SortedDictionaryOnDisk,
        BTreeOnDiskTreeNode,
        BTreeAlgorithm,
        File,
        FileSet,
        //** for rename to item on disk
        BTreeItemOnDisk,
        VirtualBTree,
        VirtualBTreeOnDisk,
        DeletedBlockInfo,
        Segment,
        DataReference,
        BackupDataLogKey,
        BackupDataLogValue,
        UserDefined,
        MaxType = UserDefined,
    }
}