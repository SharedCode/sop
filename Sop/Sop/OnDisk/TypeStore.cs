// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.Algorithm.LinkedList;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.OnDisk.DataBlock;
using Sop.OnDisk.File;

namespace Sop.OnDisk
{
    /// <summary>
    /// Type Store
    /// </summary>
    internal class TypeStore
    {
        /// <summary>
        /// Create Instance
        /// </summary>
        /// <param name="transaction"> </param>
        /// <param name="typeId"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public object CreateInstance(Transaction.ITransactionLogger transaction,
                                     int typeId, params KeyValuePair<string, object>[] parameters)
        {
            if (typeId >= (int) BuiltinTypes.MinType && typeId <= (int) BuiltinTypes.MaxType)
            {
                File.File f;
                switch ((BuiltinTypes) typeId)
                {
                    case BuiltinTypes.SortedDictionaryOnDisk:
                        f = (File.File) CollectionOnDisk.GetParamValue(parameters, "File");
                        if (f != null)
                        {
                            if (transaction is Sop.Transaction.TransactionBase)
                                return ((Sop.Transaction.TransactionBase) transaction).CreateCollection(f);
                            else
                                return new SortedDictionaryOnDisk(f);
                        }
                        return transaction is Sop.Transaction.TransactionBase
                                   ? ((Sop.Transaction.TransactionBase) transaction).CreateCollection(f)
                                   : new SortedDictionaryOnDisk();
                    case BuiltinTypes.SharedBlockOnDiskList:
                        f = (File.File) CollectionOnDisk.GetParamValue(parameters, "File");
                        if (f != null)
                            return new SharedBlockOnDiskList(f);
                        return new SharedBlockOnDiskList();
                    case BuiltinTypes.BTreeAlgorithm:
                        return new BTreeAlgorithm();
                    case BuiltinTypes.File:
                        return transaction is Sop.Transaction.TransactionBase
                                   ? ((Sop.Transaction.TransactionBase) transaction).CreateFile()
                                   : new File.File();
                    case BuiltinTypes.FileSet:
                        return transaction is Sop.Transaction.TransactionBase
                                   ? ((Sop.Transaction.TransactionBase) transaction).CreateFileSet()
                                   : new FileSet();
                    case BuiltinTypes.LinkedListOnDisk:
                        return new LinkedListOnDisk();
                    case BuiltinTypes.LinkedListOnDiskItemOnDisk:
                        return new LinkedListOnDisk.LinkedItemOnDisk();
                    case BuiltinTypes.BTreeOnDiskTreeNode:
                        return new BTreeNodeOnDisk();
                    case BuiltinTypes.BTreeItemOnDisk:
                        return new ItemOnDisk();
                    case BuiltinTypes.DeletedBlockInfo:
                        return new DeletedBlockInfo();
                    case BuiltinTypes.DataReference:
                        return new DataReference();
                    case BuiltinTypes.BackupDataLogKey:
                        return new Transaction.Transaction.BackupDataLogKey();
                    case BuiltinTypes.BackupDataLogValue:
                        return new Transaction.Transaction.BackupDataLogValue();
                    case BuiltinTypes.UserDefined:
                        return null;
                    default:
                        throw new ArgumentOutOfRangeException("typeId", typeId, "Not supported Type ID.");
                }
            }
            //** TypeID should be a User type...
            throw new InvalidOperationException(
                string.Format("Built-in TypeStore doesn't support this Type ID '{0}'.", typeId)
                );
        }

        /// <summary>
        /// Register type and return a Type ID.
        /// </summary>
        /// <param name="value"> </param>
        /// <returns>ID(integer) of the type</returns>
        public int RegisterType(object value)
        {
            if (value is SortedDictionaryOnDisk)
                return (int) BuiltinTypes.SortedDictionaryOnDisk;
            if (value is FileSet)
                return (int) BuiltinTypes.FileSet;
            if (value is File.File)
                return (int) BuiltinTypes.File;
            if (value is BTreeAlgorithm)
                return (int) BuiltinTypes.BTreeAlgorithm;
            if (value is SharedBlockOnDiskList)
                return (int) BuiltinTypes.SharedBlockOnDiskList;
            if (value is LinkedListOnDisk)
                return (int) BuiltinTypes.LinkedListOnDisk;
            if (value is LinkedListOnDisk.LinkedItemOnDisk)
                return (int) BuiltinTypes.LinkedListOnDiskItemOnDisk;
            if (value is BTreeNodeOnDisk)
                return (int) BuiltinTypes.BTreeOnDiskTreeNode;
            if (value is ItemOnDisk)
                return (int) BuiltinTypes.BTreeItemOnDisk;
            if (value is DeletedBlockInfo)
                return (int) BuiltinTypes.DeletedBlockInfo;
            if (value is DataReference)
                return (int) BuiltinTypes.DataReference;
            if (value is Transaction.Transaction.BackupDataLogKey)
                return (int) BuiltinTypes.BackupDataLogKey;
            if (value is Transaction.Transaction.BackupDataLogValue)
                return (int) BuiltinTypes.BackupDataLogValue;
            return (int) BuiltinTypes.UserDefined;
        }
    }
}