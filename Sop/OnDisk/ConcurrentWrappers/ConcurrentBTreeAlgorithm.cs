// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sop.Synchronization;

namespace Sop.OnDisk.ConcurrentWrappers
{
    internal class ConcurrentBTreeAlgorithm : ConcurrentCollectionOnDisk<Algorithm.BTree.IBTreeAlgorithm>,
                                              Algorithm.BTree.IBTreeAlgorithm
    {
        public ConcurrentBTreeAlgorithm(Algorithm.BTree.IBTreeAlgorithm bTree) : base(bTree) { }
        public ConcurrentBTreeAlgorithm(File.IFile file,
                              System.Collections.IComparer comparer = null,
                              string name = null,
                              DataBlock.IDataBlockDriver dataBlockDriver = null,
                              bool isDataInKeySegment = true)
        {
            Collection = new Algorithm.BTree.BTreeAlgorithm(file, comparer, name, dataBlockDriver, isDataInKeySegment);
        }

        new public ISynchronizer Locker
        {
            get
            {
                return (ISynchronizer)SyncRoot;
            }
        }

        public bool AddIfNotExist(Algorithm.BTree.BTreeItemOnDisk item)
        {
            Locker.Lock();
            var r = Collection.AddIfNotExist(item);
            Locker.Unlock();
            return r;
        }

        public void Add(Algorithm.BTree.BTreeItemOnDisk item)
        {
            Locker.Lock();
            Collection.Add(item);
            Locker.Unlock();
        }

        public bool ChangeRegistry
        {
            get
            {
                Locker.Lock();
                var r = Collection.ChangeRegistry;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.ChangeRegistry = value;
                Locker.Unlock();
            }
        }

        public void Clear()
        {
            Locker.Lock();
            Collection.Clear();
            Locker.Unlock();
        }

        public object Clone()
        {
            Locker.Lock();
            var r = Collection.Clone();
            Locker.Unlock();
            return r;
        }

        public System.Collections.IComparer Comparer
        {
            get
            {
                Locker.Lock();
                var r = Collection.Comparer;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.Comparer = value;
                Locker.Unlock();
            }
        }

        public object CurrentKey
        {
            get
            {
                Locker.Lock();
                var r = Collection.CurrentKey;
                Locker.Unlock();
                return r;
            }
        }

        public Algorithm.BTree.BTreeNodeOnDisk CurrentNode
        {
            get
            {
                Locker.Lock();
                var r = Collection.CurrentNode;
                Locker.Unlock();
                return r;
            }
        }

        public object CurrentValue
        {
            get
            {
                Locker.Lock();
                var r = Collection.CurrentValue;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.CurrentValue = value;
                Locker.Unlock();
            }
        }

        public long DataAddress
        {
            get
            {
                if (Collection == null)
                    return -1;
                Locker.Lock();
                var r = Collection.DataAddress;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.DataAddress = value;
                Locker.Unlock();
            }
        }

        public void Delete()
        {
            Locker.Lock();
            Collection.Delete();
            Locker.Unlock();
        }

        public long GetNextSequence()
        {
            Locker.Lock();
            var r = Collection.GetNextSequence();
            Locker.Unlock();
            return r;
        }

        public int HintBatchCount
        {
            get
            {
                Locker.Lock();
                var r = Collection.HintBatchCount;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.HintBatchCount = value;
                Locker.Unlock();
            }
        }

        public int HintKeySizeOnDisk
        {
            get
            {
                Locker.Lock();
                var r = Collection.HintKeySizeOnDisk;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.HintKeySizeOnDisk = value;
                Locker.Unlock();
            }
        }

        public bool HintSequentialRead
        {
            get
            {
                Locker.Lock();
                var r = Collection.HintSequentialRead;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.HintSequentialRead = value;
                Locker.Unlock();
            }
        }

        public int HintValueSizeOnDisk
        {
            get
            {
                Locker.Lock();
                var r = Collection.HintValueSizeOnDisk;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.HintValueSizeOnDisk = value;
                Locker.Unlock();
            }
        }

        public DataBlockSize IndexBlockSize
        {
            get
            {
                Locker.Lock();
                var r = Collection.IndexBlockSize;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.IndexBlockSize = value;
                Locker.Unlock();
            }
        }

        public bool IsDataInKeySegment
        {
            get
            {
                Locker.Lock();
                var r = Collection.IsDataInKeySegment;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.IsDataInKeySegment = value;
                Locker.Unlock();
            }
        }

        public bool IsOnPackEventHandlerSet
        {
            get
            {
                Locker.Lock();
                var r = Collection.IsOnPackEventHandlerSet;
                Locker.Unlock();
                return r;
            }
        }

        public event OnObjectPack OnInnerMemberKeyPack
        {
            add
            {
                Locker.Lock();
                Collection.OnInnerMemberKeyPack += value;
                Locker.Unlock();
            }
            remove
            {
                Locker.Lock();
                Collection.OnInnerMemberKeyPack -= value;
                Locker.Unlock();
            }
        }

        public event OnObjectUnpack OnInnerMemberKeyUnpack
        {
            add
            {
                Locker.Lock();
                Collection.OnInnerMemberKeyUnpack += value;
                Locker.Unlock();
            }
            remove
            {
                Locker.Lock();
                Collection.OnInnerMemberKeyUnpack -= value;
                Locker.Unlock();
            }
        }

        public event OnObjectPack OnInnerMemberValuePack
        {
            add
            {
                Locker.Lock();
                Collection.OnInnerMemberValuePack += value;
                Locker.Unlock();
            }
            remove
            {
                Locker.Lock();
                Collection.OnInnerMemberValuePack -= value;
                Locker.Unlock();
            }
        }

        public event OnObjectUnpack OnInnerMemberValueUnpack
        {
            add
            {
                Locker.Lock();
                Collection.OnInnerMemberValueUnpack += value;
                Locker.Unlock();
            }
            remove
            {
                Locker.Lock();
                Collection.OnInnerMemberValueUnpack -= value;
                Locker.Unlock();
            }
        }

        public event OnObjectPack OnKeyPack
        {
            add
            {
                Locker.Lock();
                Collection.OnKeyPack += value;
                Locker.Unlock();
            }
            remove
            {
                Locker.Lock();
                Collection.OnKeyPack -= value;
                Locker.Unlock();
            }
        }

        public event OnObjectUnpack OnKeyUnpack
        {
            add
            {
                Locker.Lock();
                Collection.OnKeyUnpack += value;
                Locker.Unlock();
            }
            remove
            {
                Locker.Lock();
                Collection.OnKeyUnpack -= value;
                Locker.Unlock();
            }
        }

        public event OnObjectPack OnValuePack
        {
            add
            {
                Locker.Lock();
                Collection.OnValuePack += value;
                Locker.Unlock();
            }
            remove
            {
                Locker.Lock();
                Collection.OnValuePack -= value;
                Locker.Unlock();
            }
        }

        public event OnObjectUnpack OnValueUnpack
        {
            add
            {
                Locker.Lock();
                Collection.OnValueUnpack += value;
                Locker.Unlock();
            }
            remove
            {
                Locker.Lock();
                Collection.OnValueUnpack -= value;
                Locker.Unlock();
            }
        }

        public void OnMaxCapacity()
        {
            Locker.Lock();
            Collection.OnMaxCapacity();
            Locker.Unlock();
        }

        public int OnMaxCapacity(System.Collections.IEnumerable nodes)
        {
            Locker.Lock();
            var r = Collection.OnMaxCapacity(nodes);
            Locker.Unlock();
            return r;
        }

        public bool Query(QueryExpression[] items, out QueryResult[] results)
        {
            Locker.Lock();
            var r = Collection.Query(items, out results);
            Locker.Unlock();
            return r;
        }

        public void Remove()
        {
            Locker.Lock();
            Collection.Remove();
            Locker.Unlock();
        }

        public bool Remove(QueryExpression[] items, bool removeAllOccurrence, out QueryResult[] results)
        {
            Locker.Lock();
            var r = Collection.Remove(items, removeAllOccurrence, out results);
            Locker.Unlock();
            return r;
        }

        public bool Remove(object item)
        {
            Locker.Lock();
            var r = Collection.Remove(item);
            Locker.Unlock();
            return r;
        }

        public bool Remove(object item, bool removeAllOccurrence)
        {
            Locker.Lock();
            var r = Collection.Remove(item, removeAllOccurrence);
            Locker.Unlock();
            return r;
        }

        public Algorithm.BTree.BTreeNodeOnDisk RootNode
        {
            get
            {
                Locker.Lock();
                var r = Collection.RootNode;
                Locker.Unlock();
                return r;
            }
        }

        public bool Search(object item)
        {
            Locker.Lock();
            var r = Collection.Search(item);
            Locker.Unlock();
            return r;
        }

        public bool Search(object item, bool goToFirstInstance)
        {
            Locker.Lock();
            var r = Collection.Search(item, goToFirstInstance);
            Locker.Unlock();
            return r;
        }

        public void SetDiskBlock(Sop.DataBlock headBlock)
        {
            Locker.Lock();
            Collection.SetDiskBlock(headBlock);
            Locker.Unlock();
        }

        public short SlotLength
        {
            get
            {
                Locker.Lock();
                var r = Collection.SlotLength;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.SlotLength = value;
                Locker.Unlock();
            }
        }
    }
}
