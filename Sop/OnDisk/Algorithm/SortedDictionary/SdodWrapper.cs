// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using Sop.Collections.BTree;
using Sop.Mru;
using System.Collections;
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.DataBlock;
using Sop.OnDisk.File;
using Sop.OnDisk.IO;
using Sop.Persistence;
using ICollection = System.Collections.ICollection;
using IDictionary = System.Collections.IDictionary;

namespace Sop.OnDisk.Algorithm.SortedDictionary
{
    internal class SdodWrapper : Algorithm.SortedDictionary.ISortedDictionaryOnDisk
    {
        public SdodWrapper()
        {
        }

        public SdodWrapper(File.IFile file, System.Collections.IComparer comparer, string name,
                           bool isDataInKeySegment)
        {
            RealDictionary = new SortedDictionaryOnDisk(file, comparer, name, isDataInKeySegment);
        }

        public SdodWrapper(Algorithm.SortedDictionary.ISortedDictionaryOnDisk sDod)
        {
            if (sDod == null)
                throw new ArgumentNullException("sDod");
            RealDictionary = sDod;
        }


        public Sop.Collections.ISynchronizer Locker
        {
            get
            {
                return ((Sop.Collections.ISynchronizer)SyncRoot);
            }
        }

        public virtual DataBlockSize IndexBlockSize
        {
            get { return RealDictionary.IndexBlockSize; }
        }

        public virtual IPersistent GetValue(object key, IPersistent target)
        {
            return RealDictionary.GetValue(key, target);
        }

        public virtual IPersistent GetCurrentValue(IPersistent target)
        {
            return RealDictionary.GetCurrentValue(target);
        }

        public virtual Sop.ISortedDictionaryOnDisk Container
        {
            get { return RealDictionary.Container; }
            set { RealDictionary.Container = value; }
        }

        public virtual IComparer Comparer
        {
            get { return RealDictionary.Comparer; }
            set { RealDictionary.Comparer = value; }
        }

        public virtual bool Query(QueryExpression[] keys, out QueryResult[] values)
        {
            return RealDictionary.Query(keys, out values);
        }

        public virtual void Initialize(File.IFile file)
        {
            RealDictionary.Initialize(file);
        }

        public virtual bool ChangeRegistry
        {
            get { return RealDictionary.ChangeRegistry; }
            set { RealDictionary.ChangeRegistry = value; }
        }

        public virtual BTreeItemOnDisk CurrentItemOnDisk
        {
            get { return RealDictionary.CurrentItemOnDisk; }
        }

        public virtual long CurrentSequence
        {
            get { return RealDictionary.CurrentSequence; }
            set { RealDictionary.CurrentSequence = value; }
        }

        public virtual long DataAddress
        {
            get { return RealDictionary.DataAddress; }
            set { RealDictionary.DataAddress = value; }
        }

        public virtual IDataBlockDriver DataBlockDriver
        {
            get { return RealDictionary.DataBlockDriver; }
        }

        public virtual bool Update(object key, long itemAddress, object value)
        {
            return RealDictionary.Update(key, itemAddress, value);
        }

        public virtual void Delete()
        {
            RealDictionary.Delete();
        }

        public virtual long GetId()
        {
            return RealDictionary.GetId();
        }

        public virtual long GetNextSequence()
        {
            return RealDictionary.GetNextSequence();
        }

        public virtual int HintKeySizeOnDisk
        {
            get { return RealDictionary.HintKeySizeOnDisk; }
            set { RealDictionary.HintKeySizeOnDisk = value; }
        }

        public virtual int HintValueSizeOnDisk
        {
            get { return RealDictionary.HintValueSizeOnDisk; }
            set { RealDictionary.HintValueSizeOnDisk = value; }
        }

        public virtual bool IsDataLongInt
        {
            get { return RealDictionary.IsDataLongInt; }
            set { RealDictionary.IsDataLongInt = value; }
        }

        public virtual bool IsDataInKeySegment
        {
            get { return RealDictionary.IsDataInKeySegment; }
            set { RealDictionary.IsDataInKeySegment = value; }
        }

        public virtual bool IsItMe(CollectionOnDisk other)
        {
            return RealDictionary.IsItMe(other);
        }

        public virtual bool IsOnInnerMemberPackEventHandlerSet
        {
            get { return RealDictionary.IsOnInnerMemberPackEventHandlerSet; }
        }

        public virtual bool IsOnPackEventHandlerSet
        {
            get { return RealDictionary.IsOnPackEventHandlerSet; }
        }

        //public virtual event OnAddressAcquired OnAddressAcquired
        //{
        //    add { RealDictionary.OnAddressAcquired += value; }
        //    remove { RealDictionary.OnAddressAcquired -= value; }
        //}

        public virtual OnDiskBinaryReader OnDiskBinaryReader
        {
            get { return RealDictionary.OnDiskBinaryReader; }
        }

        public virtual event OnObjectPack OnInnerMemberKeyPack
        {
            add { RealDictionary.OnInnerMemberKeyPack += value; }
            remove { RealDictionary.OnInnerMemberKeyPack -= value; }
        }

        public virtual event OnObjectUnpack OnInnerMemberKeyUnpack
        {
            add { RealDictionary.OnInnerMemberKeyUnpack += value; }
            remove { RealDictionary.OnInnerMemberKeyUnpack -= value; }
        }

        public virtual event OnObjectPack OnInnerMemberValuePack
        {
            add { RealDictionary.OnInnerMemberValuePack += value; }
            remove { RealDictionary.OnInnerMemberValuePack -= value; }
        }

        public virtual event OnObjectUnpack OnInnerMemberValueUnpack
        {
            add { RealDictionary.OnInnerMemberValueUnpack += value; }
            remove { RealDictionary.OnInnerMemberValueUnpack -= value; }
        }

        public virtual event OnObjectPack OnKeyPack
        {
            add { RealDictionary.OnKeyPack += value; }
            remove { RealDictionary.OnKeyPack -= value; }
        }

        public virtual event OnObjectUnpack OnKeyUnpack
        {
            add { RealDictionary.OnKeyUnpack += value; }
            remove { RealDictionary.OnKeyUnpack -= value; }
        }

        public virtual event OnObjectPack OnValuePack
        {
            add { RealDictionary.OnValuePack += value; }
            remove { RealDictionary.OnValuePack -= value; }
        }

        public virtual event OnObjectUnpack OnValueUnpack
        {
            add { RealDictionary.OnValueUnpack += value; }
            remove { RealDictionary.OnValueUnpack -= value; }
        }

        public virtual OnObjectPack OnKeyPackEventHandler
        {
            get { return RealDictionary.OnKeyPackEventHandler; }
        }

        public virtual OnObjectUnpack OnKeyUnpackEventHandler
        {
            get { return RealDictionary.OnKeyUnpackEventHandler; }
        }

        public virtual OnObjectPack OnValuePackEventHandler
        {
            get { return RealDictionary.OnValuePackEventHandler; }
        }

        public virtual OnObjectUnpack OnValueUnpackEventHandler
        {
            get { return RealDictionary.OnValueUnpackEventHandler; }
        }

        public virtual Transaction.ITransactionLogger ParentTransactionLogger
        {
            get { return RealDictionary.ParentTransactionLogger; }
            set { RealDictionary.ParentTransactionLogger = value; }
        }

        public virtual object ReadFromBlock(Sop.DataBlock source)
        {
            return RealDictionary.ReadFromBlock(source);
        }

        public bool Detach(QueryExpression key)
        {
            return RealDictionary.Detach(key);
        }

        public virtual void Remove()
        {
            RealDictionary.Remove();
        }

        public virtual void Remove(object key, bool removeAllOccurence)
        {
            RealDictionary.Remove(key, removeAllOccurence);
        }

        public virtual bool Remove(QueryExpression[] keys, bool removeAllOccurence, out QueryResult[] results)
        {
            return RealDictionary.Remove(keys, removeAllOccurence, out results);
        }

        public virtual void Rename(string newName)
        {
            RealDictionary.Rename(newName);
        }

        public virtual IBTreeNodeOnDisk CurrentNode
        {
            get { return RealDictionary.CurrentNode; }
        }

        public virtual IBTreeNodeOnDisk RootNode
        {
            get { return RealDictionary.RootNode; }
        }

        public virtual void SetId(long address)
        {
            RealDictionary.SetId(address);
        }

        public virtual int Size
        {
            get { return RealDictionary.Size; }
        }

        public virtual IBTree Synchronized()
        {
            return RealDictionary.Synchronized();
        }

        public virtual void SetCurrentValueInMemoryData(object value)
        {
            RealDictionary.SetCurrentValueInMemoryData(value);
        }

        public virtual Collections.Generic.ISortedDictionary<long, Sop.DataBlock> Blocks
        {
            get { return RealDictionary.Blocks; }
            set { RealDictionary.Blocks = value; }
        }

        public virtual bool RemoveInMemory(long dataAddress, Transaction.ITransactionLogger transaction)
        {
            return RealDictionary.RemoveInMemory(dataAddress, transaction);
        }

        public virtual bool AutoDisposeItem
        {
            get { return RealDictionary.AutoDisposeItem; }
            set { RealDictionary.AutoDisposeItem = value; }
        }
        public virtual bool AutoDispose
        {
            get { return RealDictionary.AutoDispose; }
            set { RealDictionary.AutoDispose = value; }
        }
        public virtual bool AutoFlush
        {
            get { return RealDictionary.AutoFlush; }
            set { RealDictionary.AutoFlush = value; }
        }

        public virtual void RegisterChange(bool partialRegister = false)
        {
            RealDictionary.RegisterChange(partialRegister);
        }

        public virtual string Name
        {
            get { return RealDictionary.Name; }
            set { RealDictionary.Name = value; }
        }

        public virtual bool IsValueInStream
        {
            get { return RealDictionary.IsValueInStream; }
        }

        public virtual bool IsTransactionStore
        {
            get { return RealDictionary.IsTransactionStore; }
        }

        public virtual System.IO.BinaryReader StreamReader
        {
            get { return RealDictionary.StreamReader; }
        }

        public virtual File.IFile File
        {
            get { return RealDictionary.File; }
            set { RealDictionary.File = value; }
        }

        Sop.IFile Sop.ICollectionOnDisk.File
        {
            get { return File; }
            set { File = (File.IFile) value; }
        }

        public virtual void Load()
        {
            RealDictionary.Load();
        }

        public virtual void Close()
        {
            RealDictionary.Close();
        }

        public virtual void OnCommit()
        {
            RealDictionary.OnCommit();
        }

        public virtual void OnRollback()
        {
            RealDictionary.OnRollback();
        }

        public virtual bool MoveTo(long dataAddress)
        {
            return RealDictionary.MoveTo(dataAddress);
        }

        public virtual Transaction.ITransactionLogger Transaction
        {
            get { return RealDictionary.Transaction; }
            set { RealDictionary.Transaction = value; }
        }

        Sop.ITransaction Sop.ICollectionOnDisk.Transaction
        {
            get { return Transaction; }
            set { Transaction = (Transaction.ITransactionLogger) value; }
        }

        public virtual int OnMaxCapacity(int countOfBlocksUnloadToDisk)
        {
            return RealDictionary.OnMaxCapacity(countOfBlocksUnloadToDisk);
        }

        public virtual bool IsOpen
        {
            get { return RealDictionary.IsOpen; }
        }

        public virtual bool IsUnique
        {
            get { return RealDictionary.IsUnique; }
            set { RealDictionary.IsUnique = value; }
        }


        public virtual bool IsUnloading
        {
            get { return RealDictionary.IsUnloading; }
            set { RealDictionary.IsUnloading = value; }
        }

        public virtual bool IsCloned
        {
            get { return RealDictionary.IsCloned; }
            set { RealDictionary.IsCloned = value; }
        }

        public virtual FileStream FileStream
        {
            get { return RealDictionary.FileStream; }
            set { RealDictionary.FileStream = value; }
        }

        public virtual IMruManager MruManager
        {
            get { return RealDictionary.MruManager; }
            set { RealDictionary.MruManager = value; }
        }

        public virtual object CurrentEntry
        {
            get { return ((IBTreeBase) RealDictionary).CurrentEntry; }
        }

        public virtual long CurrentEntryDataAddress
        {
            get { return RealDictionary.CurrentEntryDataAddress; }
            set { RealDictionary.CurrentEntryDataAddress = value; }
        }

        public virtual DataBlockSize DataBlockSize
        {
            get { return RealDictionary.DataBlockSize; }
        }

        public virtual IInternalPersistent GetParent(Type parentType)
        {
            return RealDictionary.GetParent(parentType);
        }

        public virtual IInternalPersistent Parent
        {
            get { return RealDictionary.Parent; }
            set { RealDictionary.Parent = value; }
        }

        public virtual void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            RealDictionary.Pack(parent, writer);
        }

        public virtual void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            RealDictionary.Unpack(parent, reader);
        }

        public virtual Sop.DataBlock DiskBuffer
        {
            get { return RealDictionary.DiskBuffer; }
            set { RealDictionary.DiskBuffer = value; }
        }

        public virtual bool IsDirty
        {
            get { return RealDictionary.IsDirty; }
            set { RealDictionary.IsDirty = value; }
        }

        public virtual int HintSizeOnDisk
        {
            get { return RealDictionary.HintSizeOnDisk; }
        }

        public virtual void Open()
        {
            RealDictionary.Open();
        }

        public int InMemoryId
        {
            get { return RealDictionary.InMemoryId; }
        }

        public virtual void Flush()
        {
            RealDictionary.Flush();
        }

        public virtual bool MoveFirst()
        {
            return RealDictionary.MoveFirst();
        }

        public virtual bool MoveNext()
        {
            return RealDictionary.MoveNext();
        }

        public virtual bool MovePrevious()
        {
            return RealDictionary.MovePrevious();
        }

        public virtual bool MoveLast()
        {
            return RealDictionary.MoveLast();
        }

        public virtual void CopyTo(Array array, int index)
        {
            RealDictionary.CopyTo(array, index);
        }

        public virtual long Count
        {
            get { return RealDictionary.Count; }
        }
        int ICollection.Count
        {
            get { return (int)Count; }
        }

        public virtual bool IsSynchronized
        {
            get { return RealDictionary.IsSynchronized; }
        }

        public virtual object SyncRoot
        {
            get { return RealDictionary.SyncRoot; }
        }

        public virtual IEnumerator GetEnumerator()
        {
            return RealDictionary.GetEnumerator();
        }

        public virtual SortOrderType SortOrder
        {
            get { return RealDictionary.SortOrder; }
            set { RealDictionary.SortOrder = value; }
        }

        public virtual object CurrentKey
        {
            get { return RealDictionary.CurrentKey; }
        }

        public virtual object CurrentValue
        {
            get { return RealDictionary.CurrentValue; }
            set { RealDictionary.CurrentValue = value; }
        }

        public virtual bool EndOfTree()
        {
            return RealDictionary.EndOfTree();
        }

        public virtual bool Search(object key)
        {
            return RealDictionary.Search(key);
        }

        public virtual bool Search(object key, bool goToFirstInstance)
        {
            return RealDictionary.Search(key, goToFirstInstance);
        }

        DictionaryEntry IBTreeBase.CurrentEntry
        {
            get { return ((IBTreeBase) RealDictionary).CurrentEntry; }
        }

        public virtual void Add(object key, object value)
        {
            RealDictionary.Add(key, value);
        }
        public virtual bool AddIfNotExist(object key, object value)
        {
            return RealDictionary.AddIfNotExist(key, value);
        }

        public virtual void Clear()
        {
            RealDictionary.Clear();
        }

        public virtual bool Contains(object key)
        {
            return RealDictionary.Contains(key);
        }

        IDictionaryEnumerator IDictionary.GetEnumerator()
        {
            return ((IDictionary) RealDictionary).GetEnumerator();
        }

        public virtual bool IsFixedSize
        {
            get { return RealDictionary.IsFixedSize; }
        }

        public virtual bool IsReadOnly
        {
            get { return RealDictionary.IsReadOnly; }
        }

        public virtual ICollection Keys
        {
            get { return RealDictionary.Keys; }
        }

        public virtual void Remove(object key)
        {
            RealDictionary.Remove(key);
        }

        public virtual ICollection Values
        {
            get { return RealDictionary.Values; }
        }

        public virtual object this[object key]
        {
            get { return RealDictionary[key]; }
            set { RealDictionary[key] = value; }
        }

        public virtual object Clone()
        {
            return RealDictionary.Clone();
        }

        public virtual void Pack(System.IO.BinaryWriter writer)
        {
            RealDictionary.Pack(writer);
        }

        public virtual void Unpack(System.IO.BinaryReader reader)
        {
            RealDictionary.Unpack(reader);
        }

        public virtual bool IsDisposed
        {
            get { return RealDictionary == null || RealDictionary.IsDisposed; }
            set { RealDictionary.IsDisposed = value; }
        }

        public virtual void Dispose()
        {
            if (RealDictionary != null)
            {
                RealDictionary.Dispose();
                RealDictionary = null;
            }
        }

        public virtual bool HintSequentialRead
        {
            get { return RealDictionary.HintSequentialRead; }
            set { RealDictionary.HintSequentialRead = value; }
        }

        public virtual int HintBatchCount
        {
            get { return RealDictionary.HintBatchCount; }
            set { RealDictionary.HintBatchCount = value; }
        }

        protected Algorithm.SortedDictionary.ISortedDictionaryOnDisk RealDictionary;
    }
}