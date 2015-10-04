// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sop.OnDisk.ConcurrentWrappers
{
    /// <summary>
    /// Concurrent Collection On Disk.
    /// </summary>
    internal abstract class ConcurrentCollectionOnDisk<T> : Algorithm.Collection.ICollectionOnDisk, IDisposable
        where T : class, Algorithm.Collection.ICollectionOnDisk
    {
        protected ConcurrentCollectionOnDisk() { }

        public ConcurrentCollectionOnDisk(T collection)
        {
            Collection = collection;
        }
        public void RegisterChange(bool partialRegister = false)
        {
            Locker.Lock();
            Collection.RegisterChange(partialRegister);
            Locker.Unlock();
        }

        public void Dispose()
        {
            if (Collection == null) return;
            Locker.Lock();
            ((IDisposable)Collection).Dispose();
            var l = Locker;
            Collection = null;
            l.Unlock();
        }

        public bool IsValueInStream
        {
            get
            {
                Locker.Lock();
                bool r = Collection.IsValueInStream;
                Locker.Unlock();
                return r;
            }
        }

        public bool IsTransactionStore
        {
            get
            {
                Locker.Lock();
                bool r = Collection.IsTransactionStore;
                Locker.Unlock();
                return r;
            }
        }

        public System.IO.BinaryReader StreamReader
        {
            get
            {
                Locker.Lock();
                var r = Collection.StreamReader;
                Locker.Unlock();
                return r;
            }
        }

        public File.IFile File
        {
            get
            {
                Locker.Lock();
                var r = Collection.File;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.File = value;
                Locker.Unlock();
            }
        }

        public void Load()
        {
            Locker.Lock();
            Collection.Load();
            Locker.Unlock();
        }

        public void OnCommit()
        {
            Locker.Lock();
            Collection.OnCommit();
            Locker.Unlock();
        }

        public void OnRollback()
        {
            Locker.Lock();
            Collection.OnRollback();
            Locker.Unlock();
        }

        public bool MoveTo(long dataAddress)
        {
            Locker.Lock();
            bool r = MoveTo(dataAddress);
            Locker.Unlock();
            return r;
        }

        public Transaction.ITransactionLogger Transaction
        {
            get
            {
                Locker.Lock();
                var r = Collection.Transaction;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.Transaction = value;
                Locker.Unlock();
            }
        }

        public override string ToString()
        {
            return string.Format("ConcurrentCollectionOnDisk {0}", Collection.ToString());
        }

        public int OnMaxCapacity(int countOfBlocksUnloadToDisk)
        {
            Locker.Lock();
            var r = Collection.OnMaxCapacity(countOfBlocksUnloadToDisk);
            Locker.Unlock();
            return r;
        }

        public bool IsUnloading
        {
            get
            {
                Locker.Lock();
                var r = Collection.IsUnloading;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.IsUnloading = value;
                Locker.Unlock();
            }
        }

        public bool IsCloned
        {
            get
            {
                Locker.Lock();
                var r = Collection.IsCloned;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.IsCloned = value;
                Locker.Unlock();
            }
        }

        public File.FileStream FileStream
        {
            get
            {
                Locker.Lock();
                var r = Collection.FileStream;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.FileStream = value;
                Locker.Unlock();
            }
        }

        public object CurrentEntry
        {
            get
            {
                Locker.Lock();
                var r = Collection.CurrentEntry;
                Locker.Unlock();
                return r;
            }
        }

        public long CurrentEntryDataAddress
        {
            get
            {
                Locker.Lock();
                var r = Collection.CurrentEntryDataAddress;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.CurrentEntryDataAddress = value;
                Locker.Unlock();
            }
        }

        public Persistence.IInternalPersistent GetParent(Type parentType)
        {
            Locker.Lock();
            var r = Collection.GetParent(parentType);
            Locker.Unlock();
            return r;
        }

        public Persistence.IInternalPersistent Parent
        {
            get
            {
                Locker.Lock();
                var r = Collection.Parent;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.Parent = value;
                Locker.Unlock();
            }
        }

        public string Name
        {
            get
            {
                Locker.Lock();
                var r = Collection.Name;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.Name = value;
                Locker.Unlock();
            }
        }

        IFile ICollectionOnDisk.File
        {
            get
            {
                Locker.Lock();
                var r = Collection.File;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.File = (OnDisk.File.IFile)value;
                Locker.Unlock();
            }
        }

        public bool IsOpen
        {
            get
            {
                Locker.Lock();
                var r = Collection.IsOpen;
                Locker.Unlock();
                return r;
            }
        }

        public void Close()
        {
            Locker.Lock();
            Collection.Close();
            Locker.Unlock();
        }

        public int InMemoryId
        {
            get
            {
                Locker.Lock();
                var r = Collection.InMemoryId;
                Locker.Unlock();
                return r;
            }
        }

        ITransaction ICollectionOnDisk.Transaction
        {
            get
            {
                Locker.Lock();
                var r = Collection.Transaction;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.Transaction = (Sop.Transaction.ITransactionLogger)value;
                Locker.Unlock();
            }
        }

        public DataBlockSize DataBlockSize
        {
            get
            {
                Locker.Lock();
                var r = Collection.DataBlockSize;
                Locker.Unlock();
                return r;
            }
        }

        public void Pack(Persistence.IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            Locker.Lock();
            Collection.Pack(parent, writer);
            Locker.Unlock();
        }

        public void Unpack(Persistence.IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            Locker.Lock();
            Collection.Unpack(parent, reader);
            Locker.Unlock();
        }

        public Sop.DataBlock DiskBuffer
        {
            get
            {
                Locker.Lock();
                var r = Collection.DiskBuffer;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.DiskBuffer = value;
                Locker.Unlock();
            }
        }

        public bool IsDirty
        {
            get
            {
                Locker.Lock();
                var r = Collection.IsDirty;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.IsDirty = value;
                Locker.Unlock();
            }
        }

        public int HintSizeOnDisk
        {
            get
            {
                Locker.Lock();
                var r = Collection.HintSizeOnDisk;
                Locker.Unlock();
                return r;
            }
        }

        public void Open()
        {
            Locker.Lock();
            Collection.Open();
            Locker.Unlock();
        }

        public void Flush()
        {
            Locker.Lock();
            Collection.Flush();
            Locker.Unlock();
        }

        public bool MoveFirst()
        {
            Locker.Lock();
            var r = Collection.MoveFirst();
            Locker.Unlock();
            return r;
        }

        public bool MoveNext()
        {
            Locker.Lock();
            var r = Collection.MoveNext();
            Locker.Unlock();
            return r;
        }

        public bool MovePrevious()
        {
            Locker.Lock();
            var r = Collection.MovePrevious();
            Locker.Unlock();
            return r;
        }

        public bool MoveLast()
        {
            Locker.Lock();
            var r = Collection.MoveLast();
            Locker.Unlock();
            return r;
        }

        public void CopyTo(Array array, int index)
        {
            Locker.Lock();
            Collection.CopyTo(array, index);
            Locker.Unlock();
        }

        public long Count
        {
            get
            {
                return Collection.Count;
            }
        }
        int System.Collections.ICollection.Count
        {
            get
            {
                return (int)Count;
            }
        }

        public bool IsSynchronized
        {
            get
            {
                return true;
            }
        }

        public object SyncRoot
        {
            get
            {
                if (Collection == null) return null;
                return Collection.SyncRoot;
            }
        }

        public System.Collections.IEnumerator GetEnumerator()
        {
            Locker.Lock();
            var r = Collection.GetEnumerator();
            Locker.Unlock();
            return r;
        }

        public Mru.IMruManager MruManager
        {
            get
            {
                Locker.Lock();
                var r = Collection.MruManager;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.MruManager = value;
                Locker.Unlock();
            }
        }

        public Collections.Generic.ISortedDictionary<long, Sop.DataBlock> Blocks
        {
            get
            {
                Locker.Lock();
                var r = Collection.Blocks;
                Locker.Unlock();
                return r;
            }
            set
            {
                Locker.Lock();
                Collection.Blocks = value;
                Locker.Unlock();
            }
        }

        protected Collections.ISynchronizer Locker
        {
            get
            {
                if (Collection == null) return null;
                return (Collections.ISynchronizer)Collection.SyncRoot;
            }
        }
        internal protected T Collection;
    }
}
