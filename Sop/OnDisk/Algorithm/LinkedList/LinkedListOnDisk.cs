// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Sop.Mru;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.IO;
using Sop.Persistence;

namespace Sop.OnDisk.Algorithm.LinkedList
{
    /// <summary>
    /// List On Disk stores and manages a list of objects on Disk
    /// </summary>
    internal class LinkedListOnDisk : CollectionOnDisk
    {
        internal LinkedListOnDisk()
        {
        }

        public LinkedListOnDisk(File.IFile container) : this(container, string.Empty)
        {
        }

        public LinkedListOnDisk(File.IFile container, string name,
                                params KeyValuePair<string, object>[] extraParams)
        {
            Initialize(container, name, extraParams);
        }

        protected internal override void Initialize(File.IFile file, params KeyValuePair<string, object>[] parameters)
        {
            base.Initialize(file, parameters);
            if (FirstItem == null)
                FirstItem = new LinkedItemOnDisk(file.DataBlockSize);
            if (CurrentItem == null)
                CurrentItem = FirstItem;
            if (LastItem == null)
                LastItem = new LinkedItemOnDisk(file.DataBlockSize);

            //** don't call open here, let caller code do it to ensure proper "state" when Open is called!!
        }

        internal LinkedItemOnDisk CurrentItem;
        internal LinkedItemOnDisk FirstItem;
        internal LinkedItemOnDisk LastItem;

        /// <summary>
        /// MoveNext makes the next entry the current one
        /// </summary>
        public override bool MoveNext()
        {
            if (CurrentItem == null || CurrentItem.NextItemAddress < 0)
                return false;
            bool r = this.DataBlockDriver.MoveTo(this, CurrentItem.NextItemAddress);
            if (!r) return false;
            currentEntry = null;
            object o = this.CurrentEntry;
            return true;
        }

        /// <summary>
        /// MovePrevious makes the previous entry the current one
        /// </summary>
        public override bool MovePrevious()
        {
            if (CurrentItem == null)
                return false;
            bool r = this.DataBlockDriver.MoveTo(this, CurrentItem.PreviousItemAddress);
            if (!r) return false;
            currentEntry = null;
            object o = this.CurrentEntry;
            return true;
        }

        /// <summary>
        /// MoveFirst makes the first entry in the Collection the current one
        /// </summary>
        public override bool MoveFirst()
        {
            if (Count > 0 && CurrentItem != null)
            {
                if (FirstItem.DiskBuffer.DataAddress != CurrentEntryDataAddress ||
                    FirstItem.DiskBuffer.DataAddress != CurrentItem.DiskBuffer.DataAddress)
                {
                    CurrentEntryDataAddress = -1;
                    this.DataBlockDriver.MoveTo(this, FirstItem.DiskBuffer.DataAddress);
                    Sop.DataBlock db = DataBlockDriver.ReadBlockFromDisk(this, FirstItem.DiskBuffer.DataAddress, false);
                    this.currentDataBlock = db;
                    CurrentItem = (LinkedItemOnDisk) ReadFromBlock(db);
                }
                return true;
            }
            return false;
        }

        public override bool MoveTo(long dataAddress)
        {
            if (dataAddress == CurrentEntryDataAddress)
                return true;
            CurrentEntryDataAddress = -1;
            return this.DataBlockDriver.MoveTo(this, dataAddress);
        }

        /// <summary>
        /// MoveLast makes the last entry in the Collection the current one
        /// </summary>
        public override bool MoveLast()
        {
            CurrentEntryDataAddress = -1;
            if (CurrentItem != null)
                return this.DataBlockDriver.MoveTo(this, LastItem.DiskBuffer.DataAddress);
            return false;
        }

        /// <summary>
        /// Returns the Current entry DeSerialized from File Stream.
        /// Will return:
        /// - byte[] if IInternalPersistent was saved
        /// - DeSerialized Value if object was Serialized
        /// </summary>
        public override object CurrentEntry
        {
            get
            {
                if (currentEntry == null)
                {
                    object v = this.ReadFromBlock(this.CurrentDataBlock);
                    if (v is LinkedItemOnDisk)
                    {
                        var o = (LinkedItemOnDisk) v;
                        if (o.DiskBuffer != CurrentDataBlock)
                            o.DiskBuffer = CurrentDataBlock;
                        this.CurrentItem = o;
                        currentEntry = o.Data;
                    }
                    else
                        return v;
                }
                return currentEntry;
            }
        }

        /// <summary>
        /// Update the data stored at a given Address.
        /// </summary>
        /// <param name="value"></param>
        public virtual void Update(object value)
        {
            //** Update the Current Entry w/ 'value'
            CurrentItem.Data = value;
            //** CurrentItem.DataAddress should be the same as DiskBuffer.DataAddress.
            WriteToDisk(CurrentItem, false);
            MruManager.Add(CurrentItem.DiskBuffer.DataAddress, CurrentItem);
            RegisterChange(true);
            //IsDirty = true;
        }

        /// <summary>
        /// Update the data stored at a given Address.
        /// </summary>
        /// <param name="dataAddress"></param>
        /// <param name="value"></param>
        public virtual void Update(long dataAddress, object value)
        {
            MoveTo(dataAddress);
            Update(value);
        }

        [ThreadStatic] private static int _sizeOf;
        [ThreadStatic] internal static System.Text.Encoding Encoding;

        internal static int SizeOfMetaData(IInternalPersistent parent)
        {
            if (parent == null)
                throw new ArgumentNullException("parent");
            if (!(parent is CollectionOnDisk))
                throw new ArgumentException("Parent is not CollectionOnDisk type.");

            if (Encoding != ((CollectionOnDisk) parent).File.Server.Encoding)
            {
                Encoding = ((CollectionOnDisk) parent).File.Server.Encoding;
                _sizeOf = 0;
            }
            if (_sizeOf == 0)
            {
                var o = new LinkedItemOnDisk
                            {
                                DiskBuffer = ((CollectionOnDisk) parent).DataBlockDriver.
                                    CreateBlock(((CollectionOnDisk) parent).File.DataBlockSize),
                                Data = new byte[0]
                            };
                var w = new OnDiskBinaryWriter(Encoding) {DataBlock = o.DiskBuffer};
                WritePersistentData(parent, o, w);
                _sizeOf = (int) w.BaseStream.Position;
                w.Close();
            }
            return _sizeOf;
        }

        /// <summary>
        /// Allows child class to purify Meta data from data
        /// </summary>
        /// <param name="biod"></param>
        /// <param name="db"></param>
        protected virtual void PurifyMeta(LinkedItemOnDisk biod, Sop.DataBlock db)
        {
        }

        /// <summary>
        /// Add 'Value' to the Collection
        /// </summary>
        public long Add(object value)
        {
            var o = new LinkedItemOnDisk(this.DataBlockSize) {Data = value};
            WriteToDisk(o, false);
            UpdateCount(UpdateCountType.Increment);
            //*** update Current, Last & First
            if (LastItem.DiskBuffer.DataAddress != -1)
            {
                o.PreviousItemAddress = LastItem.DiskBuffer.DataAddress;
                WriteToDisk(o, false);
                LinkedItemOnDisk biod = CurrentItem;
                biod.NextItemAddress = o.DiskBuffer.DataAddress;
                Sop.DataBlock db = DataBlockDriver.ReadBlockFromDisk(this, LastItem.DiskBuffer.DataAddress, true);
                if (CurrentItem.DiskBuffer.DataAddress != LastItem.DiskBuffer.DataAddress)
                {
                    biod = (LinkedItemOnDisk) ReadFromBlock(db);
                    biod.NextItemAddress = o.DiskBuffer.DataAddress;
                }
                else
                {
                    PurifyMeta(biod, db);
                    if (db.SizeOccupied > 0)
                        biod.DiskBuffer = db;
                }
                WriteToDisk(biod, false);
            }
            else
                FirstItem.DiskBuffer.DataAddress = o.DiskBuffer.DataAddress;

            currentEntry = null;
            CurrentItem = o;
            CurrentEntryDataAddress = o.DiskBuffer.DataAddress;
            currentDataBlock = o.DiskBuffer;
            LastItem.DiskBuffer.DataAddress = o.DiskBuffer.DataAddress;

            MruManager.Add(CurrentEntryDataAddress, o);

            //** update the header
            RegisterChange(true);
            //IsDirty = true;

            return o.DiskBuffer.DataAddress;
        }

        /// <summary>
        /// Remove current item
        /// </summary>
        public void Remove()
        {
            Sop.DataBlock currBlock = this.GetCurrentDataBlock(true);
            RemoveAt(currBlock.DataAddress, false);
        }

        /// <summary>
        /// Remove ObjectToRemove from the Collection if found, else throws an exception
        /// </summary>
        public void RemoveAt(long dataAddress)
        {
            RemoveAt(dataAddress, true);
        }

        //internal override bool RemoveInMemory(long DataAddress, Transaction.ITransactionLogger Transaction)
        //{
        //    if (LastItem != null)
        //        LastItem.Clear();
        //    if (FirstItem != null)
        //        FirstItem.Clear();
        //    CurrentItem = null;
        //    base.RemoveInMemory(DataAddress, Transaction);
        //    return true;
        //}
        private void RemoveAt(long dataAddress, bool willMove)
        {
            if (willMove && !MoveTo(dataAddress)) return;
            if (IsDeletedBlocksList && Count == 1)
                return;
            Sop.DataBlock currBlock = this.GetCurrentDataBlock(true);
            if (FirstItem.DiskBuffer.DataAddress == dataAddress)
            {
                MoveFirst();
                if (MoveNext())
                {
                    FirstItem.DiskBuffer.DataAddress = CurrentItem.DiskBuffer.DataAddress;
                    CurrentItem.PreviousItemAddress = -1;
                    Sop.DataBlock db = WriteToBlock(CurrentItem, CurrentItem.DiskBuffer);
                    DataBlockDriver.SetDiskBlock(this, db, false);
                }
                else
                {
                    long address = FirstItem.DiskBuffer.DataAddress;
                    FirstItem.DiskBuffer.DataAddress = LastItem.DiskBuffer.DataAddress = -1;
                    CurrentItem = FirstItem;
                    Sop.DataBlock db = WriteToBlock(CurrentItem, CurrentItem.DiskBuffer);
                    db.DataAddress = address;
                    DataBlockDriver.SetDiskBlock(this, db, false);
                    db.DataAddress = -1;
                }
            }
            else if (LastItem.DiskBuffer.DataAddress == dataAddress)
            {
                if (MovePrevious() || FirstItem.DiskBuffer.DataAddress == CurrentItem.DiskBuffer.DataAddress)
                {
                    LastItem.DiskBuffer.DataAddress = CurrentItem.DiskBuffer.DataAddress;
                    CurrentItem.NextItemAddress = -1;
                    Sop.DataBlock db = WriteToBlock(CurrentItem, CurrentItem.DiskBuffer);
                    DataBlockDriver.SetDiskBlock(this, db, false);
                }
                else
                    throw new InvalidOperationException("Can't go previous but First is not the only item.");
            }
            else
            {
                LinkedItemOnDisk curr = CurrentItem;
                LinkedItemOnDisk prev = null, next = null;
                if (MoveTo(curr.PreviousItemAddress))
                {
                    prev = CurrentItem;
                }
                if (MoveTo(curr.NextItemAddress))
                {
                    next = CurrentItem;
                }
                if (prev != null && next != null)
                {
                    prev.NextItemAddress = curr.NextItemAddress;
                    next.PreviousItemAddress = curr.PreviousItemAddress;
                    Sop.DataBlock db = WriteToBlock(prev, prev.DiskBuffer);
                    DataBlockDriver.SetDiskBlock(this, db, false);
                    db = WriteToBlock(next, next.DiskBuffer);
                    DataBlockDriver.SetDiskBlock(this, db, false);
                }
            }
            if (MruManager.Count > 0)
                MruManager.Remove(dataAddress, true);
            DataBlockDriver.Remove(this, currBlock);
        }

        /// <summary>
        /// Remove ObjectToRemove from the Collection if found, else throws an exception
        /// </summary>
        public void Remove(object item)
        {
            if (!Contains(item)) return;
            Sop.DataBlock currBlock = this.GetCurrentDataBlock(true);
            if (GetId(currBlock) >= 0)
                MruManager.Remove(GetId(currBlock), true);
            DataBlockDriver.Remove(this, currBlock);
        }

        public override void Pack(IInternalPersistent parent, BinaryWriter writer)
        {
            writer.Write(FirstItem.DiskBuffer.DataAddress);
            writer.Write(LastItem.DiskBuffer.DataAddress);
            base.Pack(parent, writer);
        }

        public override void Unpack(IInternalPersistent parent, BinaryReader reader)
        {
            if (CurrentItem == null)
            {
                FirstItem = new LinkedItemOnDisk(File.DataBlockSize);
                LastItem = new LinkedItemOnDisk(File.DataBlockSize);
            }
            CurrentItem = FirstItem;

            long firstItemDataAddress = reader.ReadInt64();
            long lastItemDataAddress = reader.ReadInt64();

            FirstItem.DiskBuffer.DataAddress = firstItemDataAddress;
            LastItem.DiskBuffer.DataAddress = lastItemDataAddress;
            base.Unpack(parent, reader);
        }

        /// <summary>
        /// Shallow copy the Collection into a new instance and return it.
        /// </summary>
        public virtual object Clone()
        {
            var lid = new LinkedListOnDisk
                          {
                              File = File,
                              DataBlockSize = DataBlockSize,
                              HintSizeOnDisk = HintSizeOnDisk,
                              currentEntry = currentEntry,
                              DataAddress = DataAddress,
                              MruManager = MruManager,
                              MruMinCapacity = MruMinCapacity,
                              MruMaxCapacity = MruMaxCapacity,
                              SyncRoot = this.SyncRoot,
                              DataBlockDriver = this.DataBlockDriver,
                              FirstItem = new LinkedItemOnDisk(File.DataBlockSize)
                          };
            lid.CurrentItem = lid.FirstItem;
            lid.LastItem = new LinkedItemOnDisk(File.DataBlockSize);
            int systemDetectedBlockSize;
            lid.FileStream = File.UnbufferedOpen(out systemDetectedBlockSize);
            lid.isOpen = true;
            lid.IsCloned = true;
            lid.Name = Name; // string.Format("{0} Clone", Name);
            lid.OnDiskBinaryWriter = new OnDiskBinaryWriter(File.Server.Encoding);
            lid.OnDiskBinaryReader = new OnDiskBinaryReader(File.Server.Encoding);
            return lid;
        }

        public override IEnumerator GetEnumerator()
        {
            var clone = (LinkedListOnDisk) Clone();
            return new ListOnDiskEnumerator(clone);
        }

        #region IList Members

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool IsFixedSize
        {
            get { return false; }
        }

        public bool Contains(object value)
        {
            return false;
        }

        #endregion

        internal class ListOnDiskEnumerator : System.Collections.IEnumerator
        {
            public ListOnDiskEnumerator(LinkedListOnDisk lid)
            {
                this._listOnDisk = lid;
                this.Reset();
            }

            public void Reset()
            {
                if (_listOnDisk.Count > 0)
                    this._currentIndex = 0;
                _bWasReset = true;
            }

            public object Current
            {
                get
                {
                    if (!_bWasReset)
                    {
                        if (_listOnDisk.Count > 0 && _currentIndex < _listOnDisk.Count)
                            return _listOnDisk.CurrentEntry;
                        return null;
                    }
                    throw new InvalidOperationException("Reset Error");
                }
            }

            public bool MoveNext()
            {
                if (!_bWasReset)
                {
                    if (_currentIndex < _listOnDisk.Count - 1)
                    {
                        _currentIndex++;
                        return true;
                    }
                    return false;
                }
                _bWasReset = false;
                return _listOnDisk.Count > 0;
            }

            private readonly LinkedListOnDisk _listOnDisk;
            private int _currentIndex = 0;
            private bool _bWasReset = true;
        }

        internal class LinkedItemOnDisk : ItemOnDisk
        {
            public LinkedItemOnDisk()
            {
            }

            public LinkedItemOnDisk(DataBlockSize dataBlockSize)
                : base(dataBlockSize)
            {
                DiskBuffer = new Sop.DataBlock(dataBlockSize);
            }

            public LinkedItemOnDisk Next
            {
                get { return null; }
            }

            public LinkedItemOnDisk Previous
            {
                get { return null; }
            }

            private long _nextItemAddress = -1;

            public long NextItemAddress
            {
                get { return _nextItemAddress; }
                set { _nextItemAddress = value; }
            }

            public long PreviousItemAddress = -1;

            public int DataSizeInStream = -1;
            public BinaryReader DataReader = null;

            public override void Pack(IInternalPersistent parent, BinaryWriter writer)
            {
                writer.Write(this.NextItemAddress);
                writer.Write(this.PreviousItemAddress);
                writer.Write(this.DiskBuffer.DataAddress);
                base.Pack(parent, writer);
            }

            public override void Unpack(IInternalPersistent parent,
                                        BinaryReader reader)
            {
                if (DiskBuffer == null)
                {
                    Sop.DataBlock db = ((OnDiskBinaryReader)reader).DataBlock;
                    DiskBuffer = db;
                }
                NextItemAddress = reader.ReadInt64();
                PreviousItemAddress = reader.ReadInt64();
                long l = reader.ReadInt64();
                if (DiskBuffer == null || (l > -1 && DiskBuffer.DataAddress != l))
                {
                    DiskBuffer = ((CollectionOnDisk) parent).DataBlockDriver.
                        CreateBlock(((CollectionOnDisk) parent).DataBlockSize);
                    this.DiskBuffer.DataAddress = l;
                }
                DiskBuffer.IsDirty = false;
                base.Unpack(parent, reader);
            }
        }

        public override void Close()
        {
            if (!IsOpen) return;
            base.Close();
            if (OnDiskBinaryWriter != null)
            {
                OnDiskBinaryWriter.Close();
                OnDiskBinaryWriter = null;
            }
            if (OnDiskBinaryReader != null)
            {
                OnDiskBinaryReader.Close();
                OnDiskBinaryReader = null;
            }

            FirstItem = new LinkedItemOnDisk(File.DataBlockSize);
            this.CurrentItem = FirstItem;
            LastItem = new LinkedItemOnDisk(File.DataBlockSize);
        }

        public override void Clear()
        {
            if (Count <= 0) return;
            FirstItem = new LinkedItemOnDisk(File.DataBlockSize);
            this.CurrentItem = FirstItem;
            LastItem = new LinkedItemOnDisk(File.DataBlockSize);
            base.Clear();
        }
    }
}