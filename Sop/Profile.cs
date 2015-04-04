// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Text;
using Sop.OnDisk.File;
using Sop.SystemInterface;
using Sop.Persistence;

namespace Sop
{
    /// <summary>
    /// Profile contains all variable settings user can tweak to change
    /// SOP performance and data allocations on disk. A Profile can be
    /// set as an optional parameter when creating a new ObjectServer instance
    /// for use as the Server SystemFile's configuration.
    /// Also, it can be set as optional parameter when creating a new File.
    /// </summary>
    public class Profile : Preferences
    {
        public Profile(Profile profile) : this((Preferences)profile)
        {
            this.MaxInMemoryBlockCount = profile.MaxInMemoryBlockCount;
            this.StoreSegmentSize = profile.StoreSegmentSize;
            this.MruMinCapacity = profile.MruMinCapacity;
            this.MruMaxCapacity = profile.MruMaxCapacity;
        }
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="profileScheme"></param>
        /// <param name="dataBlockSize"></param>
        /// <param name="collectionSegmentSize"></param>
        /// <param name="maxInMemoryBlockCount"></param>
        public Profile(Preferences preferences = null) : base(preferences)
        {
            ComputeOtherSettings();
        }
        #region ctor utility
        private void ComputeOtherSettings()
        {
            StoreSegmentSize = StoreSegmentSizeInKb * 1024;
            // compute MaxInMemoryBlockCount based on allocatable RAM as set by user...
            ulong memSize = (ulong)SystemAdaptor.SystemInterface.GetMemorySize();
            float ml = MemoryLimitInPercent;
            if (ml > 75) ml = 75;   // max is 75% of RAM
            if (ml < 10) ml = 10;   // min is 10% of RAM
            var allocatableMemSize = (ulong)(memSize * (ml / 100));
            if (allocatableMemSize == 0)
                allocatableMemSize = (ulong)(memSize * .45);

            int allocMemDivideFactor = MaxStoreCount * (int)512;   // DataBlockSize.Minimum;
            MaxInMemoryBlockCount = (int)((float)allocatableMemSize / allocMemDivideFactor / (int)DataBlockSize); // / 8);

            // compute Min/Max BTree Node items based on allocatable RAM as set by user...
            const int BTreeNodeAvgBlockSize = 29;
            MruMaxCapacity = (int)(MaxInMemoryBlockCount / BTreeNodeAvgBlockSize) * 8;
            MruMinCapacity = (int)(MruMaxCapacity * .72);

            // handle minimum limits:
            if (MruMinCapacity < 15)
            {
                MruMinCapacity = 15;
                MruMaxCapacity = 20;
            }

            // compute Store pooling max count based on available memory
            if (MaxStoreInstancePoolCount == StoreFactory.DefaultMaxOpenStoreInMemoryCount)
                MaxStoreInstancePoolCount = (int)(((float)BytesToGB(memSize) / 4) * StoreFactory.DefaultMaxOpenStoreInMemoryCount * 2);

            if (TrackStoreTypes == null && BytesToGB(memSize) > 9)
                TrackStoreTypes = true;

            StoreGrowthSizeInNob = (int)(StoreSegmentSize / (int)DataBlockSize);
            if (StoreGrowthSizeInNob == 0)
                throw new InvalidOperationException("Computed StoreGrowthSizeInNob can't be zero. Ensure StoreSegmentSize > DataBlockSize.");
        }
        private int BytesToGB(ulong memSizeInBytes)
        {
            const ulong GBytes = 1000ul * 1024ul * 1024ul;
            int r = (int)(memSizeInBytes / GBytes);
            if (memSizeInBytes % GBytes != 0)
                r++;
            return r;
        }
        #endregion

        /// <summary>
        /// Data segment size on disk. Multiple POCOs can be stored
        /// in one segment, depending on POCO's serialized data size.
        /// </summary>
        internal long StoreSegmentSize { get; set; }

        /// <summary>
        /// Collection on disk growth size in number of blocks
        /// </summary>
        public int StoreGrowthSizeInNob { get; private set; }

        /// <summary>
        /// MRU Minimum Capacity
        /// </summary>
        public int MruMinCapacity { get; set; }

        /// <summary>
        /// BigDataBlockCount is a data block count threshold value 
        /// used to classify a Data Value whether it is considered 
        /// big data or not. A Store entry's data Value that is 
        /// considered Big Data is automatically set to null after 
        /// being returned by the CurrentValue property to 
        /// conserve memory.
        /// 
        /// User code doesn't get affected by this, CurrentValue 
        /// property still behaves the same, 'just the property gets 
        /// set to null internally after deserializing and returning 
        /// the Value to converve memory.
        /// 
        /// NOTE: this only applies if Store keeps the Data Value
        /// on a separate Data Segment than the Data Key.
        /// Store's IsDataInKeySegment is false.
        /// 
        /// Minimum this can be set is to 10, anything less will be 
        /// set to 10.
        /// </summary>
        public int BigDataBlockCount
        {
            get
            {
                return _bigDataBlockCount;
            }
            set
            {
                if (value < 10)
                    value = 10;
                _bigDataBlockCount = value;
            }
        }
        private int _bigDataBlockCount = 16;

        /// <summary>
        /// MRU Maximum Capacity
        /// </summary>
        public int MruMaxCapacity { get; set; }

        /// <summary>
        /// Maximum count of data blocks to hold in memory before triggering a flush to disk
        /// of SOP block buffers.
        /// </summary>
        public int MaxInMemoryBlockCount { get; set; }

        /// <summary>
        /// getter/setter for maximum number of opened file stream supported by SOP.
        /// </summary>
        public static int MaxOpenedFileStreamCount
        {
            get { return FileStream.MaxInstanceCount; }
            set { FileStream.MaxInstanceCount = value; }
        }

        ///// <summary>
        ///// Maximum number of concurrent I/O threads allowed in Concurrent IO Pool Manager
        ///// which is used by Data Manager during bulk I/O operations on times of page
        ///// swap to disk cases.
        ///// </summary>
        //public static int MaxConcurrentIO
        //{
        //    get { return Sop.OnDisk.IO.ConcurrentIOPoolManager.MaxConcurrentIO; }
        //    set { Sop.OnDisk.IO.ConcurrentIOPoolManager.MaxConcurrentIO = value; }
        //}

        internal override protected void Pack(System.IO.BinaryWriter writer)
        {
            base.Pack(writer);
            writer.Write(StoreSegmentSize);
            writer.Write(StoreGrowthSizeInNob);
            writer.Write(MruMinCapacity);
            writer.Write(MruMaxCapacity);
            writer.Write(MaxInMemoryBlockCount);
        }
        internal override protected void Unpack(System.IO.BinaryReader reader)
        {
            base.Unpack(reader);
            StoreSegmentSize = reader.ReadInt64();
            StoreGrowthSizeInNob = reader.ReadInt32();
            MruMinCapacity = reader.ReadInt32();
            MruMaxCapacity = reader.ReadInt32();
            MaxInMemoryBlockCount = reader.ReadInt32();
        }
    }
}
