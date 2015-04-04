// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Text;
using Sop.OnDisk.File;

namespace Sop
{
    /// <summary>
    /// Profile contains all variable settings user can tweak to change
    /// SOP performance and data allocations on disk.
    /// </summary>
    public class Profile : Preferences
    {
        public Profile(Profile profile) : this((Preferences)profile)
        {
            this.MaxInMemoryBlockCount = profile.MaxInMemoryBlockCount;
            this.CollectionSegmentSize = profile.CollectionSegmentSize;
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
            CollectionSegmentSize = CollectionSegmentSizeInKb * 1024;
            // compute MaxInMemoryBlockCount based on allocatable RAM as set by user...
            var memSize = Utility.Win32.GetMemorySize();
            float ml = MemoryLimitInPercent;
            if (ml > 75) ml = 75;   // max is 75% of RAM
            if (ml < 10) ml = 10;   // min is 10% of RAM
            var allocatableMemSize = (ulong)(memSize * (ml / 100));
            if (allocatableMemSize == 0)
                allocatableMemSize = (ulong)(memSize * .45);

            int allocMemDivideFactor = MaxCollectionCount * 512;
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

            CollectionGrowthSizeInNob = (int)(CollectionSegmentSize / (int)DataBlockSize);
            if (CollectionGrowthSizeInNob == 0)
                throw new InvalidOperationException("Computed CollectionGrowthSizeInNob can't be zero. Ensure CollectionSegmentSize > DataBlockSize.");
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
        /// This defaults to 
        /// </summary>
        public long CollectionSegmentSize { get; set; }

        /// <summary>
        /// Collection on disk growth size in number of blocks
        /// </summary>
        public int CollectionGrowthSizeInNob { get; private set; }

        /// <summary>
        /// MRU Minimum Capacity
        /// </summary>
        public int MruMinCapacity { get; set; }

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
        /// <summary>
        /// Maximum number of concurrent I/O threads allowed in Concurrent IO Pool Manager
        /// which is used by Data Manager during bulk I/O operations on times of page
        /// swap to disk cases.
        /// </summary>
        public static int MaxConcurrentIO
        {
            get { return Sop.OnDisk.IO.ConcurrentIOPoolManager.MaxConcurrentIO; }
            set { Sop.OnDisk.IO.ConcurrentIOPoolManager.MaxConcurrentIO = value; }
        }
    }
}