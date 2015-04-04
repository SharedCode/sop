// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sop
{
    /// <summary>
    /// Use this to specify commonly settings to customize
    /// SOP behavior per performance and RAM/disk data allocations.
    /// </summary>
    public class Preferences
    {
        public Preferences()
        {
            CollectionSegmentSizeInKb = 512;
            DataBlockSize = Sop.DataBlockSize.FiveTwelve;
            // default mem use limit
            MemoryLimitInPercent = 72;
            BTreeSlotLength = 200;
            // 10 Collections on disk (data stores).
            MaxCollectionCount = 10;
        }
        public Preferences(Preferences pref) : this()
        {
            if (pref != null)
            {
                CollectionSegmentSizeInKb = pref.CollectionSegmentSizeInKb;
                DataBlockSize = pref.DataBlockSize;
                MemoryLimitInPercent = pref.MemoryLimitInPercent;
                BTreeSlotLength = pref.BTreeSlotLength;
                MaxCollectionCount = pref.MaxCollectionCount;
                MemoryExtenderMode = pref.MemoryExtenderMode;
                TrackStoreTypes = pref.TrackStoreTypes;
                TrashBinType = pref.TrashBinType;
                IsDataInKeySegment = pref.IsDataInKeySegment;
                Encoding = pref.Encoding;
            }
        }
        /// <summary>
        /// Set this hint higher to reduce amount of memory SOP will use or set this
        /// smaller to increase memory use. More memory available to SOP will mean faster
        /// performance as more data objects will tend to be kept/stay accessible from memory,
        /// reduced page swap between disk and memory.
        /// </summary>
        public int MaxCollectionCount { get; set; }
        /// <summary>
        /// Data segment size on disk. Multiple POCOs can be stored
        /// in one segment, depending on POCO's serialized data size.
        /// This defaults to 
        /// </summary>
        public long CollectionSegmentSizeInKb { get; set; }

        /// <summary>
        /// Data Block size
        /// </summary>
        public DataBlockSize DataBlockSize { get; set; }

        /// <summary>
        /// % RAM to use, leave 0 to let SOP manage mem-use.
        /// </summary>
        public ushort MemoryLimitInPercent { get; set; }

        /// <summary>
        /// Maximum number of Opened Data Stores in-memory.
        /// NOTE: When total number of opened and in-memory Stores 
        /// reach this amount, StoreFactory will start to auto-dispose
        /// least used opened Stores in-memory in order to maintain 
        /// memory/resource consumption within reasonable levels.
        /// </summary>
        public static int MaxStoreInstancePoolCount
        {
            get { return StoreFactory.MaxStoreInstancePoolCount; }
            set { StoreFactory.MaxStoreInstancePoolCount = value; }
        }

        /// <summary>
        /// true will use disk to extend memory. Each new run will cleanup (delete!)
        /// previous run's data file.
        /// 
        /// This mode is useful in some scenarios where application would like to
        /// extend memory capacity and data persistence is not important.
        /// </summary>
        public bool MemoryExtenderMode { get; set; }

        /// <summary>
        /// ObjectServer Character Encoding. Defaults to UTF-8.
        /// NOTE: once Encoding is defined, it can't be changed. Encoding is typically set 
        /// during creation of ObjectServer, once created, it can't be updated to another encoding.
        /// </summary>
        public Encoding Encoding
        {
            get { return _encoding; }
            set { _encoding = value; }
        }
        private Encoding _encoding = Encoding.UTF8;

        /// <summary>
        /// BTree Slot Length. Typical values are 12, 24, 48
        /// </summary>
        public short BTreeSlotLength { get; set; }

        /// <summary>
        /// Default value for the File's Collection's IsDataInKeySegment attribute.
        /// true means data is saved in Key segment, otherwise is saved on its own data segment.
        /// NOTE: this contains default value, your code can define a different IsDataInKeySegment
        /// value while retrieving data stores using StoreFactory.
        /// </summary>
        public bool IsDataInKeySegment { get; set; }

        /// <summary>
        /// true will log user Store information as StoreFactory.Getxxx method is invoked.
        /// ObjectServer.StoreTypes table contains these information.
        /// </summary>
        public bool? TrackStoreTypes { get; set; }
        /// <summary>
        /// Trash Bin Type defaults to one bin per Collection.
        /// </summary>
        public TrashBinType TrashBinType { get; set; }
    }
}
