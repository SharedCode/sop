// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using Sop.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sop
{
    /// <summary>
    /// Use this to specify commonly used settings to customize
    /// SOP behavior per performance and RAM/disk data allocations.
    /// </summary>
    public class Preferences : IPersistent
    {
        public Preferences()
        {
            StoreSegmentSizeInKb = 512;
            DataBlockSize = Sop.DataBlockSize.Minimum;
            // default mem use limit
            MemoryLimitInPercent = 40;
            BTreeSlotLength = 150;
            // 10 Collections on disk (data stores).
            MaxStoreCount = 10;
        }
        public Preferences(Preferences pref) : this()
        {
            if (pref != null)
            {
                StoreSegmentSizeInKb = pref.StoreSegmentSizeInKb;
                DataBlockSize = pref.DataBlockSize;
                MemoryLimitInPercent = pref.MemoryLimitInPercent;
                BTreeSlotLength = pref.BTreeSlotLength;
                MaxStoreCount = pref.MaxStoreCount;
                MemoryExtenderMode = pref.MemoryExtenderMode;
                TrackStoreTypes = pref.TrackStoreTypes;
                TrashBinType = pref.TrashBinType;
                IsDataInKeySegment = pref.IsDataInKeySegment;
                Encoding = pref.Encoding;
            }
        }
        /// <summary>
        /// (hint) Maximum number of Stores (Collections on disk).
        /// Defaults to 10, specify a value if maximum number of Stores to keep
        /// in the ObjectServer is known. This value helps SOP decide how much
        /// memory buffers to allocate and how many MRU Stores get cached in-memory.
        /// 
        /// Set this hint higher to reduce amount of memory buffers SOP will use or vice versa.
        /// More memory available to SOP will mean faster
        /// performance as more data objects will tend to be kept in-memory,
        /// resulting in reduced page swapping between memory and disk.
        /// </summary>
        public int MaxStoreCount { get; set; }

        /// <summary>
        /// Data segment size on disk. Multiple POCOs can be stored
        /// in one segment, depending on POCO's serialized data size.
        /// This defaults to 
        /// </summary>
        public long StoreSegmentSizeInKb { get; set; }

        /// <summary>
        /// Data Block size
        /// </summary>
        internal DataBlockSize DataBlockSize { get; set; }

        /// <summary>
        /// % RAM to use, leave 0 to let SOP manage mem-use.
        /// In current version, leaving this to 0 will use default
        /// Memory limit of 40%.
        /// </summary>
        public short MemoryLimitInPercent { get; set; }

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
        /// Defaults to false.
        /// </summary>
        public bool MemoryExtenderMode { get; set; }

        /// <summary>
        /// ObjectServer Character Encoding. Defaults to UTF-8.
        /// NOTE: once Encoding is defined, it can't be changed. Encoding is typically set 
        /// during creation of ObjectServer, once created, it can't be updated to another encoding.
        /// 
        /// Encoding object is not persisted, your code has to set it explicitly or use the default.
        /// </summary>
        public Encoding Encoding
        {
            get { return _encoding; }
            set { _encoding = value; }
        }
        private Encoding _encoding = Sop.SystemInterface.SystemAdaptor.SystemInterface.DefaultEncoding;

        /// <summary>
        /// BTree Slot Length. Defaults to 100.
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

        virtual internal protected void Pack(System.IO.BinaryWriter writer)
        {
            writer.Write(this.BTreeSlotLength);
            writer.Write(this.StoreSegmentSizeInKb);
            //writer.Write((int)this.DataBlockSize);
            writer.Write(this.IsDataInKeySegment);
            writer.Write(this.MaxStoreCount);
            writer.Write(this.MemoryExtenderMode);
            writer.Write(this.MemoryLimitInPercent);
            writer.Write((byte) (TrackStoreTypes == null ? 0 : (TrackStoreTypes.Value ? 1 : 2)));
            writer.Write((byte)this.TrashBinType);
        }

        virtual internal protected void Unpack(System.IO.BinaryReader reader)
        {
            BTreeSlotLength = reader.ReadInt16();
            StoreSegmentSizeInKb = reader.ReadInt64();
            //writer.Write((int)this.DataBlockSize);
            IsDataInKeySegment = reader.ReadBoolean();
            MaxStoreCount = reader.ReadInt32();
            MemoryExtenderMode = reader.ReadBoolean();
            MemoryLimitInPercent = reader.ReadInt16();
            var tst = reader.ReadByte();
            if (tst == 1)
                TrackStoreTypes = true;
            else if (tst == 2)
                TrackStoreTypes = false;
            else
                TrackStoreTypes = null;
            TrashBinType = (TrashBinType)reader.ReadByte();
        }
        void IPersistent.Pack(System.IO.BinaryWriter writer)
        {
            Pack(writer);
        }
        void IPersistent.Unpack(System.IO.BinaryReader reader)
        {
            Unpack(reader);
        }
        bool IPersistent.IsDisposed { get; set; }
        int Sop.IWithHintSize.HintSizeOnDisk { get { return 0; } }
    }
}
