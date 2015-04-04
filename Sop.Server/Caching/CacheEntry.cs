using System;
using System.Collections.Generic;
using System.Xml.Serialization;
using System.Runtime.Caching;

namespace Sop.Caching
{
    /// <summary>
    /// Cache Key, Value, Policy "structure".
    /// </summary>
    public class CacheKeyValue
    {
        /// <summary>
        /// Cache Key.
        /// </summary>
        public string Key;
        /// <summary>
        /// Cache Value.
        /// </summary>
        public object Value;
        /// <summary>
        /// Cache Policy.
        /// </summary>
        public CacheItemPolicy Policy;
        /// <summary>
        /// RegionName is not used/ignored in this SOP caching implementation.
        /// </summary>
        public string RegionName;
    }

    /// <summary>
    /// Cache Entry Key to the data Store.
    /// </summary>
    public class CacheKey : Sop.Persistence.Persistent
    {
        public CacheKey() { }
        public CacheKey(string key)
        {
            Key = key;
        }
        /// <summary>
        /// The actual Cache Key.
        /// </summary>
        public string Key;

        /// <summary>
        /// Contains DateTime Utc long int value that is used as key for the 
        /// Timestamp record of this CacheEntry in the Cache's Timestamp Store
        /// (StoreByDate data Store).
        /// 
        /// NOTE: this is not compared part of the Key, but is saved in the 
        /// Key Segment for performance. It's quite performant storing small
        /// amounts of data in the Key Segment, 'this prevents need to update/load
        /// unnecesarily the data Value residing in the Data Segment.
        /// Or using another store just for storing these related attribute(s).
        /// </summary>
        public long TimeStamp;

        public override void Pack(System.IO.BinaryWriter writer)
        {
            writer.Write(Key);
            writer.Write(TimeStamp);
        }

        public override void Unpack(System.IO.BinaryReader reader)
        {
            Key = reader.ReadString();
            TimeStamp = reader.ReadInt64();
        }
    }
    public class CacheKeyComparer : IComparer<CacheKey>
    {
        public int Compare(CacheKey x, CacheKey y)
        {
            return string.Compare(x.Key, y.Key);
        }
    }

    /// <summary>
    /// Cache Entry Base.
    /// </summary>
    public abstract class CacheEntryBase : Sop.Persistence.Persistent
    {
        public CacheEntryBase() { }
        public CacheEntryBase(CacheItemPolicy policy, long nowInTicks = 0)
        {
            if (policy.AbsoluteExpiration != ObjectCache.InfiniteAbsoluteExpiration &&
                policy.SlidingExpiration != ObjectCache.NoSlidingExpiration)
                throw new ArgumentException("Both AbsoluteExpiration and SlidingExpiration are set, a policy with only one of them specified is supported.");

            ExpirationTime = policy.AbsoluteExpiration.UtcTicks;
            SlidingExpiration = policy.SlidingExpiration.Ticks;
            if (policy.SlidingExpiration != ObjectCache.NoSlidingExpiration)
                ExpirationTime = nowInTicks == 0 ? DateTime.UtcNow.Add(policy.SlidingExpiration).Ticks :
                    nowInTicks + policy.SlidingExpiration.Ticks;
            Priority = policy.Priority;
        }
        public CacheEntryBase(CacheEntryBase source)
        {
            SlidingExpiration = source.SlidingExpiration;
            ExpirationTime = source.ExpirationTime;
            Priority = source.Priority;
        }
        public bool IsExpired(long now)
        {
            return now >= ExpirationTime;
        }

        /// <summary>
        /// Returns true if this Cache Entry does not expire,
        /// otherwise false.
        /// </summary>
        public bool NonExpiring
        {
            get
            {
                return Priority == CacheItemPriority.NotRemovable ||
                       ExpirationTime == DateTimeOffset.MaxValue.UtcTicks;
            }
        }

        /// <summary>
        /// Expiration Time in UtcTicks.
        /// </summary>
        public long ExpirationTime { get; set; }
        /// <summary>
        /// Sliding Expiration in Ticks.
        /// </summary>
        public long SlidingExpiration { get; set; }
        /// <summary>
        /// Cache Item Priority.
        /// </summary>
        public CacheItemPriority Priority { get; set; }

        public override void Pack(System.IO.BinaryWriter writer)
        {
            writer.Write(ExpirationTime);
            writer.Write(SlidingExpiration);
            writer.Write((byte)Priority);
        }

        public override void Unpack(System.IO.BinaryReader reader)
        {
            ExpirationTime = reader.ReadInt64();
            SlidingExpiration = reader.ReadInt64();
            Priority = (CacheItemPriority)reader.ReadByte();
        }
    }
    /// <summary>
    /// Cache Entry reference.
    /// </summary>
    public class CacheEntryReference : CacheEntryBase
    {
        public CacheEntryReference() { }
        public CacheEntryReference(CacheEntryBase other) : base(other) { }
        public CacheEntryReference(CacheItemPolicy policy) : base(policy) { }
        /// <summary>
        /// Cache Entry Key used for retrieving the actual Cache Entry
        /// from the Cache Store.
        /// </summary>
        public string CacheEntryKey;

        override public void Pack(System.IO.BinaryWriter writer)
        {
            base.Pack(writer);
            writer.Write(CacheEntryKey);
        }
        override public void Unpack(System.IO.BinaryReader reader)
        {
            base.Unpack(reader);
            CacheEntryKey = reader.ReadString();
        }
    }

    /// <summary>
    /// Cache Entry. Declared public so it can get Xml Serialized in SOP data store.
    /// </summary>
    public class CacheEntry : CacheEntryBase
    {
        public CacheEntry() { }
        public CacheEntry(CacheItemPolicy policy, long nowInTicks = 0) : base(policy, nowInTicks) { }
        /// <summary>
        /// Copy constructor.
        /// </summary>
        /// <param name="source"></param>
        public CacheEntry(CacheEntry source) : base(source)
        {
            Value = source.Value;
            SlidingExpiration = source.SlidingExpiration;
            ExpirationTime = source.ExpirationTime;
        }
        /// <summary>
        /// Value.
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// Convert this instance onto a CacheItem.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public CacheItem Convert(CacheKey key)
        {
            return new CacheItem(key.Key, Value);
        }
    }
}
