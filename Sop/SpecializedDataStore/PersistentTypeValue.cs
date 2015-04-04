// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using Sop.Persistence;

namespace Sop.SpecializedDataStore
{
    /// <summary>
    /// Sorted Dictionary for:
    /// * Simple Key
    /// * IPersistent derived Value POCOs
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class PersistentTypeValueSimpleKey<TKey, TValue> : SimpleKeyValue<TKey, TValue>
        where TValue : IPersistent, new()
    {
        #region Constructors
        public PersistentTypeValueSimpleKey()
        {
        }

        public PersistentTypeValueSimpleKey(
            object container, string came) :
                this(container, null, came)
        {
        }

        public PersistentTypeValueSimpleKey(
            object container, IComparer<TKey> comparer, string came) :
                this(container, comparer, came, DataStoreType.SopOndisk)
        {
        }

        internal PersistentTypeValueSimpleKey(object container,
                                              IComparer<TKey> comparer, string came, DataStoreType dataStoreType) :
                                                  this(container, comparer, came, dataStoreType, null)
        {
        }

        internal PersistentTypeValueSimpleKey(object container,
                                              IComparer<TKey> comparer, string came, DataStoreType dataStoreType,
                                              ISortedDictionaryOnDisk dataStore) :
                                                  this(container, comparer, came, dataStoreType, dataStore, false)
        {
        }

        internal PersistentTypeValueSimpleKey(object container,
                                              IComparer<TKey> comparer, string came, DataStoreType dataStoreType,
                                              ISortedDictionaryOnDisk dataStore, bool isDataInKeySegment) :
                                                  base(
                                                  container, comparer, came, dataStoreType, dataStore,
                                                  isDataInKeySegment)
        {
        }
        #endregion
        /// <summary>
        /// Override SimpleKeyValue GetCollection to add support for IPersistent Value POCO (de)serialization.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="came"></param>
        /// <param name="isDataInKeySegment"></param>
        /// <returns></returns>
        protected override ISortedDictionaryOnDisk GetCollection(
            ISortedDictionaryOnDisk container, GenericComparer<TKey> comparer, string came, bool isDataInKeySegment)
        {
            var o = (OnDisk.Algorithm.SortedDictionary.ISortedDictionaryOnDisk)
                base.GetCollection(container, comparer, came, isDataInKeySegment);

            // pack/unpack for Xml Serializable objects...
            o.OnKeyPack += Collection_XmlSerOnPack;
            o.OnKeyUnpack += Collection_XmlSerOnUnpack;

            // pack/unpack for IPersistent implementing objects...
            o.OnValuePack += new OnObjectPack(Collection_OnPack);
            o.OnValueUnpack += new OnObjectUnpack(Collection_OnValueUnpack);
            return o;
        }

        #region Object Pack event handlers

        internal static object Collection_OnValueUnpack(System.IO.BinaryReader reader)
        {
            var v = new TValue();
            ((IPersistent) v).Unpack(reader);
            return v;
        }

        protected void Collection_OnPack(System.IO.BinaryWriter writer, object objectToPack)
        {
            ((IPersistent) objectToPack).Pack(writer);
        }

        #endregion
    }
}