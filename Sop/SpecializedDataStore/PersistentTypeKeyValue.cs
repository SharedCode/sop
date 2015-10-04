// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.Persistence;

namespace Sop.SpecializedDataStore
{
    //** internal friend
    //[assembly:InternalsVisibleToAttribute("AssemblyB")] 

    /// <summary>
    /// Sorted Dictionary for:
    /// * IPersistent implementing Key POCO
    /// * IPersistent implementing Value POCO
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class PersistentTypeKeyValue<TKey, TValue> : SimpleKeyValue<TKey, TValue>
        where TKey : IPersistent, new()
        where TValue : IPersistent, new()
    {
        #region Constructors
        public PersistentTypeKeyValue()
        {
        }

        public PersistentTypeKeyValue(object container,
                                         IComparer<TKey> comparer, string name) :
                                             this(container, comparer, name, DataStoreType.SopOndisk)
        {
        }

        internal PersistentTypeKeyValue(object container,
                                           IComparer<TKey> comparer, string name, DataStoreType dataStoreType) :
                                               this(container, comparer, name, dataStoreType, null)
        {
        }

        internal PersistentTypeKeyValue(object container,
                                           IComparer<TKey> comparer, string name, DataStoreType dataStoreType,
                                           ISortedDictionaryOnDisk dataStore) :
                                               this(container, comparer, name, dataStoreType, dataStore, false)
        {
        }

        internal PersistentTypeKeyValue(object container,
                                           IComparer<TKey> comparer, string name, DataStoreType dataStoreType,
                                           ISortedDictionaryOnDisk dataStore, bool isDataInKeySegment) :
                                               base(
                                               container, comparer, name, dataStoreType, dataStore, isDataInKeySegment)
        {
        }
        #endregion

        /// <summary>
        /// Override SimpleKeyValue GetCollection to add support for IPersistent Key POCO (de)serialization.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"></param>
        /// <returns></returns>
        protected override ISortedDictionaryOnDisk GetCollection(
            ISortedDictionaryOnDisk container, GenericComparer<TKey> comparer, string name, bool isDataInKeySegment)
        {
            var o = (OnDisk.Algorithm.SortedDictionary.ISortedDictionaryOnDisk)
                base.GetCollection(container, comparer, name, isDataInKeySegment);
            o.OnKeyPack += new OnObjectPack(Collection_OnPack);
            o.OnKeyUnpack += new OnObjectUnpack(Collection_OnKeyUnpack);
            o.OnValuePack += new OnObjectPack(Collection_OnPack);
            o.OnValueUnpack += new OnObjectUnpack(Collection_OnValueUnpack);

            if (((SortedDictionaryOnDisk) o).BTreeAlgorithm.RootNeedsReload)
                ((SortedDictionaryOnDisk) o).ReloadRoot();
            return o;
        }

        protected void Collection_OnPack(System.IO.BinaryWriter writer, object objectToPack)
        {
            ((IPersistent)objectToPack).Pack(writer);
        }
        static internal object Collection_OnKeyUnpack(System.IO.BinaryReader reader)
        {
            TKey k = new TKey();
            ((TKey) k).Unpack(reader);
            return k;
        }
        internal static object Collection_OnValueUnpack(System.IO.BinaryReader reader)
        {
            var v = new TValue();
            ((IPersistent)v).Unpack(reader);
            return v;
        }
    }
}