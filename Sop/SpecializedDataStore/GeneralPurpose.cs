// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Xml.Serialization;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.OnDisk.IO;

namespace Sop.SpecializedDataStore
{
    /// <summary>
    /// General Purpose Data Store.
    /// </summary>
    /// <typeparam name="TKey">Any of the supported simple types or any Xml Serializable type.</typeparam>
    /// <typeparam name="TValue">Any of the supported simple types or any Xml Serializable type.</typeparam>
    public class GeneralPurpose<TKey, TValue> : SimpleKeyValue<TKey, TValue>
    {
        #region Constructors
        /// <summary>
        /// Default constructor.
        /// </summary>
        public GeneralPurpose()
        {
        }
        /// <summary>
        /// Constructor expecting container instance and name of the Data Store.
        /// </summary>
        /// <param name="container">Container data store this data store instance is an item of.</param>
        /// <param name="name">Name of this data store.</param>
        public GeneralPurpose(object container, string name) :
            this(container, null, name)
        {
        }
        /// <summary>
        /// Constructor expecting container instance, a comparer and name of the Data Store.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        public GeneralPurpose(object container,
                                        IComparer<TKey> comparer, string name) :
                                            this(container, comparer, name, DataStoreType.SopOndisk)
        {
        }

        internal GeneralPurpose(object container,
                                          IComparer<TKey> comparer, string name, DataStoreType dataStoreType) :
                                              this(container, comparer, name, dataStoreType, null)
        {
        }

        internal GeneralPurpose(object container,
                                          IComparer<TKey> comparer, string name, DataStoreType dataStoreType,
                                          ISortedDictionaryOnDisk dataStore) :
                                              this(container, comparer, name, dataStoreType, dataStore, false)
        {
        }

        internal GeneralPurpose(object container,
                                          IComparer<TKey> comparer, string name, DataStoreType dataStoreType,
                                          ISortedDictionaryOnDisk dataStore, bool isDataInKeySegment) :
                                              base(
                                              container, comparer, name, dataStoreType, dataStore, isDataInKeySegment)
        {
        }
        #endregion

        /// <summary>
        /// Override SimpleKeyValue GetCollection to add support for key/value POCO Xml (de)serialization support.
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
            o.OnValuePack += Collection_XmlSerOnPack;
            o.OnValueUnpack += Collection_XmlSerOnUnpack;
            o.OnKeyPack += Collection_XmlSerOnPack;
            o.OnKeyUnpack += Collection_XmlSerOnUnpack;
            if (((SortedDictionaryOnDisk)o).BTreeAlgorithm.RootNeedsReload)
                ((SortedDictionaryOnDisk)o).ReloadRoot();
            return o;
        }
    }
}
