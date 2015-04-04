// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
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
            o.OnValuePack += Collection_OnPack;
            o.OnValueUnpack += Collection_OnUnpack;
            o.OnKeyPack += Collection_OnPack;
            o.OnKeyUnpack += Collection_OnUnpack;
            if (((SortedDictionaryOnDisk)o).BTreeAlgorithm.RootNeedsReload)
                ((SortedDictionaryOnDisk)o).ReloadRoot();
            return o;
        }

        static private void Collection_OnPack(System.IO.BinaryWriter writer, object objectToPack)
        {
            // write the object type info to the stream...
            Type t = objectToPack.GetType();
            string typeInfo = GetObjectTypeInfo(t);
            writer.Write(typeInfo);
            // serialize the object...
            var xmlSer = new XmlSerializer(t);
            ((OnDiskBinaryWriter) writer).WriteAsXml(xmlSer, objectToPack);
        }
        internal static object Collection_OnUnpack(System.IO.BinaryReader reader)
        {
            string typeInfo = reader.ReadString();
            Type t = Type.GetType(typeInfo);
            // serialize the object...
            var xmlSer = new XmlSerializer(t);
            return ((OnDiskBinaryReader)reader).ReadFromXml(xmlSer);
        }
    }
}
