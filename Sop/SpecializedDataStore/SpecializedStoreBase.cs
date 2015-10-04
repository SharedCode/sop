// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Xml.Serialization;
using Sop.Collections.BTree;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.OnDisk.File;
using Sop.OnDisk.IO;

namespace Sop.SpecializedDataStore
{
    /// <summary>
    /// Specialized Store Base.
    /// </summary>
    public abstract class SpecializedStoreBase
    {
        /// <summary>
        /// Returns the internal SortedDictionaryOnDisk type collection that
        /// actually manages storage/retrieval of data on disk.
        /// </summary>
        public ISortedDictionaryOnDisk Collection
        {
            get { return _collection; }
            set
            {
                if (value == null && _collection != null)
                    _collection.Container = null;
                _collection = value;
            }
        }
        private ISortedDictionaryOnDisk _collection;

        /// <summary>
        /// Format Store name using SOP standard method of concatenating
        /// container with target store's name.
        /// </summary>
        /// <param name="containerStoreName"> </param>
        /// <param name="storeName"></param>
        /// <returns></returns>
        internal static string FormatStoreName(string containerStoreName, string storeName)
        {
            return string.Format("{0}{2}{1}", containerStoreName, storeName, System.IO.Path.DirectorySeparatorChar);
        }

        /// <summary>
        /// For internal use only, returns debugging useful information.
        /// </summary>
        /// <returns></returns>
        public string GetHeaderInfo()
        {
            return ((SortedDictionaryOnDisk)Collection).GetHeaderInfo();
        }


        /// <summary>
        /// Returns a globally unique name, e.g. Filename suffix with the Store name.
        /// </summary>
        /// <returns></returns>
        public string UniqueName
        {
            get { return Collection == null ? base.ToString() : Collection.ToString(); }
        }

        /// <summary>
        /// Locker object provides monitor type(enter/exit) of access locking to the Store.
        /// </summary>
        public Collections.ISynchronizer Locker
        {
            get 
            {
                if (_locker == null)
                    _locker = (Collections.ISynchronizer)Collection.SyncRoot; ;
                return _locker;
            }
        }
        private Collections.ISynchronizer _locker;

        internal bool InvokeFromMru { get; set; }

        #region Get Object's Type info for version independent serialization
        static protected string GetObjectTypeInfo(Type t)
        {
            string assemblyName = t.Assembly.FullName;
            // Strip off the version and culture info 
            assemblyName = assemblyName.Substring(0, assemblyName.IndexOf(",")).Trim();
            string typeName = t.FullName + ", " + assemblyName;
            return CutOutVersionNumbers(typeName);
        }
        private static string CutOutVersionNumbers(string fullTypeName)
        {
            string shortTypeName = fullTypeName;
            var versionIndex = shortTypeName.IndexOf("Version");
            while (versionIndex != -1)
            {
                int commaIndex = shortTypeName.IndexOf(",", versionIndex);
                shortTypeName = shortTypeName.Remove(versionIndex, commaIndex - versionIndex + 1);
                versionIndex = shortTypeName.IndexOf("Version");
            }
            return shortTypeName;
        }
        #endregion

        #region Pack/Unpack using Xml Serialization
        /// <summary>
        /// Xml Serialization object packager.
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="objectToPack"></param>
        static public void Collection_XmlSerOnPack(System.IO.BinaryWriter writer, object objectToPack)
        {
            // write the object type info to the stream...
            Type t = objectToPack.GetType();
            string typeInfo = GetObjectTypeInfo(t);
            writer.Write(typeInfo);
            // serialize the object...
            var xmlSer = new XmlSerializer(t);
            ((OnDiskBinaryWriter)writer).WriteAsXml(xmlSer, objectToPack);
        }
        /// <summary>
        /// Xml Serialization object unpackager.
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static object Collection_XmlSerOnUnpack(System.IO.BinaryReader reader)
        {
            string typeInfo = reader.ReadString();
            Type t = Type.GetType(typeInfo);
            // serialize the object...
            var xmlSer = new XmlSerializer(t);
            return ((OnDiskBinaryReader)reader).ReadFromXml(xmlSer);
        }
        #endregion
    }
}
