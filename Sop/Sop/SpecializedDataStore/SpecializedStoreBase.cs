// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using Sop.Collections.BTree;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.OnDisk.File;

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
            return string.Format("{0}\\{1}", containerStoreName, storeName);
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
        public ISynchronizer Locker
        {
            get { return (ISynchronizer) Collection.SyncRoot; }
        }

        internal bool InvokeFromMru { get; set; }
    }
}
