using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sop
{
    /// <summary>
    /// Store Parameters.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public class StoreParameters<TKey>
    {
        /// <summary>
        /// Key Comparer to use by the Store when building up and searching entries
        /// of the B-Tree.
        /// </summary>
        public IComparer<TKey> StoreKeyComparer;
        /// <summary>
        /// true (default) will create the Store if not found, false will not and return null
        /// when retrieving the Store via the StoreFactory or StoreNavigator.
        /// </summary>
        public bool CreateStoreIfNotExist = true;
        /// <summary>
        /// true (default) means data Store will put the entry's Value data in the Key Segment
        /// where the Nodes and Items of the B-Tree are stored. Default is true.
        /// Guideline: if Value data is fairly small (e.g. - around 2KB more or less),
        /// it is recommended to store it in the Key segment. However, when Value data
        /// is somewhat bigger, it is recommended to store it in the Data Segment,
        /// set this to false in this case.
        /// </summary>
        public bool IsDataInKeySegment = true;
        /// <summary>
        /// true (default) means the Store is managed in the container Store's MRU cache.
        /// This means the Store can be automatically disposed and offloaded from
        /// memory during "MRU is full" scenario. Caller code doesn't manage the Store
        /// just simply let it get out of scope and assured that the Store's container
        /// will manage its lifetime.
        /// </summary>
        public bool MruManaged = true;
        /// <summary>
        /// true will mark this Store to contain unique entries only. Each insert
        /// of a Key/Value pair the key will be checked for uniqueness.
        /// Default is false.
        /// </summary>
        public bool IsUnique;
        /// <summary>
        /// Set the Store to AutoFlush so inserted Blob data will get mapped to disk right away 
        /// and not buffered in Store's MRU cache (a.k.a. - streaming).
        /// Currently, only applicable if data Value is stored in Data Segment (IsDataInKeySegment = false).
        /// Default is false.
        /// </summary>
        public bool AutoFlush;
    }
}
