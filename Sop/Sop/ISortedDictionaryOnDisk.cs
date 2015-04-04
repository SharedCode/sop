// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using Sop.Persistence;

namespace Sop
{
    /// <summary>
    /// Sorted Dictionary On Disk contains Sorted Dictionary like members plus
    /// additional disk I/O specific methods.
    /// </summary>
    public interface ISortedDictionaryOnDisk : ICollectionOnDisk,
                                               IBTreeBase, IDisposable,
                                               IWithHintBatchAction, IDeleteable
    {
        /// <summary>
        /// Get the Value given a Key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="target">Valid Target which will contain the data read from Disk. 
        /// NOTE: this should be a valid instance, argument null exception will be thrown if null is passed in</param>
        /// <returns>Target</returns>
        IPersistent GetValue(object key, IPersistent target);

        /// <summary>
        /// Get the Current Item's Value.
        /// NOTE: call one of the Move functions or the Search/Contains 
        /// function to position the Item pointer to the one you are interested
        /// about(Key) then call GetCurrentValue to get the Item Value
        /// </summary>
        /// <param name="target"></param>
        /// <returns></returns>
        IPersistent GetCurrentValue(IPersistent target);

        /// <summary>
        /// Comparer for use in comparing Item Keys of the Dictionary on disk
        /// </summary>
        System.Collections.IComparer Comparer { get; set; }

        /// <summary>
        /// Container dictionary
        /// </summary>
        ISortedDictionaryOnDisk Container { get; set; }

        /// <summary>
        /// Query the B-Tree for each Keys submitted, retrieve their values
        /// and store them in the array out parameter Values
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="values"></param>
        /// <returns>true if at least a key gets a match, otherwise false</returns>
        bool Query(QueryExpression[] keys, out QueryResult[] values);

        long CurrentSequence { get; set; }
        long DataAddress { get; }
        bool Update(object key, long itemAddress, object value);
        long GetId();
        long GetNextSequence();

        /// <summary>
        /// true means data is stored in key data segment, a.k.a. - clustered Index, otherwise in its own segment.
        /// NOTE: generates significantly smaller data file & with potential for a faster data retrieval, but as data becomes bigger, performance is traded off.
        /// </summary>
        bool IsDataInKeySegment { get; }

        void Remove();
        void Remove(object key, bool removeAllOccurence);
        bool Remove(QueryExpression[] keys, bool removeAllOccurence, out QueryResult[] results);
        void Rename(string newName);

        /// <summary>
        /// Auto Dispose Item of this Store when it gets removed from Cache or when it gets deleted.
        /// </summary>
        bool AutoDisposeItem { get; set; }

        /// <summary>
        /// Auto Dispose this Store when it gets removed from the container's cache.
        /// </summary>
        bool AutoDispose { get; set; }
    }
}

