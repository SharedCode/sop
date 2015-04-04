namespace Sop.Collections.BTree
{
    /// <summary>
    /// Extends the Collections.BTreeICollection interface and adds Dictionary related api
    /// </summary>
    internal interface IDictionary : Collections.BTree.ICollection
    {
        /// <summary>
        /// Add Key and Value as entry to the Dictionary/Collection
        /// </summary>
        void Add(object key, object value);

        /// <summary>
        /// Returns the Current entry's key
        /// </summary>
        object CurrentKey { get; }

        /// <summary>
        /// Returns the Current entry's Value
        /// </summary>
        object CurrentValue { get; }
    }
}