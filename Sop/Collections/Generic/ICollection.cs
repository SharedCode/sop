namespace Sop.Collections.Generic
{
    /// <summary>
    /// In-Memory Collection interface.
    /// </summary>
    public interface ICollection<T> : IBaseCollection<T>
    {
        /// <summary>
        /// Returns the Current entry.
        /// </summary>
        T CurrentEntry { get; }

        /// <summary>
        /// Add entry to the Dictionary/Collection.
        /// </summary>
        void Add(T value);
    }
}
