using System;

namespace Sop.Collections.Generic
{
    /// <summary>
    /// In-Memory Base Collection
    /// </summary>
    public interface IBaseCollection<T> : ICloneable
    {
        /// <summary>
        /// MoveNext makes the next entry current.
        /// </summary>
        bool MoveNext();

        /// <summary>
        /// MovePrevious makes the previous entry current.
        /// </summary>
        bool MovePrevious();

        /// <summary>
        /// MoveFirst makes the first entry in the Collection current.
        /// </summary>
        bool MoveFirst();

        /// <summary>
        /// MoveLast makes the last entry in the Collection current.
        /// </summary>
        bool MoveLast();

        /// <summary>
        /// Search the Collection for existence of entry with a given key.
        /// </summary>
        /// <param name="key">key to search for.</param>
        /// <returns>true if found, false otherwise.</returns>
        bool Search(T key);
    }
}
