using System;
using System.Collections.Generic;
using System.Text;

namespace Sop.Client
{
    /// <summary>
    /// File Set interface provides API for storing and managing
    /// File objects in the set.
    /// </summary>
    public interface IFileSet : IDisposable
    {
        /// <summary>
        /// true will auto dispose File from memory when it gets removed from MRU cache or
        /// when it gets removed from FileSet. false otherwise.
        /// </summary>
        bool AutoDisposeItem { get; set; }

        /// <summary>
        /// Add File to the set.
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        IFile Add(IFile f);

        /// <summary>
        /// Add File with Name and Filename to the set.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="filename"></param>
        /// <returns></returns>
        IFile Add(string name, string filename = null);

        /// <summary>
        /// Check whether the set contains File 'f'.
        /// </summary>
        /// <param name="f"></param>
        /// <returns>true if f is found, false otherwise.</returns>
        bool Contains(IFile f);

        /// <summary>
        /// Check whether the file set contains file with a given name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        bool Contains(string name);

        /// <summary>
        /// Returns count of File objects in the set.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Returns File object enumerator.
        /// </summary>
        /// <returns></returns>
        System.Collections.IEnumerator GetEnumerator();

        /// <summary>
        /// Returns list of names of File objects in the set.
        /// </summary>
        /// <returns></returns>
        string[] GetNames();

        /// <summary>
        /// Remove File with a given name from the set.
        /// </summary>
        /// <param name="name"></param>
        bool Remove(string name);

        /// <summary>
        /// Get/Set File object to/from set.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        IFile this[string name] { get; }
    }
}
