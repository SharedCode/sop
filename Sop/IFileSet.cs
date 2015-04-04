// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;

namespace Sop
{
    /// <summary>
    /// File Set interface extends Client IFileSet to add common
    /// File management methods.
    /// </summary>
    public interface IFileSet : Client.IFileSet
    {
        /// <summary>
        /// Implement to add a given File to the set.
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        IFile Add(IFile f);

        /// <summary>
        /// Create a new File with a given name and filename
        /// and add this new File to the File set.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="filename"></param>
        /// <param name="profile">contains configuration data for the File being added.</param>
        /// <returns></returns>
        new IFile Add(string name, string filename = null, Profile profile = null);

        /// <summary>
        /// Check whether set contains File 'f'.
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        bool Contains(IFile f);

        /// <summary>
        /// Check whether set contains a File with a given name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        new bool Contains(string name);

        /// <summary>
        /// Implement to close File set.
        /// </summary>
        void Close();

        /// <summary>
        /// Implement to open the File Set.
        /// </summary>
        void Open();

        /// <summary>
        /// Implement to save all changes in set.
        /// </summary>
        void Flush();

        /// <summary>
        /// Implement to Get/Set File object to/from set.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        new IFile this[string name] { get; }
    }
}
