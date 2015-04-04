// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using Sop.Persistence;

namespace Sop.OnDisk.File
{
    /// <summary>
    /// File Set interface
    /// </summary>
    internal interface IFileSet : Sop.IFileSet, IInternalPersistent
    {
        /// <summary>
        /// Add File to the set
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
        new IFile Add(string name, string filename = null, Profile profile = null);

        /// <summary>
        /// Check whether set contains File "F"
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        bool Contains(IFile f);

        /// <summary>
        /// Mark File set not dirty
        /// </summary>
        void MarkNotDirty();

        /// <summary>
        /// Get/Set File object to/from set
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        new IFile this[string name] { get; }
    }
}