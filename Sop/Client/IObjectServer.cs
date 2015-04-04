using System;
using System.Collections.Generic;
using System.Text;

namespace Sop.Client
{
    /// <summary>
    /// Object Server interface.
    /// </summary>
    public interface IObjectServer : IDisposable
    {
        /// <summary>
        /// Encoding, defaults to UTF-8
        /// </summary>
        System.Text.Encoding Encoding { get; }

        /// <summary>
        /// Returns the Server's System Filename.
        /// </summary>
        string Filename { get; }
        /// <summary>
        /// Returns the Server's File Set.
        /// </summary>
        IFileSet FileSet { get; }
        /// <summary>
        /// Return the File with a given name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        IFile GetFile(string name);
        /// <summary>
        /// true if Server is in read-only mode.
        /// </summary>
        bool ReadOnly { get; }
        /// <summary>
        /// Returns the Server's Name.
        /// </summary>
        string Name { get; }
        /// <summary>
        /// Returns the Server's physical data file path.
        /// </summary>
        string Path { get; }
        /// <summary>
        /// Returns the Server's System File.
        /// </summary>
        IFile SystemFile { get; }
    }
}
