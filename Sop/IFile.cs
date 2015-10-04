// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;

namespace Sop
{
    /// <summary>
    /// SOP File interface extends the Client IFile adding basic I/O methods.
    /// </summary>
    public interface IFile : Client.IFile
    {
        /// <summary>
        /// Implement to open the File.
        /// </summary>
        void Open();

        /// <summary>
        /// Implement to close the File.
        /// </summary>
        void Close();

        /// <summary>
        /// Implement to save to disk all modified POCOs (in-memory) of each Store of the File.
        /// </summary>
        void Flush();
    }
}
