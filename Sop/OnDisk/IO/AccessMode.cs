// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

namespace Sop.OnDisk.IO
{
    /// <summary>
    /// Access Modes
    /// </summary>
    internal enum AccessMode
    {
        /// <summary>
        /// Read Only
        /// </summary>
        ReadOnly = System.IO.FileAccess.Read,

        /// <summary>
        /// Read Write
        /// </summary>
        ReadWrite = System.IO.FileAccess.ReadWrite
    }
}