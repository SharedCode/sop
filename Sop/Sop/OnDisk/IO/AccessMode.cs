// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
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