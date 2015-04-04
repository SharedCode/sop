// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

namespace Sop.OnDisk.File
{
    /// <summary>
    /// File Entity Interface
    /// </summary>
    internal interface IFileEntity : IEntity
    {
        /// <summary>
        /// Open the File Entity
        /// </summary>
        void Open();

        /// <summary>
        /// Close the File Entity
        /// </summary>
        void Close();

        /// <summary>
        /// File Entity is Open(true) or not
        /// </summary>
        bool IsOpen { get; }

        /// <summary>
        /// Save this file entity
        /// </summary>
        void Flush();
    }
}