// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)


namespace Sop.OnDisk.File
{
    /// <summary>
    /// Internal File Entity
    /// </summary>
    internal interface IInternalFileEntity
    {
        /// <summary>
        /// Close the File stream
        /// </summary>
        void CloseStream();

        /// <summary>
        /// Open the File stream
        /// </summary>
        void OpenStream();
    }
}