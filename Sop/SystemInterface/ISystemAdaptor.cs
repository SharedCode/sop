using System.IO;
using Sop.OnDisk.File;

namespace Sop.SystemInterface
{
    /// <summary>
    /// Implement System Adaptor interface to integrate SOP to a given environment or system.
    /// E.g. - Linux, Azure Cloud, etc...
    /// </summary>
    public interface ISystemAdaptor
    {
        /// <summary>
        /// Implement to provide low-level API implementations for a given system.
        /// E.g. - win32, equivalent Linux system calls, etc...
        /// </summary>
        ISystemInterface SystemInterface { get; }

        #region High-level SOP system integration API
        /// <summary>
        /// Instantiate an env't or system specific FileStream implementation.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="mode"></param>
        /// <param name="access"></param>
        /// <param name="share"></param>
        /// <returns></returns>
        IFileStream CreateFileStream(string path, FileMode mode, FileAccess access, FileShare share);
        /// <summary>
        /// Instantiate an env't or system specific FileStream implementation.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="mode"></param>
        /// <param name="acc"></param>
        /// <param name="share"></param>
        /// <param name="sequential"></param>
        /// <param name="async"></param>
        /// <param name="blockSize"></param>
        /// <returns></returns>
        IFileStream CreateFileStream(string path, FileMode mode, FileAccess acc, FileShare share, bool sequential, bool async, int blockSize);
        #endregion
    }
}
