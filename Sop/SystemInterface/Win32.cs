// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

#if (_WIN32)

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security;
using Microsoft.Win32.SafeHandles;

namespace Sop.SystemInterface
{
    using System;
    using System.IO;
    using System.Security;

    /// <summary>
    /// System I/O interface methods.
    /// </summary>
    public class SystemInterface : ISystemInterface
    {
        /// <summary>
        /// Encoding defaults to UTF7 on Linux systems.
        /// </summary>
        public System.Text.Encoding DefaultEncoding
        {
            get
            {
                return System.Text.Encoding.UTF8;
            }
        }

        public long GetMemorySize()
        {
            return Win32.GetMemorySize();
        }
        public int GetMaxStdio()
        {
            return Win32.GetMaxStdio();
        }
        public int SetMaxStdio(int newMax)
        {
            return Win32.SetMaxStdio(newMax);
        }

        public int GetDriveSectorSize(string path)
        {
            return Win32.GetDriveSectorSize(path);
        }

        public FileStream UnbufferedOpen(string path, FileMode mode, FileAccess acc, FileShare share, bool sequential, bool async, int blockSize)
        {
            return Win32.UnbufferedOpen(path, mode, acc, share, sequential, async, blockSize);
        }
    }

    /// <summary>
    /// Win 32 I/O API interop declarations.
    /// </summary>
    static class Win32
    {
        #region get RAM size
        /// <summary>
        /// Returns the total physical memory (RAM) size of this host.
        /// </summary>
        /// <returns></returns>
        [SecuritySafeCritical]
        public static long GetMemorySize()
        {
            if (installedMemory == 0)
            {
                MEMORYSTATUSEX memStatus = new MEMORYSTATUSEX();
                if (GlobalMemoryStatusEx(memStatus))
                {
                    installedMemory = (long)memStatus.ullTotalPhys;
                }
            }
            return installedMemory;
        }
        private static long installedMemory;

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
        private class MEMORYSTATUSEX
        {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;
            public MEMORYSTATUSEX()
            {
                this.dwLength = (uint)Marshal.SizeOf(this);
            }
        }
        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        static extern bool GlobalMemoryStatusEx([In, Out] MEMORYSTATUSEX lpBuffer);
        #endregion


        [SecuritySafeCritical]
        public static int GetMaxStdio()
        {
            return _getmaxstdio();
        }
        [SecuritySafeCritical]
        public static int SetMaxStdio(int newMax)
        {
            return _setmaxstdio(newMax);
        }

        //[SecuritySafeCritical]
        [DllImport("msvcrt.dll")]
        internal static extern int _getmaxstdio();

        [DllImport("msvcrt.dll")]
        internal static extern int _setmaxstdio(int newmax);

        //[SecuritySafeCritical]
        [DllImport("KERNEL32", SetLastError = true, CharSet = CharSet.Auto, BestFitMapping = false)]
        private static extern bool GetDiskFreeSpace(string path,
                                                    out uint sectorsPerCluster,
                                                    out uint bytesPerSector,
                                                    out uint numberOfFreeClusters,
                                                    out uint totalNumberOfClusters);

        /// <summary>
        /// Return the sector size of the volume the specified filepath lives on.
        /// </summary>
        /// <param name="path">UNC path name for the file or directory</param>
        /// <returns>device sector size in bytes </returns>
        [SecuritySafeCritical]
        public static int GetDriveSectorSize(string path)
        {
            string PathRoot = OnDisk.ObjectServer.GetRootPath(path);
            if (string.IsNullOrEmpty(PathRoot))
                throw new InvalidOperationException(string.Format("Can't get path root from '{0}'", path));

            uint size; // sector size in bytes. 
            uint i; // ignored
            GetDiskFreeSpace(PathRoot, out i, out size, out i, out i);
            return (int)size;
        }

        private const int FileFlagNoBuffering = unchecked((int)0x20000000);
        private const int FileFlagOverlapped = unchecked((int)0x40000000);
        private const int FileFlagSequentialScan = unchecked((int)0x08000000);

        //[SecuritySafeCritical]
        [DllImport("KERNEL32", SetLastError = true, CharSet = CharSet.Auto, BestFitMapping = false)]
        private static extern SafeFileHandle CreateFile(String fileName,
                                                        int desiredAccess,
                                                        FileShare shareMode,
                                                        IntPtr securityAttrs,
                                                        FileMode creationDisposition,
                                                        int flagsAndAttributes,
                                                        IntPtr templateFile);


        /// <summary>
        /// Given a path and mode/sharing/etc flags, open a file in un-buffered mode.
        ///    (i.e. NTFS is told not to cache the file contents).
        /// </summary>
        /// <param name="path">File name</param>
        /// <param name="mode"> System.IO.FileMode </param>
        /// <param name="acc">System.IO.FileAccess: Read | Write | ReadWrite</param>
        /// <param name="share">System.IO.FileShare</param>
        /// <param name="sequential">sequential file access</param>
        /// <param name="async">async file access</param>
        /// <param name="blockSize">block size in bytes</param>
        /// <returns>Unbuffered file stream.</returns>
        [SecuritySafeCritical]
        public static System.IO.FileStream UnbufferedOpen(string path,
                                                FileMode mode,
                                                FileAccess acc,
                                                FileShare share,
                                                bool sequential,
                                                bool async,
                                                int blockSize)
        {
            int flags = FileFlagNoBuffering; // default to simple no buffering
            /* Construct the proper 'flags' value to pass to CreateFile() */
            if (sequential) flags |= FileFlagSequentialScan;
            if (async) flags |= FileFlagOverlapped;

            FileStream stream = null; // the file stream created by this open.  
            SafeFileHandle handle; // the underlying NTFS file handle. 

            //* Call the Windows CreateFile() API to open the file
            handle = CreateFile(path,
                                (int)acc,
                                share,
                                IntPtr.Zero,
                                mode,
                                flags,
                                IntPtr.Zero);

            if (!handle.IsInvalid)
            {
                // NOTE: experiment using 4096 buffersize as shown in Jim Grey's Sequential & Performance File programming sample code...
                //if (blockSize < 4096) blockSize = 4096;
                /* Wrap the handle in a stream and return it to the caller */
                stream = new FileStream(handle, acc, blockSize, async);
            }
            else
            {
                throw new InvalidOperationException(string.Format("Error opening file '{0}'", path));
            }
            return stream;
        }
    }
}

#endif
