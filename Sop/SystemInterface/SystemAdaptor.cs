using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Sop.OnDisk.File;

namespace Sop.SystemInterface
{
    public class SystemAdaptor : SystemAdaptor<SystemAdaptor>
    {
    }

    /// <summary>
    /// System Adaptor for use on interfacing with Systems such as Windows (Win32), Linux,
    /// Azure cloud and other future systems to be supported, where SOP will be ported.
    /// </summary>
    public class SystemAdaptor<T> : ISystemAdaptor
        where T : ISystemAdaptor, new()
    {
        public SystemAdaptor()
        {
            SystemInterface = new SystemInterface();
        }
        static SystemAdaptor()
        {
            if (Instance == null)
            {
                lock(Locker)
                {
                    if (Instance != null)
                        return;
                    Instance = new T();
                }
            }
        }

        public readonly static ISystemAdaptor Instance;
        protected static object Locker = new object();

        /// <summary>
        /// Returns a System interface.
        /// </summary>
        public ISystemInterface SystemInterface { get; private set; }

        public virtual IFileStream CreateFileStream(string path, FileMode mode, FileAccess acc,
                          FileShare share, bool sequential, bool async, int blockSize)
        {
            return new OnDisk.File.FileStream(path, mode, acc,share, sequential, async, blockSize);
        }
        public virtual IFileStream CreateFileStream(string path, FileMode mode, FileAccess access, FileShare share)
        {
            return new OnDisk.File.FileStream(path, mode, access, share);
        }
    }
}
