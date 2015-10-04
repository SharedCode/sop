using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Sop.OnDisk.File;
using Sop.SystemInterface;

namespace Sop.Adaptations.Azure
{
    public class SystemAdaptor : SystemInterface.SystemAdaptor<SystemAdaptor>
    {
        public SystemAdaptor() { }

        override public IFileStream CreateFileStream(string path, FileMode mode, FileAccess access, FileShare share)
        {
            throw new NotImplementedException();
        }

        override public IFileStream CreateFileStream(string path, FileMode mode, FileAccess acc, FileShare share,
            bool sequential, bool async, int blockSize)
        {
            throw new NotImplementedException();
        }
    }
}
