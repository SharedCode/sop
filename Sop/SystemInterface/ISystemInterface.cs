// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.IO;
using System.Security;

namespace Sop.SystemInterface
{
    /// <summary>
    /// SOP's Operating System interface methods.
    /// </summary>
    public interface ISystemInterface
    {
        System.Text.Encoding DefaultEncoding { get; }
        long GetMemorySize();
        int GetMaxStdio();
        int SetMaxStdio(int newMax);
        int GetDriveSectorSize(string path);
        System.IO.FileStream UnbufferedOpen(string path,
                                                FileMode mode,
                                                FileAccess acc,
                                                FileShare share,
                                                bool sequential,
                                                bool async,
                                                int blockSize);
    }
}
