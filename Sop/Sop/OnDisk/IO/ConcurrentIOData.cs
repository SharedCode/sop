// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Threading;
using Sop.OnDisk.File;

namespace Sop.OnDisk.IO
{
    /// <summary>
    /// Data entity used for doing a highly optimized,
    /// events based concurrent I/O.
    /// </summary>
    internal class ConcurrentIOData : IDisposable
    {
        public void Dispose()
        {
            if (FileStream == null) return;
            FileStream.Close();
            FileStream.Dispose();
            FileStream = null;
            Event.Close();
            Buffer = null;
        }
        /// <summary>
        /// Filename
        /// </summary>
        public string Filename;
        /// <summary>
        /// AutoResetEvent Event.
        /// </summary>
        public AutoResetEvent Event = new AutoResetEvent(false);
        /// <summary>
        /// Concurrent I/O buffer.
        /// </summary>
        public byte[] Buffer;
        /// <summary>
        /// FileStream.
        /// </summary>
        public FileStream FileStream;
    }
}
