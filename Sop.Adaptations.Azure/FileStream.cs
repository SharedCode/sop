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
    /// <summary>
    /// Azure File Stream implementation uses Azure Blob API to store/manage data onto
    /// Azure Blob(s).
    /// </summary>
    public class FileStream : IFileStream
    {
        public int Id { get; internal set; }
        public long Length { get; internal set; }
        public long Position
        {
            get
            {
                throw new NotImplementedException();
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        public System.IO.FileStream RealStream
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public IAsyncResult BeginRead(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject)
        {
            throw new NotImplementedException();
        }

        public IAsyncResult BeginWrite(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject)
        {
            throw new NotImplementedException();
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public BinaryReader CreateBinaryReader(Encoding encoding)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public int EndRead(IAsyncResult asyncResult, bool leaveInUse = false)
        {
            throw new NotImplementedException();
        }

        public void EndWrite(IAsyncResult asyncResult, bool leaveInUse = false)
        {
            throw new NotImplementedException();
        }

        public void Flush()
        {
            throw new NotImplementedException();
        }

        public long GetPosition(bool leaveInUse = false)
        {
            throw new NotImplementedException();
        }

        public System.IO.FileStream Open()
        {
            throw new NotImplementedException();
        }

        public int Read([In, Out]byte[] array, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public Task<int> ReadAsync(byte[] array, int offset, int numBytes)
        {
            throw new NotImplementedException();
        }

        public long Seek(long offset, SeekOrigin origin, bool leaveInUse = false)
        {
            throw new NotImplementedException();
        }

        public void SetLength(long value, bool leaveInUse = false)
        {
            throw new NotImplementedException();
        }

        public void Write(byte[] array, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}
