using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Sop.OnDisk.File
{
    /// <summary>
    /// FileStream interface.
    /// </summary>
    public interface IFileStream : IDisposable
    {
        int Id { get; }
        long Length { get; }
        long Position { get; set; }
        System.IO.FileStream RealStream { get; }
        System.IO.BinaryReader CreateBinaryReader(Encoding encoding);

        IAsyncResult BeginRead(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject);
        IAsyncResult BeginWrite(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject);
        System.IO.FileStream Open();
        void Close();
        int EndRead(IAsyncResult asyncResult, bool leaveInUse = false);
        void EndWrite(IAsyncResult asyncResult, bool leaveInUse = false);
        void Flush();
        long GetPosition(bool leaveInUse = false);
        int Read([In, Out]byte[] array, int offset, int count);
        Task<int> ReadAsync(byte[] array, int offset, int numBytes);
        long Seek(long offset, SeekOrigin origin, bool leaveInUse = false);
        void SetLength(long value, bool leaveInUse = false);
        void Write(byte[] array, int offset, int count);
    }
}