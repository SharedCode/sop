// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using Sop.OnDisk.File;

namespace Sop.OnDisk.DataBlock
{
    /// <summary>
    /// Data Block Read ahead buffer logic.
    /// </summary>
    internal class DataBlockReadBufferLogic
    {
        public DataBlockReadBufferLogic() { }
        public DataBlockReadBufferLogic(DataBlockReadBufferLogic source)
        {
            this._readBuffer = source._readBuffer;
            this._readBufferDataAddress = source._readBufferDataAddress;
            this._readBufferSize = source._readBufferSize;
        }
        public DataBlockReadBufferLogic(byte[] buffer, long dataAddress, int dataSize)
        {
            this._readBuffer = new byte[dataSize];
            buffer.CopyTo(_readBuffer, 0);
            this._readBufferDataAddress = dataAddress;
            this._readBufferSize = dataSize;
        }

        public void Read(FileStream fileStream, long dataAddress, int dataSize)
        {
            if (Contains(dataAddress, dataSize))
                return;

            if (fileStream.Position != dataAddress)
                fileStream.Seek(dataAddress, System.IO.SeekOrigin.Begin);

            if (_readBuffer == null ||
                _readBuffer.Length < dataSize)
                _readBuffer = new byte[dataSize];
            _readBufferSize = dataSize;
            _readBufferDataAddress = dataAddress;
            int res = fileStream.Read(_readBuffer, 0, dataSize);
            if (res < dataSize)
                throw new SopException(string.Format("ReadBlockFromDisk: read {0} bytes, requested {1}", res, dataSize));
        }

        private bool Contains(long dataAddress, int dataSize, bool allowPartialRead = false)
        {
            if (allowPartialRead)
                return dataAddress >= _readBufferDataAddress &&
                    dataAddress < _readBufferDataAddress + _readBufferSize;
            return dataAddress >= _readBufferDataAddress &&
                dataAddress + dataSize <= _readBufferDataAddress + _readBufferSize;
        }

        public byte[] Get(long dataAddress, int dataSize, bool allowPartialRead = false)
        {
            if (Contains(dataAddress, dataSize, allowPartialRead))
            {
                // adjust amount of data to copy for "partial copy" case.
                if (allowPartialRead &&
                    dataAddress + dataSize > _readBufferDataAddress + _readBufferSize)
                    dataSize -= ((int)(dataAddress + dataSize - (_readBufferDataAddress + _readBufferSize)));
                byte[] r = new byte[dataSize];
                Array.Copy(_readBuffer, dataAddress - _readBufferDataAddress, r, 0, r.Length);
                return r;
            }
            Clear();
            return null;
        }

        public void Clear()
        {
            //_readBuffer = null;
            _readBufferDataAddress = -1;
        }

        public bool IsEmpty
        {
            get { return _readBufferDataAddress == -1; }
        }

        private byte[] _readBuffer;
        private int _readBufferSize;
        private long _readBufferDataAddress = -1;
    }
}
