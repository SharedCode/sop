// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
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
        public void Read(FileStream fileStream, long dataAddress, int dataSize)
        {
            if (_readBuffer == null ||
                _readBuffer.Length < dataSize)
                _readBuffer = new byte[dataSize];
            _readBufferSize = dataSize;
            _readBufferDataAddress = dataAddress;
            int res = fileStream.Read(_readBuffer, 0, dataSize);
            if (res < dataSize)
                throw new SopException(string.Format("ReadBlockFromDisk: read {0} bytes, requested {1}", res, dataSize));
        }

        public byte[] Get(long dataAddress, int dataSize)
        {
            if (_readBufferDataAddress != 0 &&
                dataAddress >= _readBufferDataAddress &&
                dataAddress + dataSize <= _readBufferDataAddress + _readBufferSize)
            {
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
            _readBufferDataAddress = 0;
        }

        public bool IsEmpty
        {
            get { return _readBufferDataAddress == 0; }
        }

        private byte[] _readBuffer;
        private int _readBufferSize;
        private long _readBufferDataAddress;
    }
}
