// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using System.Threading;
using Sop.Mru.Generic;
using Sop.OnDisk.IO;
using Sop.Utility;

namespace Sop.OnDisk.File
{
    internal class FileStream : IDisposable
    {
        #region Constructors
        public FileStream(string path, FileMode mode, FileAccess acc,
                          FileShare share, bool sequential, bool async, int blockSize)
        {
            _path = path;
            _mode = mode;
            _access = acc;
            _share = share;
            _sequential = sequential;
            _async = async;
            _blockSize = blockSize;
            _unBuffered = true;
            // assign Id for this instance...
            Id = Interlocked.Increment(ref _instanceCount);
            _fileStream = Open();
            MaintainOpenedFileCount();
        }
        public FileStream(string path, FileMode mode, FileAccess access, FileShare share)
        {
            _path = path;
            _mode = mode;
            _access = access;
            _share = share;
            _fileStream = new System.IO.FileStream(path, mode, access, share);
        }
        #endregion

        #region Open/Close
        private System.IO.FileStream Open()
        {
            MruManager.Add(Id, this);
            return Win32.UnbufferedOpen(_path, _mode, _access, _share,
                                        _sequential, _async, _blockSize);
        }

        public void Close()
        {
            if (_isDisposed || _fileStream == null) return;
            _fileStream.Close();
            if (_unBuffered)
            {
                InUse = false;
                MruManager.Remove(Id);
                _fileStream = null;
            }
        }
        #endregion

        /// <summary>
        /// Returns in-memory ID of this file stream object.
        /// </summary>
        public int Id { get; private set; }
        /// <summary>
        /// Dispose the File Stream.
        /// </summary>
        public void Dispose()
        {
            _isDisposed = true;
            if (_fileStream == null) return;
            if (!_unBuffered)
            {
                _fileStream.Dispose();
                _isDisposed = true;
                _fileStream = null;
                return;
            }
            MruManager.Remove(Id);
            lock (_locker)
            {
                if (_fileStream == null) return;
                InUse = false;
                _fileStream.Dispose();
                _isDisposed = true;
                _fileStream = null;
            }
        }

        internal BinaryReader CreateBinaryReader(Encoding encoding)
        {
            return new BinaryReader(RealStream, encoding)
            {
                Container = this
            };
        }

        public long Length
        {
            get
            {
                long l = RealStream.Length;
                InUse = false;
                return l;
            }
        }

        public long Position
        {
            get
            {
                long l = RealStream.Position;
                InUse = false;
                return l;
            }
            set
            {
                RealStream.Position = value;
                InUse = false;
            }
        }
        public void Flush()
        {
            if (_isDisposed)
                throw new SopException("Can't access a disposed FileStream.");
            if (!_unBuffered)
            {
                if (_fileStream != null)
                    _fileStream.Flush();
                return;
            }
            if (_fileStream == null) return;
            lock (_locker)
            {
                if (_fileStream == null) return;
                _fileStream.Flush();
                InUse = false;
            }
        }

        public void SetLength(long value)
        {
            RealStream.SetLength(value);
            InUse = false;
        }

        public int Read([In, Out] byte[] array, int offset, int count)
        {
            int i;
            if (count <= 512)
                i = RealStream.Read(array, offset, count);
            else
            {
                IAsyncResult iar = RealStream.BeginRead(array, offset, count, null, null);
                if (!iar.IsCompleted)
                    iar.AsyncWaitHandle.WaitOne();
                i = RealStream.EndRead(iar);
            }
            InUse = false;
            return i;
        }

        public long Seek(long offset, SeekOrigin origin)
        {
            long l = RealStream.Seek(offset, origin);
            InUse = false;
            return l;
        }

        public void Write(byte[] array, int offset, int count)
        {
            RealStream.Write(array, offset, count);
            InUse = false;
        }

        public IAsyncResult BeginRead(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject)
        {
            return RealStream.BeginRead(array, offset, numBytes, userCallback, stateObject);
        }

        public int EndRead(IAsyncResult asyncResult)
        {
            int i = RealStream.EndRead(asyncResult);
            InUse = false;
            return i;
        }

        public IAsyncResult BeginWrite(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject)
        {
            return RealStream.BeginWrite(array, offset, numBytes, userCallback, stateObject);
        }

        public void EndWrite(IAsyncResult asyncResult)
        {
            RealStream.EndWrite(asyncResult);
            InUse = false;
        }
        /// <summary>
        /// Returns the System.IO.FileStream object.
        /// </summary>
        public System.IO.FileStream RealStream
        {
            get
            {
                if (_isDisposed)
                    throw new SopException("Can't access a disposed FileStream.");
                if (!_unBuffered)
                    return _fileStream;

                if (_fileStream != null && InUse) return _fileStream;
                lock (_locker)
                {
                    InUse = true;
                    if (_fileStream == null)
                    {
                        _fileStream = Open();
                        //_fileStream.Seek(_streamPosition, SeekOrigin.Begin);
                    }
                    return _fileStream;
                }
            }
        }

        private bool _inUse;
        private bool InUse
        {
            get
            {
                return _inUse;
            }
            set
            {
                _inUse = value;
                if (!value) return;
                MruManager.Remove(Id);
                MruManager.Add(Id, this);
            }
        }

        private static int _maxInstanceCount;
        internal static int MaxInstanceCount
        {
            get
            {
                if (_maxInstanceCount == 0)
                {
                    _maxInstanceCount = Win32.GetMaxStdio() - ConcurrentIOPoolManager.MaxConcurrentIO;
                }
                return _maxInstanceCount;
            }
            set
            {
                if (_maxInstanceCount > 0)
                    throw new SopException(
                        string.Format("Can't update FileStream.MaxInstanceCount, it is already set to {0}.",
                                      _maxInstanceCount));
                if (value < 5)
                    throw new SopException(string.Format("Can't set FileStream.MaxInstanceCount to {0}, minimum is 5.",
                                                         value));
                _maxInstanceCount = value;
            }
        }
        private void MaintainOpenedFileCount()
        {
            if (MruManager.Count <= MaxInstanceCount) return;
            while (MruManager.Count > MruManager.MinCapacity)
            {
                Mru.Generic.MruItem<int, FileStream> item = MruManager.PeekInTail();
                if (item.Value.InUse) return;
                lock (item.Value._locker)
                {
                    if (item.Value.InUse || item.Value._fileStream == null)
                        return;
                    MruManager.Remove(item.Value.Id);
                    //item.Value._streamPosition = item.Value._fileStream.Position;
                    item.Value._fileStream.Flush();
                    item.Value._fileStream.Close();
                    item.Value._fileStream = null;
                }
            }
        }

        private static Mru.Generic.IMruManager<int, FileStream> _mruManager;
        private static Mru.Generic.IMruManager<int, FileStream> MruManager
        {
            get
            {
                return _mruManager ??
                       (_mruManager =
                        new ConcurrentMruManager<int, FileStream>((int)(MaxInstanceCount * 0.75), MaxInstanceCount) { GeneratePruneEvent = false });
            }
        }

        #region variables for state tracking
        //private long _streamPosition;
        private static int _instanceCount;
        private object _locker = new object();
        private System.IO.FileStream _fileStream;
        private bool _isDisposed;
        private bool _unBuffered;
        #endregion

        #region parameters used to re-open the file stream
        private string _path;
        private FileMode _mode;
        private FileAccess _access;
        private FileShare _share;
        private bool _sequential;
        private bool _async;
        private int _blockSize;
        #endregion

        #region FileStream private Binary reader
        internal class BinaryReader : System.IO.BinaryReader
        {
            public BinaryReader(Stream input, Encoding encoding) : base(input, encoding) { }
            public override void Close()
            {
                //Container.Close();
                Container.InUse = false;
                //MruManager.Remove(Container.Id);
                Container = null;
                //base.Close();
            }
            internal FileStream Container;
        }
        #endregion
    }
}
