// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Threading;
using Sop.Transaction;

namespace Sop.OnDisk.IO
{
    /// <summary>
    /// Concurrent I/O Pool manager.
    /// </summary>
    internal class ConcurrentIOPoolManager : IDisposable
    {
        private int Count
        {
            get
            {
                if (_pool == null) return 0;
                return _pool.Count;
            }
        }
        public void Dispose()
        {
            if (_pool == null) return;
            WaitClear();
            _pool = null;
            // rethrow the Async IO exception encountered to rollback the transaction.
            if (AsyncThreadException != null)
                throw AsyncThreadException;
        }

        public void WaitClear(bool checkIfMax = false)
        {
            lock (this)
            {
                if (checkIfMax && Count < MaxConcurrentIO) return;
                ManualResetEvent[] writers = GetEvents();
                if (writers != null && writers.Length > 0)
                    WaitAll(writers);
                foreach (ConcurrentIOData o in _pool)
                    o.Dispose();
                _pool.Clear();
            }
        }

        public ManualResetEvent[] GetEvents()
        {
            lock (this)
            {
                var r = new List<ManualResetEvent>(_pool.Count);
                for (int i = 0; i < _pool.Count; i++)
                {
                    r.Add(_pool[i].Event);
                }
                return r.ToArray();
            }
        }
        public ManualResetEvent[] GetEvents(string filename)
        {
            lock (this)
            {
                var r = new List<ManualResetEvent>(_pool.Count);
                for (int i = 0; i < _pool.Count; i++)
                {
                    if (string.IsNullOrEmpty(filename) || _pool[i].Filename == filename)
                        r.Add(_pool[i].Event);
                }
                return r.ToArray();
            }
        }
        public ConcurrentIOData GetItem(ManualResetEvent evnt)
        {
            lock (this)
            {
                for (int i = 0; i < _pool.Count; i++)
                {
                    if (_pool[i].Event == evnt)
                    {
                        evnt.Reset();
                        var rw = _pool[i];
                        rw.FileStream.InUse = true;
                        return rw;
                    }
                }
                return null;
            }
        }
        public ConcurrentIOData GetInstance(File.IFile f, long size = 0)
        {
            if (f.Server != null && f.Server.Transaction != null)
                return GetInstance(f.Filename,
                                           (TransactionRoot)
                                           ((TransactionBase)f.Server.Transaction).Root, size);
            return GetInstance(f.Filename, null, size);
        }

        public ConcurrentIOData GetInstance(string filename,
                                                           TransactionRoot transRoot,
                                                           long size = 0)
        {
            lock (this)
            {
                ConcurrentIOData r = null;
                ManualResetEvent[] readers = GetEvents(filename);
                if (_pool.Count == 0 || readers.Length == 0)
                {
                    r = new ConcurrentIOData { Filename = filename, Buffer = new byte[size], PoolManager = this };
                    if (transRoot != null)
                        transRoot.RegisterOpenFile(filename);
                    r.FileStream = File.File.UnbufferedOpen(filename);
                    _pool.Add(r);
                }
                else
                {
                    if (_pool.Count < MaxConcurrentIO || readers.Length < 4)
                    {
                        int index = WaitHandle.WaitAny(ToWaitHandles(readers), 30, false);
                        if (index == WaitHandle.WaitTimeout)
                        {
                            r = new ConcurrentIOData { Filename = filename, Buffer = new byte[size], PoolManager = this };
                            if (transRoot != null)
                                transRoot.RegisterOpenFile(filename);
                            r.FileStream = File.File.UnbufferedOpen(filename);
                            _pool.Add(r);
                        }
                        else
                            r = GetItem(readers[index]);
                    }
                    else
                    {
                        int index = WaitHandle.WaitAny(ToWaitHandles(readers));
                        r = GetItem(readers[index]);
                    }
                }
                if (r.Buffer == null || r.Buffer.Length != size)
                    r.Buffer = new byte[size];
                return r;
            }
        }


        #region Wait until all events are signalled
        internal static bool WaitAll(ManualResetEvent[] handles)
        {
            if (handles == null)
                throw new ArgumentNullException("handles");
            if (Thread.CurrentThread.GetApartmentState() == ApartmentState.STA)
            {
                // WaitAll for multiple handles on an STA thread is not supported.
                // ...so wait on each handle individually.
                foreach (ManualResetEvent myWaitHandle in handles)
                    WaitHandle.WaitAny(new WaitHandle[] { myWaitHandle });
                return true;
            }
            return WaitHandle.WaitAll(ToWaitHandles(handles));
        }
        static internal WaitHandle[] ToWaitHandles(ManualResetEvent[] handles)
        {
            var hes = new WaitHandle[handles.Length];
            for (int i = 0; i < handles.Length; i++)
                hes[i] = handles[i];
            return hes;
        }
        #endregion

        public const int MaxConcurrentIO = 64;
        private List<ConcurrentIOData> _pool = new List<ConcurrentIOData>(MaxConcurrentIO);

        internal void AddException(Exception exc)
        {
            lock (this)
            {
                if (AsyncThreadException == null)
                {
                    AsyncThreadException = new IOExceptions(exc.Message, exc);
                    return;
                }
            }
            AsyncThreadException.OtherThreadExceptions.Add(exc);
        }
        /// <summary>
        /// Asynchronous (I/O) thread encountered exceptions.
        /// If this is set, SOP will rollback the transaction to prevent data corruption.
        /// </summary>
        internal IO.IOExceptions AsyncThreadException;
    }
}
