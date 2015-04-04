// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
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
        public int Count
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
        }

        public void WaitClear()
        {
            WaitClear(false);
        }
        public void WaitClear(bool checkIfMax)
        {
            if (checkIfMax && Count < MaxConcurrentIO) return;
            AutoResetEvent[] writers = GetEvents();
            if (writers != null && writers.Length > 0)
                WaitAll(writers);
            foreach (ConcurrentIOData o in _pool)
                o.Dispose();
            _pool.Clear();
        }

        public AutoResetEvent[] GetEvents()
        {
            var r = new List<AutoResetEvent>(_pool.Count);
            for (int i = 0; i < _pool.Count; i++)
            {
                r.Add(_pool[i].Event);
            }
            return r.ToArray();
        }
        public AutoResetEvent[] GetEvents(string filename)
        {
            var r = new List<AutoResetEvent>(_pool.Count);
            for (int i = 0; i < _pool.Count; i++)
            {
                if (string.IsNullOrEmpty(filename) || _pool[i].Filename == filename)
                    r.Add(_pool[i].Event);
            }
            return r.ToArray();
        }
        public ConcurrentIOData GetItem(AutoResetEvent evnt)
        {
            for (int i = 0; i < _pool.Count; i++)
            {
                if (_pool[i].Event == evnt)
                    return _pool[i];
            }
            return null;
        }
        public ConcurrentIOData GetInstance(File.IFile f, long size)
        {
            if (f.Server != null && f.Server.Transaction != null)
                return GetInstance(f.Filename,
                                           (TransactionRoot)
                                           ((TransactionBase)f.Server.Transaction).Root, size);
            return GetInstance(f.Filename, null, size);
        }
        public ConcurrentIOData GetInstance(string filename,
                                                           TransactionRoot transRoot,
                                                           long size)
        {
            ConcurrentIOData r = null;
            AutoResetEvent[] readers = GetEvents(filename);
            if (_pool.Count == 0 || readers.Length == 0)
            {
                r = new ConcurrentIOData { Filename = filename, Buffer = new byte[size] };
                if (transRoot != null)
                    transRoot.RegisterOpenFile(filename);
                r.FileStream = File.File.UnbufferedOpen(filename);
                _pool.Add(r);
            }
            else
            {
                if (_pool.Count < MaxConcurrentIO || readers.Length < 4)
                {
                    int index = WaitHandle.WaitAny(ToWaitHandles(readers), 2000, false);
                    if (index == WaitHandle.WaitTimeout)
                    {
                        r = new ConcurrentIOData { Filename = filename, Buffer = new byte[size] };
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
                    //Logger.Instance.LogLine("WaitAny on Readers");
                }
            }
            if (r.Buffer.Length != size)
                r.Buffer = new byte[size];
            return r;
        }



        #region Wait until all events are signalled
        internal static bool WaitAll(AutoResetEvent[] handles)
        {
            if (handles == null)
                throw new ArgumentNullException("handles");
            if (Thread.CurrentThread.ApartmentState == ApartmentState.STA)
            {
                // WaitAll for multiple handles on an STA thread is not supported.
                // ...so wait on each handle individually.
                foreach (AutoResetEvent myWaitHandle in handles)
                    WaitHandle.WaitAny(new WaitHandle[] { myWaitHandle });
                return true;
            }
            return WaitHandle.WaitAll(ToWaitHandles(handles));
        }
        static internal WaitHandle[] ToWaitHandles(AutoResetEvent[] handles)
        {
            var hes = new WaitHandle[handles.Length];
            for (int i = 0; i < handles.Length; i++)
                hes[i] = handles[i];
            return hes;
        }
        #endregion

        /// <summary>
        /// Maximum number of concurrent IO. Defaults to 32.
        /// </summary>
        public static int MaxConcurrentIO
        {
            get { return _maxConcurrentIO; }
            set
            {
                if (value < 5)
                    value = 5;
                _maxConcurrentIO = value;
            }
        }

        private static int _maxConcurrentIO = 64;
        private List<ConcurrentIOData> _pool = new List<ConcurrentIOData>(MaxConcurrentIO);
    }
}