// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using Sop.Collections;
using Sop.OnDisk.Algorithm.SortedDictionary;

namespace Sop.Caching
{
    public class ThreadSafeEnumerator : IEnumerator<KeyValuePair<string, object>>
    {
        void IDisposable.Dispose()
        {
            if (Store == null)
                return;
            Store.Dispose();
            Store = null;
        }

        internal Sop.ISortedDictionary<CacheKey, CacheEntry> Store;

        public ThreadSafeEnumerator(Sop.ISortedDictionary<CacheKey, CacheEntry> store)
        {
            if (store == null)
                throw new ArgumentNullException("store");
            Store = store;
        }

        #region IEnumerator<T> Members

        public KeyValuePair<string, object> Current
        {
            get
            {
                return Store.Locker.Invoke(() =>
                    {
                        if (Store.CurrentKey == null)
                            return default(KeyValuePair<string, object>);
                        return new KeyValuePair<string, object>(Store.CurrentKey.Key, Store.CurrentValue.Value);
                    });
            }
        }

        #endregion

        #region IEnumerator Members

        object IEnumerator.Current
        {
            get
            {
                return Current;
            }
        }

        public bool MoveNext()
        {
            if (_wasReset)
            {
                _wasReset = false;
                return true;
            }
            return Store.Locker.Invoke(() =>
                {
                    return Store.MoveNext();
                });
        }

        public void Reset()
        {
            Store.Locker.Invoke(() =>
                {
                    Store.MoveFirst();
                    _wasReset = true;
                });
        }
        private bool _wasReset;
        #endregion
    }
}
