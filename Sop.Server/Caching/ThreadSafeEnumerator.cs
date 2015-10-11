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
            var disposable = enumerator as IDisposable;
            if (disposable != null)
            {
                disposable.Dispose();
            }
            enumerator = null;
        }

        internal IEnumerator<KeyValuePair<CacheKey, CacheEntry>> enumerator;

        public ThreadSafeEnumerator(IEnumerator<KeyValuePair<CacheKey, CacheEntry>> enumerator)
        {
            this.enumerator = enumerator;
        }

        #region IEnumerator<T> Members

        public KeyValuePair<string, object> Current
        {
            get
            {
                return new KeyValuePair<string, object>(enumerator.Current.Key.Key, 
                    enumerator.Current.Value.Value);
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
            return enumerator.MoveNext();
        }

        public void Reset()
        {
            enumerator.Reset();
        }
        #endregion
    }
}
