// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using Sop.OnDisk.Algorithm.SortedDictionary;

namespace Sop.SpecializedDataStore
{
    public partial class SimpleKeyValue<TKey, TValue>
    {
        internal class GenericEnumerator<T> : IEnumerator<T>
        {
            void IDisposable.Dispose()
            {
                var disposable = Enumerator as IDisposable;
                if (disposable == null) return;
                disposable.Dispose();
                Enumerator = null;
            }

            internal System.Collections.IDictionaryEnumerator Enumerator;

            public GenericEnumerator(System.Collections.IDictionaryEnumerator enumerator)
            {
                this.Enumerator = enumerator;
            }

            #region IEnumerator<T> Members

            public T Current
            {
                get
                {
                    if (((SortedDictionaryOnDisk.DictionaryEnumerator) Enumerator).BTree.ItemType ==
                        Sop.Collections.BTree.ItemType.Default)
                    {
                        object o = Enumerator.Current;
                        DictionaryEntry de = (DictionaryEntry) o;
                        KeyValuePair<TKey, TValue> r = new KeyValuePair<TKey, TValue>((TKey) de.Key, (TValue) de.Value);
                        o = r;
                        return (T) o;
                    }
                    return (T) Enumerator.Current;
                }
            }

            #endregion

            #region IEnumerator Members

            object IEnumerator.Current
            {
                get { return Enumerator.Current; }
            }

            public bool MoveNext()
            {
                return Enumerator.MoveNext();
            }

            public void Reset()
            {
                Enumerator.Reset();
            }

            #endregion
        }
    }
}