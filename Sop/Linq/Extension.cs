using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sop.SpecializedDataStore;

namespace Sop.Linq
{
    public static class Extension
    {
        class FilteredEnumerator<TKey, TValue> : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            private ISortedDictionaryOnDisk _store;
            private TKey[] _keys;
            private int _keyIndex = -1;
            public FilteredEnumerator(ISortedDictionary<TKey, TValue> store, TKey[] keys)
            {
                if (keys == null || keys.Length == 0)
                    throw new ArgumentNullException("keys");
                if (store == null)
                    throw new ArgumentNullException("store");
                _store = (ISortedDictionaryOnDisk)((SpecializedStoreBase)store).Collection.Clone();
                _keys = keys;
            }

            public void Dispose()
            {
                if (_store == null) return;
                _store.Dispose();
                _store = null;
            }

            public void Reset()
            {
                _keyIndex = -1;
            }
            public bool MoveNext()
            {
                if (_keyIndex >= _keys.Length - 1)
                    return false;
                _keyIndex++;
                return true;
            }


            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    if (_store.Search(_keys[_keyIndex]))
                        return new KeyValuePair<TKey, TValue>((TKey)_store.CurrentEntry.Key, 
                            (TValue)_store.CurrentEntry.Value);
                    return default(KeyValuePair<TKey, TValue>);
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    KeyValuePair<TKey, TValue> c = Current;
                    return c;
                }
            }
        }
        class FilteredEnumerable<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
        {
            private ISortedDictionary<TKey, TValue> _store;
            private TKey[] _keys;
            public FilteredEnumerable(ISortedDictionary<TKey, TValue> store, TKey[] keys)
            {
                if (keys == null)
                    throw new ArgumentNullException("keys");
                if (store == null)
                    throw new ArgumentNullException("store");
                _store = store;
                _keys = keys;
            }
            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
            {
                return new FilteredEnumerator<TKey, TValue>(_store, _keys);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new FilteredEnumerator<TKey, TValue>(_store, _keys);
            }
        }

        public static IEnumerable<KeyValuePair<TKey, TValue>> Query<TKey, TValue>(
            this IEnumerable<KeyValuePair<TKey, TValue>> source, TKey[] keys)
        {
            return new FilteredEnumerable<TKey, TValue>((ISortedDictionary<TKey, TValue>)source, keys);
        }
    }
}
