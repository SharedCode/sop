using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sop.SpecializedDataStore;

namespace Sop.Linq
{
    /// <summary>
    /// SOP LINQ to Objects extensions.
    /// </summary>
    public static class Extension
    {
        #region Store's filtered set IEnumerable & IEnumerator
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
                // if index points to pre-1st element...
                if (_keyIndex < 0)
                {
                    _keyIndex++;
                    if (_store.Search(_keys[_keyIndex], true))
                        return true;
                    while (++_keyIndex <= _keys.Length - 1)
                    {
                        if (_store.Search(_keys[_keyIndex], true))
                            return true;
                    }
                    return false;
                }
                // move store pointer to next element and if it has same key as current one in keys array,
                // just return true so Store can return this element.
                if (_store.MoveNext() && _store.Comparer.Compare(_store.CurrentKey, _keys[_keyIndex]) == 0)
                    return true;
                while (++_keyIndex <= _keys.Length - 1)
                {
                    if (_store.Search(_keys[_keyIndex], true))
                        return true;
                }
                return false;
            }

            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    return new KeyValuePair<TKey, TValue>((TKey)_store.CurrentEntry.Key,
                            (TValue)_store.CurrentEntry.Value);
                }
            }

            object IEnumerator.Current
            {
                get { return _store.CurrentEntry; }
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
        #endregion

        /// <summary>
        /// Efficiently select into a list(IEnumerable) those records of a given Store
        /// whose keys match with the submitted keys. Marked "Efficiently" because each 
        /// returned IEnumerable is a "thin" wrapper for the Store that allows 
        /// record navigation, record filtration based on keys utilizing minimal resources.
        /// Each instance shares the same MRU cache, thus, occupying the least 
        /// amount of memory possible, for querying and filtering records of a Store.
        /// 
        /// NOTE: code can execute Query multiple times for the same Store within the same
        /// LINQ query block. Each returned IEnumerable doesn't conflict with one another
        /// nor with the Store.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="source"></param>
        /// <param name="keys"></param>
        /// <returns>IEnumerable that iterates through matching records for the submitted keys.</returns>
        public static IEnumerable<KeyValuePair<TKey, TValue>> Query<TKey, TValue>(
            this IEnumerable<KeyValuePair<TKey, TValue>> store, TKey[] keys)
        {
            return new FilteredEnumerable<TKey, TValue>((ISortedDictionary<TKey, TValue>)store, keys);
        }
    }
}
