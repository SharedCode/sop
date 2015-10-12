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
        class EnumeratorKeysFilter<TKey, TValue> : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            private ISortedDictionaryOnDisk _store;
            private TKey[] _keys;
            private int _keyIndex = -1;

            public EnumeratorKeysFilter(ISortedDictionary<TKey, TValue> store, TKey[] keys)
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
                _store.Locker.Invoke(() =>
                {
                    _store.Dispose();
                });
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
                    if (_store.Locker.Invoke(_store.Search, (object)_keys[_keyIndex], true))
                        return true;
                    while (++_keyIndex <= _keys.Length - 1)
                    {
                        if (_store.Locker.Invoke(_store.Search, (object)_keys[_keyIndex], true))
                            return true;
                    }
                    return false;
                }
                // move store pointer to next element and if it has same key as current one in keys array,
                // just return true so Store can return this element.
                _store.Locker.Lock();
                try
                {
                    if (_store.MoveNext() &&
                        _store.Comparer.Compare(_store.CurrentKey, _keys[_keyIndex]) == 0)
                        return true;
                }
                finally
                {
                    _store.Locker.Unlock();
                }
                while (++_keyIndex <= _keys.Length - 1)
                {
                    if (_store.Locker.Invoke(_store.Search, (object)_keys[_keyIndex], true))
                        return true;
                }
                return false;
            }

            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    _store.Locker.Lock();
                    var ce = _store.CurrentEntry;
                    _store.Locker.Unlock();
                    return new KeyValuePair<TKey, TValue>((TKey)ce.Key, (TValue)ce.Value);
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    _store.Locker.Lock();
                    var ce = _store.CurrentEntry;
                    _store.Locker.Unlock();
                    return ce;
                }
            }
        }
        class EnumerableKeysFilter<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
        {
            private ISortedDictionary<TKey, TValue> _store;
            private TKey[] _keys;
            public EnumerableKeysFilter(ISortedDictionary<TKey, TValue> store, TKey[] keys)
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
                return new EnumeratorKeysFilter<TKey, TValue>(_store, _keys);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new EnumeratorKeysFilter<TKey, TValue>(_store, _keys);
            }
        }

        class EnumeratorEnumeratorFilter<TKey, TValue> : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            private ISortedDictionaryOnDisk _target;
            private IEnumerator<KeyValuePair<TKey, TValue>> _source;
            private bool _wasReset = true;

            public EnumeratorEnumeratorFilter(ISortedDictionary<TKey, TValue> target,
                IEnumerable<KeyValuePair<TKey, TValue>> source)
            {
                if (target == null)
                    throw new ArgumentNullException("target");
                if (source == null)
                    throw new ArgumentNullException("source");

                _target = (ISortedDictionaryOnDisk)((SpecializedStoreBase)target).Collection.Clone();
                _source = source.GetEnumerator();
            }

            public void Dispose()
            {
                if (_target == null) return;
                _target.Locker.Invoke(() =>
                {
                    _target.Dispose();
                });
                _target = null;
            }

            public void Reset()
            {
                _wasReset = true;
                _source.Reset();
            }
            public bool MoveNext()
            {
                // if index points to pre-1st element...
                if (_wasReset)
                {
                    _wasReset = false;
                    return _source.MoveNext();
                }
                // move store pointer to next element and if it has same key as current one in keys array,
                // just return true so Store can return this element.
                _target.Locker.Lock();
                try
                {
                    if (_target.MoveNext() &&
                        _target.Comparer.Compare(_target.CurrentKey, _source.Current.Key) == 0)
                        return true;
                }
                finally
                {
                    _target.Locker.Unlock();
                }
                while (SourceMoveNextUniqueKey())
                {
                    if (_target.Locker.Invoke(_target.Search, (object)_source.Current.Key, true))
                    {
                        return true;
                    }
                }
                return false;
            }
            private bool SourceMoveNextUniqueKey()
            {
                var k = _source.Current.Key;
                while (_source.MoveNext())
                {
                    if (_target.Comparer.Compare(_source.Current.Key, k) == 0)
                        continue;
                    return true;
                }
                return false;
            }

            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    _target.Locker.Lock();
                    var ce = _target.CurrentEntry;
                    _target.Locker.Unlock();
                    return new KeyValuePair<TKey, TValue>((TKey)ce.Key, (TValue)ce.Value);
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    _target.Locker.Lock();
                    var ce = _target.CurrentEntry;
                    _target.Locker.Unlock();
                    return ce;
                }
            }
        }
        class EnumerableEnumeratorFilter<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
        {
            private ISortedDictionary<TKey, TValue> _target;
            private IEnumerable<KeyValuePair<TKey, TValue>> _source;
            public EnumerableEnumeratorFilter(ISortedDictionary<TKey, TValue> target, 
                IEnumerable<KeyValuePair<TKey, TValue>> source)
            {
                if (target == null)
                    throw new ArgumentNullException("target");
                if (source == null)
                    throw new ArgumentNullException("source");
                _target = target;
                _source = source;
            }
            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
            {
                return new EnumeratorEnumeratorFilter<TKey, TValue>(_target, _source);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new EnumeratorEnumeratorFilter<TKey, TValue>(_target, _source);
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
        /// <param name="store"></param>
        /// <param name="keys"></param>
        /// <returns>IEnumerable that iterates through matching records for the submitted keys.</returns>
        public static IEnumerable<KeyValuePair<TKey, TValue>> Query<TKey, TValue>(
            this IEnumerable<KeyValuePair<TKey, TValue>> store, TKey[] keys)
        {
            return new EnumerableKeysFilter<TKey, TValue>((ISortedDictionary<TKey, TValue>)store, keys);
        }

        public static IEnumerable<KeyValuePair<TKey, TValue>> Query<TKey, TValue>(
            this IEnumerable<KeyValuePair<TKey, TValue>> target, IEnumerable<KeyValuePair<TKey, TValue>> source)
        {
            return new EnumerableEnumeratorFilter<TKey, TValue>((ISortedDictionary<TKey, TValue>)target, source);
        }

    }
}
