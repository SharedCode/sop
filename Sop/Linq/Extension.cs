using System;
using System.Collections;
using System.Collections.Generic;
using Sop.SpecializedDataStore;

namespace Sop.Linq
{
    /// <summary>
    /// Extract a key from source item Key/Value pair.
    /// </summary>
    /// <typeparam name="TSourceKey"></typeparam>
    /// <typeparam name="TSourceValue"></typeparam>
    /// <typeparam name="TKey"></typeparam>
    /// <param name="sourceKey"></param>
    /// <param name="sourceValue"></param>
    /// <returns></returns>
    public delegate TKey ExtractKey<TSourceKey, TSourceValue, out TKey>(KeyValuePair<TSourceKey, TSourceValue> item);
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
            private bool _lockWrap;

            public EnumeratorKeysFilter(ISortedDictionary<TKey, TValue> store, TKey[] keys, bool lockWrap = false)
            {
                if (keys == null || keys.Length == 0)
                    throw new ArgumentNullException("keys");
                if (store == null)
                    throw new ArgumentNullException("store");
                _lockWrap = lockWrap;
                if (lockWrap)
                    store.Locker.Lock(OperationType.Read);
                _store = (ISortedDictionaryOnDisk)((SpecializedStoreBase)store).Collection.Clone();
                _keys = keys;
            }

            public void Dispose()
            {
                if (_store == null) return;
                if (_lockWrap)
                {
                    _store.Dispose();
                    _store.Locker.Unlock(OperationType.Read);
                }
                else
                {
                    _store.Locker.Invoke(() =>
                    {
                        _store.Dispose();
                    }, OperationType.Read);
                }
                _store = null;
            }

            public void Reset()
            {
                _keyIndex = -1;
            }
            public bool MoveNext()
            {
                currentEntry = default(DictionaryEntry);
                // if index points to pre-1st element...
                if (_keyIndex < 0)
                {
                    _keyIndex++;
                    _store.Locker.Lock(OperationType.Read);
                    try
                    {
                        if (_store.Search(_keys[_keyIndex], true))
                        {
                            currentEntry = _store.CurrentEntry;
                            return true;
                        }
                    }
                    finally
                    {
                        _store.Locker.Unlock(OperationType.Read);
                    }

                    while (++_keyIndex <= _keys.Length - 1)
                    {
                        _store.Locker.Lock(OperationType.Read);
                        try
                        {
                            if (_store.Search(_keys[_keyIndex], true))
                            {
                                currentEntry = _store.CurrentEntry;
                                return true;
                            }
                        }
                        finally
                        {
                            _store.Locker.Unlock(OperationType.Read);
                        }
                    }
                    return false;
                }
                // move store pointer to next element and if it has same key as current one in keys array,
                // just return true so Store can return this element.
                _store.Locker.Lock(OperationType.Read);
                try
                {

                    if (_store.MoveNext() &&
                        _store.Comparer.Compare(_store.CurrentKey, _keys[_keyIndex]) == 0)
                    {
                        currentEntry = _store.CurrentEntry;
                        return true;
                    }
                }
                finally
                {
                    _store.Locker.Unlock(OperationType.Read);
                }
                while (++_keyIndex <= _keys.Length - 1)
                {
                    _store.Locker.Lock(OperationType.Read);
                    try {
                        if (_store.Search(_keys[_keyIndex], true))
                        {
                            currentEntry = _store.CurrentEntry;
                            return true;
                        }
                    }
                    finally
                    {
                        _store.Locker.Unlock(OperationType.Read);
                    }
                }
                return false;
            }
            private DictionaryEntry currentEntry;
            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    var ce = currentEntry;
                    if (ce.Key == null)
                        return new KeyValuePair<TKey, TValue>(default(TKey), default(TValue));
                    return new KeyValuePair<TKey, TValue>((TKey)ce.Key, (TValue)ce.Value);
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return currentEntry;
                }
            }
        }
        class EnumerableKeysFilter<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
        {
            private ISortedDictionary<TKey, TValue> _store;
            private TKey[] _keys;
            private bool _lockWrap;
            public EnumerableKeysFilter(ISortedDictionary<TKey, TValue> store, TKey[] keys, bool lockWrap = false)
            {
                if (keys == null)
                    throw new ArgumentNullException("keys");
                if (store == null)
                    throw new ArgumentNullException("store");
                _store = store;
                _keys = keys;
                _lockWrap = lockWrap;
            }
            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
            {
                return new EnumeratorKeysFilter<TKey, TValue>(_store, _keys, _lockWrap);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new EnumeratorKeysFilter<TKey, TValue>(_store, _keys, _lockWrap);
            }
        }

        class EnumeratorEnumeratorFilter<TKey, TValue, TSourceKey, TSourceValue> : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            private ISortedDictionaryOnDisk _target;
            private IEnumerator<KeyValuePair<TSourceKey, TSourceValue>> _source;
            private ExtractKey<TSourceKey, TSourceValue, TKey> _sourceKeyExtractor;
            private bool _lockWrap = false;
            private bool _wasReset = true;

            public EnumeratorEnumeratorFilter(ISortedDictionary<TKey, TValue> target,
                IEnumerable<KeyValuePair<TSourceKey, TSourceValue>> source,
                ExtractKey<TSourceKey, TSourceValue, TKey> sourceKeyExtractor = null,
                bool lockWrap = false)
            {
                if (target == null)
                    throw new ArgumentNullException("target");
                if (source == null)
                    throw new ArgumentNullException("source");

                if (lockWrap)
                    target.Locker.Lock(OperationType.Read);
                _target = (ISortedDictionaryOnDisk)((SpecializedStoreBase)target).Collection.Clone();
                _source = source.GetEnumerator();
                _sourceKeyExtractor = sourceKeyExtractor;
                _lockWrap = lockWrap;
                if (_sourceKeyExtractor == null)
                    _sourceKeyExtractor = DefaultKeyExtractor<TSourceKey, TSourceValue, TKey>;
            }

            public void Dispose()
            {
                if (_target == null) return;
                if (_lockWrap)
                {
                    _target.Dispose();
                    _target.Locker.Unlock(OperationType.Read);
                }
                else
                {
                    _target.Locker.Invoke(() =>
                    {
                        _target.Dispose();
                    }, OperationType.Read);
                }
                _target = null;
                _source.Dispose();
                _source = null;
            }

            public void Reset()
            {
                _wasReset = true;
                _source.Reset();
            }
            public bool MoveNext()
            {
                currentEntry = default(DictionaryEntry);
                // if index points to pre-1st element...
                if (_wasReset)
                {
                    _wasReset = false;
                    if (!_source.MoveNext())
                        return false;
                    _target.Locker.Lock(OperationType.Read);
                    try
                    {
                        if (_target.Search(_sourceKeyExtractor(_source.Current), true))
                        {
                            currentEntry = _target.CurrentEntry;
                            return true;
                        }
                    }
                    finally
                    {
                        _target.Locker.Unlock(OperationType.Read);
                    }
                    while (_source.MoveNext())
                    {
                        _target.Locker.Lock(OperationType.Read);
                        try
                        {
                            if (_target.Search(_sourceKeyExtractor(_source.Current), true))
                            {
                                currentEntry = _target.CurrentEntry;
                                return true;
                            }
                        }
                        finally
                        {
                            _target.Locker.Unlock(OperationType.Read);
                        }
                    }
                    return false;
                }
                // move store pointer to next element and if it has same key as current one in keys array,
                // just return true so Store can return this element.
                _target.Locker.Lock(OperationType.Read);
                try
                {
                    if (_target.MoveNext() &&
                        _target.Comparer.Compare(_target.CurrentKey, _sourceKeyExtractor(_source.Current)) == 0)
                    {
                        currentEntry = _target.CurrentEntry;
                        return true;
                    }
                }
                finally
                {
                    _target.Locker.Unlock(OperationType.Read);
                }
                while (SourceMoveNextUniqueKey())
                {
                    _target.Locker.Lock(OperationType.Read);
                    try
                    {
                        if (_target.Search(_sourceKeyExtractor(_source.Current), true))
                        {
                            currentEntry = _target.CurrentEntry;
                            return true;
                        }
                    }
                    finally
                    {
                        _target.Locker.Unlock(OperationType.Read);
                    }
                }
                return false;
            }
            private bool SourceMoveNextUniqueKey()
            {
                var k = _sourceKeyExtractor(_source.Current);
                while (_source.MoveNext())
                {
                    if (_target.Comparer.Compare(_sourceKeyExtractor(_source.Current), k) == 0)
                        continue;
                    return true;
                }
                return false;
            }
            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    var ce = currentEntry;
                    return new KeyValuePair<TKey, TValue>((TKey)ce.Key, (TValue)ce.Value);
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return currentEntry;
                }
            }

            private DictionaryEntry currentEntry;
        }

        class EnumerableEnumeratorFilter<TKey, TValue, TSourceKey, TSourceValue> : IEnumerable<KeyValuePair<TKey, TValue>>
        {
            private ISortedDictionary<TKey, TValue> _target;
            private IEnumerable<KeyValuePair<TSourceKey, TSourceValue>> _source;
            private ExtractKey<TSourceKey, TSourceValue, TKey> _sourceKeyExtractor;
            private bool _lockWrap = false;
            public EnumerableEnumeratorFilter(ISortedDictionary<TKey, TValue> target, 
                IEnumerable<KeyValuePair<TSourceKey, TSourceValue>> source,
                ExtractKey<TSourceKey, TSourceValue, TKey> sourceKeyExtractor = null,
                bool lockWrap = false)
            {
                if (target == null)
                    throw new ArgumentNullException("target");
                if (source == null)
                    throw new ArgumentNullException("source");
                _target = target;
                _source = source;
                _sourceKeyExtractor = sourceKeyExtractor;
                _lockWrap = lockWrap;
            }
            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
            {
                return new EnumeratorEnumeratorFilter<TKey, TValue, TSourceKey, TSourceValue>(_target, _source, _sourceKeyExtractor, _lockWrap);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new EnumeratorEnumeratorFilter<TKey, TValue, TSourceKey, TSourceValue>(_target, _source, _sourceKeyExtractor, _lockWrap);
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
        /// <param name="lockWrap">true will wrap returned IEnumerator in lock (on ctor) - unlock (on dispose) calls.</param>
        /// <returns>IEnumerable that iterates through matching records for the submitted keys.</returns>
        public static IEnumerable<KeyValuePair<TKey, TValue>> Query<TKey, TValue>(
            this IEnumerable<KeyValuePair<TKey, TValue>> store, TKey[] keys, bool lockWrap = false)
        {
            return new EnumerableKeysFilter<TKey, TValue>((ISortedDictionary<TKey, TValue>)store, keys, lockWrap);
        }

        public static IEnumerable<KeyValuePair<TKey, TValue>> Query<TKey, TValue, TSourceKey, TSourceValue>(
            this IEnumerable<KeyValuePair<TKey, TValue>> target, 
            IEnumerable<KeyValuePair<TSourceKey, TSourceValue>> source,
            ExtractKey<TSourceKey, TSourceValue, TKey>  sourceKeyExtractor = null,
            bool lockWrap = false
            )
        {
            return new EnumerableEnumeratorFilter<TKey, TValue, TSourceKey, TSourceValue>(
                (ISortedDictionary<TKey, TValue>)target, source, sourceKeyExtractor, lockWrap);
        }

        private static TKey DefaultKeyExtractor<TSourceKey, TSourceValue, TKey>(KeyValuePair<TSourceKey, TSourceValue> item)
        {
            if (typeof(TKey) == typeof(TSourceValue))
                return (TKey)(object)item.Value;
            if (typeof(TKey) == typeof(TSourceKey))
                return (TKey)(object)item.Key;
            throw new SopException("DefaultKeyExtractor can only extract from Source Value or Key that has the same type as result type TKey.");
        }
    }
}
