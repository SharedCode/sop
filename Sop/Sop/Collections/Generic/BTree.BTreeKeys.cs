using System;
using System.Collections.Generic;
using System.Text;

namespace Sop.Collections.Generic.BTree
{
    internal class BTreeKeys<TKey, TValue> : System.Collections.Generic.ICollection<TKey>, IEnumerable<TKey>
    {
        public BTreeKeys(ISortedDictionary<TKey, TValue> dictionary)
        {
            this._dictionary = dictionary;
        }

        public void Add(TKey item)
        {
            throw new InvalidOperationException();
        }

        public void Clear()
        {
            throw new InvalidOperationException();
        }

        public bool Contains(TKey item)
        {
            return _dictionary.Search(item);
        }

        public void CopyTo(TKey[] array, int arrayIndex)
        {
            if (array == null)
                throw new ArgumentNullException("array");
            if (arrayIndex < 0 || arrayIndex >= array.Length)
                throw new ArgumentOutOfRangeException("arrayIndex");
            if (_dictionary.Count > array.Length - arrayIndex)
                throw new InvalidOperationException(
                    "There are more items to copy than elements on target starting from index");
            if (_dictionary.MoveFirst())
            {
                do
                {
                    array[arrayIndex++] = _dictionary.CurrentKey;
                } while (_dictionary.MoveNext());
            }
        }

        public int Count
        {
            get { return _dictionary.Count; }
        }

        public bool IsReadOnly
        {
            get { return true; }
        }

        public bool Remove(TKey item)
        {
            return _dictionary.Remove(item);
        }

        private IEnumerator<TKey> _enumerator;

        public IEnumerator<TKey> GetEnumerator()
        {
            if (_enumerator == null || ((SortedDictionary<TKey, TValue>.BTreeEnumeratorKey)_enumerator).BTree == null)
                _enumerator = new SortedDictionary<TKey, TValue>.BTreeEnumeratorKey(_dictionary);
            return _enumerator;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        private readonly ISortedDictionary<TKey, TValue> _dictionary;
    }
}