using System;
using System.Collections.Generic;
using System.Text;

namespace Sop.Collections.Generic.BTree
{
    internal class BTreeValues<TKey, TValue> : System.Collections.Generic.ICollection<TValue>, IEnumerable<TValue>
    {
        public BTreeValues(ISortedDictionary<TKey, TValue> dictionary)
        {
            this._dictionary = dictionary;
        }

        public void Add(TValue item)
        {
            throw new InvalidOperationException();
        }

        public void Clear()
        {
            throw new InvalidOperationException();
        }

        public bool Contains(TValue item)
        {
            throw new InvalidOperationException();
        }

        public void CopyTo(TValue[] array, int arrayIndex)
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
                    array[arrayIndex++] = _dictionary.CurrentValue;
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

        public bool Remove(TValue item)
        {
            throw new InvalidOperationException();
        }

        public IEnumerator<TValue> GetEnumerator()
        {
            return new SortedDictionary<TKey, TValue>.BTreeEnumeratorValue(_dictionary);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        private readonly ISortedDictionary<TKey, TValue> _dictionary;
    }
}