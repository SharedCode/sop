// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
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
        public class GenericCollection<T> : ICollection<T>
        {
            internal SortedDictionaryOnDisk Collection;

            public GenericCollection(ISortedDictionaryOnDisk collection)
            {
                this.Collection = (SortedDictionaryOnDisk) collection;
            }

            #region ICollection<T> Members

            public void Add(T item)
            {
                throw new InvalidOperationException(string.Format("Can't Add Item Type = {0}.", Collection.ItemType));
            }

            public void Clear()
            {
                Collection.Clear();
            }

            public bool Contains(T item)
            {
                if (Collection.ItemType == Sop.Collections.BTree.ItemType.Key)
                    return Collection.Contains(item);
                return false;
            }

            public void CopyTo(T[] array, int arrayIndex)
            {
                Collection.CopyTo((T[]) array, arrayIndex);
            }

            public long Count
            {
                get { return Collection.Count; }
            }
            int ICollection<T>.Count
            {
                get { return (int)Count; }
            }

            public bool IsReadOnly
            {
                get { return Collection.IsReadOnly; }
            }

            public bool Remove(T item)
            {
                return false;
            }

            #endregion

            #region IEnumerable<T> Members

            public IEnumerator<T> GetEnumerator()
            {
                return new GenericEnumerator<T>(Collection.GetEnumerator());
            }

            #endregion

            #region IEnumerable Members

            IEnumerator IEnumerable.GetEnumerator()
            {
                return Collection.GetEnumerator();
            }

            #endregion
        }
    }
}