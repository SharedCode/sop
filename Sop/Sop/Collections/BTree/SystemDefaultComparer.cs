using System;
using System.Collections;

namespace Sop.Collections.BTree
{
    /// <summary>
    /// BTree domain System default comparer. This comparer provides/uses System.Collections.Comparer.Default.Compare function.
    /// </summary>
#if !DEVICE
    [Serializable]
#endif
    internal class SystemDefaultComparer : IComparer
    {
        /// <summary>
        /// Compare object x's key with object y's key.<br/>
        /// Returns:<br/>
        ///		&lt; 0 if x.Key is &lt; y.Key<br/>
        ///		&gt; 0 if x.Key &gt; y.Key<br/>
        ///		== 0 if x.Key == y.Key
        /// </summary>
        /// <param name="x">1st object whose key is to be compared</param>
        /// <param name="y">2nd object whose key is to be compared</param>
        /// <returns></returns>
        public int Compare(object x, object y)
        {
            try
            {
                if (_isComparingObject)
                {
                    int xHash = x.GetHashCode();
                    int yHash = y.GetHashCode();
                    return xHash.CompareTo(yHash);
                }
                object xKey = x;
                if (x is DictionaryEntry)
                    xKey = ((DictionaryEntry) x).Key;
                object yKey = y;
                if (y is DictionaryEntry)
                    yKey = ((DictionaryEntry) y).Key;
                return System.Collections.Comparer.Default.Compare(xKey, yKey);
            }
            catch (Exception e)
            {
                if (!(x is ValueType || _isComparingObject))
                {
                    _isComparingObject = true;
                    if (x == null)
                        throw new ArgumentNullException("x");
                    if (y == null)
                        throw new ArgumentNullException("y");
                    int xHash = x.GetHashCode();
                    int yHash = y.GetHashCode();
                    return xHash.CompareTo(yHash);
                }
                throw new InvalidOperationException("No Comparer Error", e);
            }
        }

        private bool _isComparingObject;
    }
}