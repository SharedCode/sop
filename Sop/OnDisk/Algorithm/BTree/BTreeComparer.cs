// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections;

namespace Sop.OnDisk.Algorithm.BTree
{
    /// <summary>
    /// BTree domain System default comparer. This comparer provides/uses System.Collections.Comparer.Default.Compare function.
    /// </summary>
    internal class BTreeComparer : IComparer
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
            object xKey = x;
            if (x is BTreeItemOnDisk)
                xKey = ((BTreeItemOnDisk) x).Key;
            object yKey = y;
            if (y is BTreeItemOnDisk)
                yKey = ((BTreeItemOnDisk) y).Key;
            return Comparer.Compare(xKey, yKey);
        }

        internal BTreeComparer(IComparer Comparer)
        {
            this.Comparer = Comparer;
        }

        private IComparer Comparer;
    }

    /// <summary>
    /// BTree domain default comparer. This comparer provides numeric and string comparison behavior
    /// </summary>
#if !DEVICE
    [Serializable]
#endif
    internal class BTreeDefaultComparer : IComparer
    {
        private bool IsKeyLong = false;

        /// <summary>
        /// Compare string value of object x's key with object y's key<br/>
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
                object xKey = x;
                if (x is BTreeItemOnDisk)
                    xKey = ((BTreeItemOnDisk) x).Key;
                object yKey = y;
                if (y is BTreeItemOnDisk)
                    yKey = ((BTreeItemOnDisk) y).Key;
                if (IsKeyLong || xKey is long && yKey is long)
                {
                    IsKeyLong = true;
                    long x1, y1;
                    if (xKey is int)
                    {
                        int i = (int) xKey;
                        x1 = i;
                    }
                    else
                        x1 = (long) xKey;
                    if (yKey is int)
                    {
                        int i = (int) yKey;
                        y1 = i;
                    }
                    else
                        y1 = (long) yKey;
                    return x1 < y1 ? -1 : x1 > y1 ? 1 : 0;
                }
                else if (xKey is string && yKey is string)
                    return ((string) xKey).CompareTo((string) yKey);
                else if (xKey is IComparer && yKey is IComparer)
                    return ((IComparer) xKey).Compare(xKey, yKey);
                else if (xKey is ulong && yKey is ulong)
                {
                    ulong x1 = (ulong) xKey, y1 = (ulong) yKey;
                    return x1 < y1 ? -1 : x1 > y1 ? 1 : 0;
                }
                else if (xKey is int && yKey is int)
                {
                    int x1 = (int) xKey, y1 = (int) yKey;
                    return x1 < y1 ? -1 : x1 > y1 ? 1 : 0;
                }
                else if (xKey is uint && yKey is uint)
                {
                    uint x1 = (uint) xKey, y1 = (uint) yKey;
                    return x1 < y1 ? -1 : x1 > y1 ? 1 : 0;
                }
                else if (xKey is float && yKey is float)
                {
                    float x1 = (float) xKey, y1 = (float) yKey;
                    return x1 < y1 ? -1 : x1 > y1 ? 1 : 0;
                }
                else if (xKey is double && yKey is double)
                {
                    double x1 = (double) xKey, y1 = (double) yKey;
                    return x1 < y1 ? -1 : x1 > y1 ? 1 : 0;
                }
                else if (xKey is decimal && yKey is decimal)
                {
                    decimal x1 = (decimal) xKey, y1 = (decimal) yKey;
                    return x1 < y1 ? -1 : x1 > y1 ? 1 : 0;
                }
                else
                    return xKey.ToString().CompareTo(yKey.ToString());
            }
            catch //(Exception e)
            {
                throw new InvalidOperationException("No Comparer Error.");
            }
        }
    }
}