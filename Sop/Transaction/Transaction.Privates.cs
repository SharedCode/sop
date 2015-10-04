// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.IO;

namespace Sop.Transaction
{
    using OnDisk;

    internal partial class Transaction
    {
        private class RecordKeyComparer2<T> : IComparer<T>
            where T : RecordKey
        {
            /// <summary>
            /// Compares two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
            /// </summary>
            /// <returns>
            /// Value 
            ///                     Condition 
            ///                     Less than zero
            ///                 <paramref name="x"/> is less than <paramref name="y"/>.
            ///                     Zero
            ///                 <paramref name="x"/> equals <paramref name="y"/>.
            ///                     Greater than zero
            ///                 <paramref name="x"/> is greater than <paramref name="y"/>.
            /// </returns>
            /// <param name="x">The first object to compare.
            ///                 </param><param name="y">The second object to compare.
            ///                 </param>
            public int Compare(T x, T y)
            {
                T xKey = x;
                T yKey = y;
                //xKey.ServerSystemFilename 
                //xKey.Filename
                //xKey.CollectionName
                string a = xKey.ServerSystemFilename + xKey.Filename + xKey.CollectionName;
                string b = yKey.ServerSystemFilename + yKey.Filename + yKey.CollectionName;

                return String.Compare(a, b, System.StringComparison.OrdinalIgnoreCase);
                //if (a == b)
                //    return xKey.Address == yKey.Address;
                //return false;
            }
        }

        internal class RecordKeyComparer<T> : IComparer<T>
            where T : RecordKey
        {
            public int Compare(T x, T y)
            {
                RecordKey xKey = x;
                RecordKey yKey = y;
                //xKey.ServerSystemFilename 
                //xKey.Filename
                //xKey.CollectionName
                //xKey.Address
                string a = xKey.ServerSystemFilename + xKey.Filename + xKey.CollectionName;
                string b = yKey.ServerSystemFilename + yKey.Filename + yKey.CollectionName;
                int r = String.CompareOrdinal(a, b);
                return r == 0 ? xKey.Address.CompareTo(yKey.Address) : r;
            }
        }
        internal class RecordKeyComparer : IComparer
        {
            public int Compare(object x, object y)
            {
                RecordKey xKey = (RecordKey) x;
                RecordKey yKey = (RecordKey) y;
                //xKey.ServerSystemFilename 
                //xKey.Filename
                //xKey.CollectionName
                //xKey.Address
                string a = xKey.ServerSystemFilename + xKey.Filename + xKey.CollectionName;
                string b = yKey.ServerSystemFilename + yKey.Filename + yKey.CollectionName;
                int r = String.CompareOrdinal(a, b);
                return r == 0 ? xKey.Address.CompareTo(yKey.Address) : r;
            }
        }

        internal class FileSegmentComparer<T> : IComparer<T>
            where T : RecordKey
        {
            public int Compare(T x, T y)
            {
                RecordKey xKey = x;
                RecordKey yKey = y;
                //xKey.ServerSystemFilename 
                //xKey.Filename
                //xKey.Address
                string a = xKey.ServerSystemFilename + xKey.Filename;
                string b = yKey.ServerSystemFilename + yKey.Filename;
                int r = System.String.CompareOrdinal(a, b);
                return r == 0 ? xKey.Address.CompareTo(yKey.Address) : r;
            }
        }
        internal class FileSegmentComparer : IComparer
        {
            public int Compare(object x, object y)
            {
                RecordKey xKey = (RecordKey) x;
                RecordKey yKey = (RecordKey) y;
                //xKey.ServerSystemFilename 
                //xKey.Filename
                //xKey.Address
                string a = xKey.ServerSystemFilename + xKey.Filename;
                string b = yKey.ServerSystemFilename + yKey.Filename;
                int r = System.String.CompareOrdinal(a, b);
                return r == 0 ? xKey.Address.CompareTo(yKey.Address) : r;
            }
        }
    }
}