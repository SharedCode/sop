// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;

namespace Sop
{
    /// <summary>
    /// Generic Comparer wrapper.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public class GenericComparer<TKey> : IComparer
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="comparer"></param>
        public GenericComparer(IComparer<TKey> comparer)
        {
            if (comparer == null)
                throw new ArgumentNullException("comparer");
            this.Comparer = comparer;
        }

        /// <summary>
        /// Compare a key with another
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        public int Compare(object x, object y)
        {
            return Comparer.Compare((TKey) x, (TKey) y);
        }

        /// <summary>
        /// The real Comparer.
        /// </summary>
        public IComparer<TKey> Comparer;
    }
}