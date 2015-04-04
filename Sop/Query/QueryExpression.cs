// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;

namespace Sop
{
    /// <summary>
    /// Query expression. Exact key matching and an option to 
    /// filter on an entry's value is supported.
    /// LINQ queries may be supported in future.
    /// </summary>
    public struct QueryExpression
    {
        /// <summary>
        /// Set this to value of the Key to be searched.
        /// </summary>
        public object Key;
        /// <summary>
        /// (optional) Set this to a user defined filter function
        /// that can further refine the query matching.
        /// </summary>
        public QueryFilterFunc<object> ValueFilterFunc;
    }

    /// <summary>
    /// Generic Query expression. Exact key matching and an option to 
    /// filter on an entry's value is supported.
    /// LINQ queries may be supported in future.
    /// </summary>
    /// <typeparam name="TKey"> </typeparam>
    public struct QueryExpression<TKey>
    {
        /// <summary>
        /// Set this to value of the Key to be searched.
        /// </summary>
        public TKey Key;
        /// <summary>
        /// (optional) Set this to a user defined filter function
        /// that can further refine the query matching, extend it to do matching
        /// on the Value object in the Key/Value pair entry of the Store.
        /// </summary>
        public QueryFilterFunc<object> ValueFilterFunc;

        /// <summary>
        /// Package a given set of Keys as an array of QueryExpressions.
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public static QueryExpression<TKey>[] Package(TKey[] keys, 
            QueryFilterFunc<object> filterFunction = null)
        {
            if (keys == null || keys.Length == 0)
                throw new ArgumentNullException("keys");

            var r = new QueryExpression<TKey>[keys.Length];
            for(int i = 0; i < keys.Length; i++)
            {
                r[i].Key = keys[i];
                r[i].ValueFilterFunc = filterFunction;
            }
            return r;
        }
        #region for removal
        //public static QueryExpression<TKey>[] Package(IEnumerable<TKey> keys, QueryFilterFunc<object> filterFunction = null)
        //{
        //    if (keys == null)
        //        throw new ArgumentNullException("keys");

        //    var r = new List<QueryExpression<TKey>>();
        //    foreach(var k in keys)
        //    {
        //        var q = new QueryExpression<TKey>()
        //        {
        //            Key = k,
        //            ValueFilterFunc = filterFunction
        //        };
        //    }
        //    return r.ToArray();
        //}
        #endregion

        /// <summary>
        /// Convert to non-generic QueryExpression.
        /// </summary>
        /// <returns></returns>
        public QueryExpression Convert()
        {
            return new QueryExpression() {Key = Key, ValueFilterFunc = ValueFilterFunc};
        }
        /// <summary>
        /// Convert array of generic QueryExpression into non-generic.
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public static QueryExpression[] Convert(QueryExpression<TKey>[] source)
        {
            var r = new QueryExpression[source.Length];
            for (int i = 0; i < source.Length; i++)
                r[i] = source[i].Convert();
            return r;
        }
    }
}