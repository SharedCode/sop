// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Sop
{
    /// <summary>
    /// Generic Query Result.
    /// </summary>
    /// <typeparam name="TKey"> </typeparam>
    public struct QueryResult<TKey>
    {
        public QueryResult(TKey key, QueryResult source)
        {
            Found = source.Found;
            Value = source.Value;
            Key = key;
        }
        /// <summary>
        /// true means the Key refered to by this instance was found,
        /// otherwise false
        /// </summary>
        public bool Found;

        /// <summary>
        /// Contains the Key of the Item to Search for
        /// </summary>
        public TKey Key;

        /// <summary>
        /// Contains the Item's Value if it was found
        /// </summary>
        public object Value;

        /// <summary>
        /// Convert to non-generic QueryResult.
        /// </summary>
        /// <returns></returns>
        public QueryResult Convert()
        {
            return new QueryResult() {Found = Found, Value = Value};
        }
        /// <summary>
        /// Convert array of generic QueryResult into non-generic.
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public static QueryResult[] Convert(QueryResult<TKey>[] source)
        {
            var r = new QueryResult[source.Length];
            for (int i = 0; i < source.Length; i++)
                r[i] = source[i].Convert();
            return r;
        }
    }

    /// <summary>
    /// Query Result.
    /// </summary>
    public struct QueryResult
    {
        /// <summary>
        /// true means Key is found.
        /// </summary>
        public bool Found;

        /// <summary>
        ///  Value of the matching item.
        /// </summary>
        public object Value;
    }
}