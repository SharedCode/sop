// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System.Collections.Generic;

namespace Sop
{
    /// <summary>
    /// non-generic QueryFilterFunc.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public delegate bool QueryFilterFunc(object value);

    /// <summary>
    /// Query Filter Function allows code to submit a function parameter
    /// that contains user defined logic for further refining comparisons
    /// done by the Bulk Query/Remove.
    /// </summary>
    /// <typeparam name="T">type of the Value.</typeparam>
    /// <returns>true will signal SOP the record matches, false otherwise.</returns>
    public delegate bool QueryFilterFunc<in T>(T value);
}
