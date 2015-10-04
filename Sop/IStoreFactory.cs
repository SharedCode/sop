// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using Sop.Persistence;

namespace Sop
{
    /// <summary>
    /// Store Factory interface.
    /// 
    /// Supported Serialization techniques are:
    /// * Basic (simple) type Serialization
    /// * Object SOP.IPersistent implements
    /// * Object Xml Serialization
    /// </summary>
    public interface IStoreFactory
    {
        /// <summary>
        /// Auto Dispose Store when it gets removed from Cache.
        /// </summary>
        bool AutoDisposeItem { get; set; }

        /// <summary>
        /// Create/Get a general purpose Data Store. Key and/or Value types can be any of the following data types:
        ///     - simple type: e.g. - int, short, string, char, float, etc...
        ///     - IPersistent interface implementation.
        ///     - Xml Serializable object.
        /// Simple & IPersistent typed data are stored in non-Xml format for space optimization reasons.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="createIfNotExist"></param>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        /// <param name="mruManaged"> </param>
        /// <param name="isUnique"></param>
        /// <returns></returns>
        ISortedDictionary<TKey, TValue> Get<TKey, TValue>(object container, string name,
                System.Collections.Generic.IComparer<TKey> comparer = null, bool createIfNotExist = true,
                bool isDataInKeySegment = true, bool mruManaged = true, bool isUnique = false);

        /// <summary>
        /// Remove a Data Store from its container.
        /// </summary>
        /// <param name="container">data store container</param>
        /// <param name="name">name of the Data Store to remove</param>
        void Remove(object container, string name);

        /// <summary>
        /// Returns true if Store with name is found in the Container, otherwise false.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        bool Contains(object container, string name);

        /// <summary>
        /// Create/Get Data Store for both IPersistent Typed Key and Value
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="createIfNotExist"></param>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        /// <param name="mruManaged"> </param>
        /// <param name="isUnique"></param>
        /// <returns></returns>
        ISortedDictionary<TKey, TValue> GetPersistent<TKey, TValue>(object container, string name,
                                                                                      System.Collections.Generic.IComparer<TKey> comparer = null,
                                                                                      bool createIfNotExist = true, bool isDataInKeySegment = true,
                                                                                      bool mruManaged = true, bool isUnique = false)
            where TKey : IPersistent, new()
            where TValue : IPersistent, new();

        /// <summary>
        /// Create/Get Data Store for IPersistent typed Key and "Simple typed" Value.
        /// NOTE: Simple type means one of the integer, numeric(decimal, float...), char data types, byte array
        /// or a string
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="createIfNotExist"></param>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        /// <param name="mruManaged"> </param>
        /// <param name="isUnique"></param>
        /// <returns></returns>
        ISortedDictionary<TKey, TValue> GetPersistentKey<TKey, TValue>(object container, string name,
                                                                                      System.Collections.Generic.IComparer<TKey> comparer = null,
                                                                                      bool createIfNotExist = true, bool isDataInKeySegment = true,
                                                                                      bool mruManaged = true, bool isUnique = false)
            where TKey : IPersistent, new();

        /// <summary>
        /// Create/Get Data Store for IPersistent typed Value and "Simple typed" Key.
        /// NOTE: Simple type means one of the integer, numeric(decimal, float,...), char data types, byte array
        /// or a string
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="createIfNotExist"></param>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        /// <param name="mruManaged"> </param>
        /// <param name="isUnique"></param>
        /// <returns></returns>
        ISortedDictionary<TKey, TValue> GetPersistentValue<TKey, TValue>(object container, string name,
                                                                                      System.Collections.Generic.IComparer<TKey> comparer = null,
                                                                                      bool createIfNotExist = true, bool isDataInKeySegment = true,
                                                                                      bool mruManaged = true, bool isUnique = false)
            where TValue : IPersistent, new();
    }
}
