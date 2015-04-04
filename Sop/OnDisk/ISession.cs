// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using Sop.OnDisk.Algorithm.SortedDictionary;

namespace Sop.OnDisk
{
    /// <summary>
    /// Session interface.
    /// </summary>
    internal interface ISession
    {
        /// <summary>
        /// ID of the session.
        /// </summary>
        int Id { get; set; }
        /// <summary>
        /// Session Transaction.
        /// </summary>
        Transaction.ITransactionLogger Transaction { get; set; }
        /// <summary>
        /// Register open/create of an object server.
        /// </summary>
        /// <param name="objectServer"></param>
        void Register(ObjectServer objectServer);
        /// <summary>
        /// Register open/create of a dictionary on disk.
        /// </summary>
        /// <param name="dictionaryOnDisk"></param>
        void Register(SortedDictionaryOnDisk dictionaryOnDisk);
        /// <summary>
        /// Unregister an object server.
        /// </summary>
        /// <param name="objectServer"></param>
        void UnRegister(ObjectServer objectServer);
        /// <summary>
        /// Unregister a sorted dictionary on disk.
        /// </summary>
        /// <param name="dictionaryOnDisk"></param>
        void UnRegister(SortedDictionaryOnDisk dictionaryOnDisk);
    }
}