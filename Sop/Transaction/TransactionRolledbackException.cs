// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;

namespace Sop.Transaction
{
    /// <summary>
    /// Transaction Rolledback Exception
    /// </summary>
    public class TransactionRolledbackException : Exception
    {
        public TransactionRolledbackException()
        {
        }

        public TransactionRolledbackException(string message) : base(message)
        {
        }

        public TransactionRolledbackException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public TransactionRolledbackException(System.Runtime.Serialization.SerializationInfo info,
                                              System.Runtime.Serialization.StreamingContext context)
            : base(info, context)
        {
        }
    }
}