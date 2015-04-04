// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;

namespace Sop.OnDisk
{
    using Mru;

    /// <summary>
    /// Enumerates SOP supported Persistence Types
    /// </summary>
    internal enum PersistenceType
    {
        /// <summary>
        /// Unknown Persistent Type
        /// </summary>
        Unknown = -1,

        /// <summary>
        /// Custom serialization.
        /// Samples:
        /// - IPersistent derived class
        /// - XML serialization
        /// - in the future, other custom classified 
        /// serialization methods can be added
        /// </summary>
        Custom,

        /// <summary>
        /// e.g. - int, int32, long, char, byte, short, ushort, float, ufloat, double, udouble,
        /// decimal, uint
        /// </summary>
        SimpleType,

        /// <summary>
        /// Serialize string
        /// </summary>
        String,

        /// <summary>
        /// Serialize a Byte Array.
        /// </summary>
        ByteArray,

        /// <summary>
        /// Binary Serialized data
        /// </summary>
        BinarySerialized,

        /// <summary>
        /// Serialize "null" as a 1 byte value on disk representing null
        /// value for a reference type object.
        /// </summary>
        Null,
        ///// <summary>
        ///// Pass through data block is used for persisting
        ///// Virtual Data Block objects
        ///// </summary>
        //PassThroughBlock
    }
}