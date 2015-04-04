using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Sop.Collections.BTree
{
    /// <summary>
    /// B-Tree interface defines the available members of B-Tree manager.
    /// Extends the following .Net Framework interfaces:
    /// System.Collections.IDictionary, System.Collections.ICollection, 
    /// System.Collections.IEnumerable,	System.ICloneable, 
    /// System.Runtime.Serialization.ISerializable, System.Runtime.Serialization.IDeserializationCallback
    /// </summary>
    public interface IBTree : IBTreeBase
#if !DEVICE
                              ,
                              System.Runtime.Serialization.ISerializable,
                              System.Runtime.Serialization.IDeserializationCallback
#endif
    {
        ///// <summary>
        ///// Returns the Synchronized (or Multi-Thread Safe) version of BTree
        ///// </summary>
        ///// <returns>Synchronized BTree object</returns>
        //IBTree Synchronized();
    }
}