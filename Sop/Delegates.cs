// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// On Object Unpack event is invoked when User defined Object
    /// data is read from disk and SOP needs to DeSerialize it.
    /// User code is given chance to do its custom 
    /// DeSerialization of the Object from a given Stream
    /// </summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    public delegate object OnObjectUnpack(System.IO.BinaryReader reader);

    /// <summary>
    /// On Object Pack event is invoked when User defined Object
    /// needs to be Serialized to a Stream as preparation for persistence to Disk
    /// </summary>
    /// <param name="writer">Writer for a Target Stream</param>
    /// <param name="objectToPack">Object to be Serialized or Packed</param>
    public delegate void OnObjectPack(System.IO.BinaryWriter writer, object objectToPack);

    /// <summary>
    /// Data Store types.
    /// 
    /// NOTE: only SOP on disk store type is currently supported.
    /// Other type(s) will be supported depends on market demands as they arise.
    /// </summary>
    internal enum DataStoreType
    {
        /// <summary>
        /// SOP on Disk, default.
        /// </summary>
        SopOndisk,
        /*
		Default = SopOndisk,
		/// <summary>
		/// SOP cluster. NOT Supported yet..
		/// </summary>
		SopCluster,
		/// <summary>
		/// SQL Server. NOT Supported yet..
		/// </summary>
		SqlServer,
		/// <summary>
		/// Oracle. NOT Supported yet..
		/// </summary>
		Oracle
		 */
    }
}