// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections;
using Sop.Persistence;

namespace Sop
{
    /// <summary>
    /// Collection On Disk interface.
    /// </summary>
    public interface ICollectionOnDisk : IInternalPersistent, IBasicIO, ICollection
    {
        /// <summary>
        /// Returns count of items in this Collection on Disk.
        /// </summary>
        new long Count { get; }
        /// <summary>
        /// Get/Set the name of the Collection On Disk Instance
        /// </summary>
        string Name { get; set; }

        /// <summary>
        /// File getter returns the File container
        /// </summary>
        IFile File { get; set; }

        /// <summary>
        /// true if this Collection On Disk is open, otherwise false.
        /// </summary>
        bool IsOpen { get; }

        /// <summary>
        /// Close the collection
        /// </summary>
        void Close();

        /// <summary>
        /// Returns in-memory ID of this collection.
        /// </summary>
        int InMemoryId { get; }

        /// <summary>
        /// Transaction
        /// </summary>
        ITransaction Transaction { get; set; }

        /// <summary>
        /// Sop.DataBlock Size
        /// </summary>
        DataBlockSize DataBlockSize { get; }
    }
}