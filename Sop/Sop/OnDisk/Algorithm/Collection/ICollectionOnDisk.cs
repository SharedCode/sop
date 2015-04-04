// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using Sop.OnDisk.File;
using Sop.Persistence;

namespace Sop.OnDisk.Algorithm.Collection
{
    /// <summary>
    /// Collection On Disk interface
    /// </summary>
    internal interface ICollectionOnDisk : Sop.ICollectionOnDisk, ICollectionCache
    {
        /// <summary>
        /// Register the change(s) done. e.g. - after add/update/remove of an object,
        /// the code needs to call Collection On Disk's "RegisterChange" function.
        /// </summary>
        void RegisterChange(bool partialRegister = false);

        /// <summary>
        /// Returns true if Value was not loaded using known Serialization techniques
        /// and is awaiting custom DeSerialization by your code
        /// </summary>
        bool IsValueInStream { get; }

        /// <summary>
        /// Is Collection a transaction store or not.
        /// </summary>
        bool IsTransactionStore { get; }

        /// <summary>
        /// Returns the Collection's Stream Reader
        /// </summary>
        System.IO.BinaryReader StreamReader { get; }

        /// <summary>
        /// File getter returns the File container
        /// </summary>
        new File.IFile File { get; set; }

        /// <summary>
        /// Load the collection
        /// </summary>
        void Load();

        /// <summary>
        /// Internal use only: OnCommit is invoked to allow some Collection state to be finalized during commit phase
        /// </summary>
        void OnCommit();

        /// <summary>
        /// Internal use only: OnRollback is invoked to allow Collection to undo some state during rollback phase
        /// </summary>
        void OnRollback();

        /// <summary>
        /// Move file pointer to a disk offset specified by Address
        /// </summary>
        /// <param name="dataAddress"></param>
        /// <returns></returns>
        bool MoveTo(long dataAddress);


        /// <summary>
        /// Transaction Logger
        /// </summary>
        new Transaction.ITransactionLogger Transaction { get; set; }

        /// <summary>
        /// On Max Capacity handler method
        /// </summary>
        /// <param name="countOfBlocksUnloadToDisk"></param>
        /// <returns></returns>
        int OnMaxCapacity(int countOfBlocksUnloadToDisk);

        /// <summary>
        /// For internal use: true means collection is Unloading, otherwise false.
        /// </summary>
        bool IsUnloading { get; set; }
        /// <summary>
        /// For internal use: true means collection was a product of cloning, otherwise false.
        /// </summary>
        bool IsCloned { get; set; }

        /// <summary>
        /// FileStream used in reading data from file. Write is done via BulkWriter which uses
        /// its own set of FileStreams.
        /// </summary>
        FileStream FileStream { get; set; }

        /// <summary>
        /// Returns the Current entry
        /// </summary>
        object CurrentEntry { get; }

        /// <summary>
        /// Address on disk of current entry's data
        /// </summary>
        long CurrentEntryDataAddress { get; set; }

        /// <summary>
        /// Traverse the Parent hierarchy and look for a Parent of a given Type.
        /// Example, one can look for the "File" container of a Collection or a Parent
        /// Collection of a Collection and so on and so forth..
        /// </summary>
        /// <param name="parentType"> </param>
        /// <returns></returns>
        IInternalPersistent GetParent(Type parentType);

        /// <summary>
        /// Parent of this object can be another Collection or File object.
        /// </summary>
        IInternalPersistent Parent { get; set; }
    }
}