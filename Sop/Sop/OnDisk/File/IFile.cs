// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using Sop.OnDisk.IO;
using Sop.Persistence;

namespace Sop.OnDisk.File
{
    /// <summary>
    /// SOP File interface
    /// </summary>
    internal interface IFile : Sop.IFile,
                               IInternalPersistent, IFileEntity
    {
        new string Name { get; }
        new void Flush();

        new void Open();
        new void Close();

        /// <summary>
        /// Collection Growth Size in Number of Blocks
        /// </summary>
        int CollectionGrowthSizeInNob { get; }

        /// <summary>
        /// Create Lookup Table
        /// For Internal use.
        /// </summary>
        /// <returns></returns>
        Algorithm.SortedDictionary.ISortedDictionaryOnDisk CreateLookupTable();

        /// <summary>
        /// Create Lookup Table
        /// For Internal use.
        /// </summary>
        /// <param name="parent"></param>
        /// <returns></returns>
        Algorithm.SortedDictionary.ISortedDictionaryOnDisk CreateLookupTable(IInternalPersistent parent);

        /// <summary>
        /// Returns the Name of the File Object
        /// </summary>
        //string Name { get; }
        /// <summary>
        /// Returns the Default File Stream.
        /// NOTE: the ObjectStore's FileStream is the default FileStream
        /// of the File Object
        /// </summary>
        FileStream DefaultFileStream { get; }

        /// <summary>
        /// Retrieves a Parent of a specific Type.
        /// For Internal use.
        /// </summary>
        /// <param name="parentType"></param>
        /// <returns></returns>
        IInternalPersistent GetParent(Type parentType);

        /// <summary>
        /// Initialize the File
        /// </summary>
        /// <param name="server"></param>
        /// <param name="name"></param>
        /// <param name="filename"></param>
        /// <param name="accessMode"></param>
        /// <param name="profile"></param>
        void Initialize(ObjectServer server, string name, string filename, AccessMode accessMode, Profile profile);

        /// <summary>
        /// Returns the single ObjectStore of the File Object.
        /// NOTE: ObjectStore can store diverse kinds of Items (key/value pairs).
        /// </summary>
        new Algorithm.SortedDictionary.ISortedDictionaryOnDisk Store { get; }

        /// <summary>
        /// Save the changes to the File
        /// </summary>
        //void Flush();
        //void Shrink();
        string ToString();

        /// <summary>
        /// Returns the Transaction Root this File belongs to
        /// </summary>
        Sop.Transaction.ITransactionRoot Transaction { get; }

        /// <summary>
        /// Unbuffered open a file
        /// </summary>
        /// <param name="systemBlockSize"></param>
        /// <returns></returns>
        FileStream UnbufferedOpen(out int systemBlockSize);

        /// <summary>
        /// MRU Minimum Capacity
        /// </summary>
        long MruMinCapacity { get; }

        /// <summary>
        /// MRU Maximum Capacity
        /// </summary>
        long MruMaxCapacity { get; }

        /// <summary>
        /// DeletedCollections is the container of all deleted Collections
        /// within the File Object
        /// </summary>
        IFileRecycler DeletedCollections { get; set; }

        /// <summary>
        /// Object Server this File is a member of its FileSet
        /// </summary>
        new OnDisk.ObjectServer Server { get; set; }

        /// <summary>
        /// Mark not dirty.
        /// For Internal use
        /// </summary>
        void MarkNotDirty();

        ///// <summary>
        ///// For Internal use
        ///// </summary>
        //void PushLookupProfile();
        ///// <summary>
        ///// For Internal use
        ///// </summary>
        //void PopLookupProfile();
        /// <summary>
        /// For Internal use
        /// </summary>
        int CollectionCounter { get; set; }

        /// <summary>
        /// For Internal use
        /// </summary>
        /// <param name="collection"></param>
        void RemoveFromPool(Algorithm.Collection.ICollectionOnDisk collection);

        /// <summary>
        /// For Internal use
        /// </summary>
        /// <param name="collection"></param>
        void AddToPool(Algorithm.Collection.ICollectionOnDisk collection);

        /// <summary>
        /// For Internal use
        /// </summary>
        long Size { get; set; }
    }
}