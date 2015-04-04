using System;
using System.Collections.Generic;
using System.Text;

// NOTE: Sop.Client namespace is future facing and contains basic interfaces targetted for use in Sop.Client module when it gets implemented.
namespace Sop.Client
{
    /// <summary>
    /// SOP File interface.
    /// A File is a container of objects including Collections on disk.
    /// </summary>
    public interface IFile : IDisposable
    {
        /// <summary>
        /// Returns the size of a Data Block
        /// </summary>
        DataBlockSize DataBlockSize { get; }

        /// <summary>
        /// Returns physical filename of the File object.
        /// </summary>
        string Filename { get; }

        /// <summary>
        /// Returns the name of the File Object as it is registered in Object Server's FileSet
        /// collection.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Returns the File's data segment size.
        /// Each collection on disk that belongs to a file has the same data segment size and they
        /// grow or expand the file by this amount when space is needed to store more data.
        /// </summary>
        /// <returns></returns>
        int GetSegmentSize();

        /// <summary>
        /// Returns the character Encoding used during serialization/deserialization to/from file.
        /// </summary>
        Encoding Encoding { get; }

        /// <summary>
        /// Object Store is a Collection on disk that provides
        /// high speed and scalable item storage, access and manageability.
        /// </summary>
        ISortedDictionaryOnDisk Store { get; }

        /// <summary>
        /// Returns the File Profile.
        /// </summary>
        Profile Profile { get; }

        /// <summary>
        /// Object Server this File belongs.
        /// </summary>
        Sop.IObjectServer Server { get; }

        /// <summary>
        /// Rename the File.
        /// </summary>
        /// <param name="newName"></param>
        void Rename(string newName);
    }
}
