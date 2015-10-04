// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

namespace Sop.Persistence
{
    /// <summary>
    /// For internal use only.
    /// Allows byte level control (via binary writer/reader) of POCO's serialization/deserialization.
    /// </summary>
    public interface IInternalPersistent : IWithHintSize
    {
        /// <summary>
        /// Implement to Pack Persisted Data on a byte array for Serialization.
        /// </summary>
        /// <param name="parent"> </param>
        /// <param name="writer"> </param>
        /// <returns>packed byte array</returns>
        void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer);

        /// <summary>
        /// Given byte array read from stream, read the bytes needed to de-serialize this object by assigning
        /// to appropriate fields of this object the read data.
        /// </summary>
        /// <param name="parent"> </param>
        /// <param name="reader"> </param>
        /// <returns>0 if object completely Unpacked or the size of the Data waiting to be read on the stream.</returns>
        void Unpack(
            IInternalPersistent parent,
            System.IO.BinaryReader reader
            );


        /// <summary>
        /// Disk I/O Buffer
        /// </summary>
        DataBlock DiskBuffer { get; set; }

        /// <summary>
        /// true means this InternalPersistent Object was modified and needing save to disk.
        /// </summary>
        bool IsDirty { get; set; }
    }
}
