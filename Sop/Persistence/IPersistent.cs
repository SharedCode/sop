// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

namespace Sop.Persistence
{
    /// <summary>
    /// IPersistent interface to provide
    /// custom serialization methods for persistence. You can 
    /// save/read bytes of individual fields to/from persistence stream.
    /// </summary>
    public interface IPersistent : IWithHintSize
    {
        /// <summary>
        /// Serialize this Object onto the Stream using a Writer.
        /// </summary>
        /// <param name="writer"></param>
        void Pack(System.IO.BinaryWriter writer);

        /// <summary>
        /// DeSerialize this Object from Stream using a Reader.
        /// </summary>
        /// <param name="reader"></param>
        void Unpack(System.IO.BinaryReader reader);

        /// <summary>
        /// Return true if object can be Disposed(is disposed!) and it needs to be reloaded from Disk.
        /// Return false if your object isn't Disposable and doesn't need to reload from disk.
        /// 
        /// NOTE: If in Disposed state (true) and it still resides in SOP buffers, next time it 
        /// gets retrieved from the Container, SOP will reload it from Disk.
        /// </summary>
        bool IsDisposed { get; set; }
    }
}
