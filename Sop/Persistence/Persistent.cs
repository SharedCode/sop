// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System.Runtime.CompilerServices;

[assembly:
    InternalsVisibleTo(
        "Sop.Client, PublicKey=002400000480000094000000060200000024000052534131000400000100010059E60C6300E35F00DD0C6DB33B95A56F30828BD8A9BC96C6161E69FBF12D7DD9D91D4A1829CF0A982522F7E6C8368EAEC9618C538C7D5140A83846A3DA8CFE34A93891C5A8645C8C556B282435FAC1813C61596660FC8065B2D35CC3589EC95BF8EF8A271040398FC0DE1A8D332DEBC37582425B7FCA4C7EA81341F125EEDA98"
        )]
[assembly:
    InternalsVisibleTo(
        "Sop.Server, PublicKey=002400000480000094000000060200000024000052534131000400000100010059E60C6300E35F00DD0C6DB33B95A56F30828BD8A9BC96C6161E69FBF12D7DD9D91D4A1829CF0A982522F7E6C8368EAEC9618C538C7D5140A83846A3DA8CFE34A93891C5A8645C8C556B282435FAC1813C61596660FC8065B2D35CC3589EC95BF8EF8A271040398FC0DE1A8D332DEBC37582425B7FCA4C7EA81341F125EEDA98"
        )]

namespace Sop.Persistence
{
    /// <summary>
    /// Extend/implement Persistent base class to provide
    /// custom serialization methods for persistence. You can 
    /// define or control saving/reading of raw bytes of 
    /// individual fields to/from a given persistence stream
    /// </summary>
    public abstract class Persistent : IPersistent
    {
        /// <summary>
        /// Serialize this Object onto the Stream using a Writer
        /// </summary>
        /// <param name="writer"></param>
        public abstract void Pack(System.IO.BinaryWriter writer);

        /// <summary>
        /// DeSerialize this Object from Stream using a Reader
        /// </summary>
        /// <param name="reader"></param>
        public abstract void Unpack(System.IO.BinaryReader reader);

        /// <summary>
        /// Implement to return true if object can be Disposed.
        /// NOTE: If in Disposed state (true) and it still 
        /// resides in SOP buffers, next time it gets retrieved,
        /// SOP will reload it from Disk.
        /// Return false if your object isn't Disposable and doesn't
        /// need to reload from disk
        /// </summary>
        public virtual bool IsDisposed { get; set; }

        /// <summary>
        /// Implement to tell SOP Container the actual/maximum size of your Object in bytes.
        /// Return 0 if your code can't calculate or doesn't know the size before hand.
        /// </summary>
        public virtual int HintSizeOnDisk
        {
            get { return 0; }
        }
    }
}