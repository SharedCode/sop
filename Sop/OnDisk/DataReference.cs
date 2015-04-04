// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using Sop.Persistence;

namespace Sop.OnDisk
{
    /// <summary>
    /// Collection Reference class.
    /// Used for persisting collection reference information part of 
    /// transaction support.
    /// </summary>
    internal class CollectionReference : InternalPersistent
    {
        /// <summary>
        /// Name of Collection on Disk that contains the Data
        /// </summary>
        public string CollectionName = string.Empty;

        /// <summary>
        /// Filename where the data is stored
        /// </summary>
        public string Filename = string.Empty;

        /// <summary>
        /// Object Server System filename
        /// </summary>
        public string ServerSystemFilename = string.Empty;

        public override string ToString()
        {
            return string.Format("{0}.{1}.{2}", ServerSystemFilename, Filename, CollectionName);
        }

        public override void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            writer.Write(CollectionName);
            writer.Write(Filename);
            writer.Write(ServerSystemFilename);
        }

        public override void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            CollectionName = reader.ReadString();
            Filename = reader.ReadString();
            ServerSystemFilename = reader.ReadString();
        }
    }

    /// <summary>
    /// Data Reference class
    /// </summary>
    internal class DataReference : CollectionReference
    {
        /// <summary>
        /// Offset in file where the first byte of data is stored
        /// </summary>
        public long Address = -1;

        public override string ToString()
        {
            return string.Format("{0}.{1}", base.ToString(), Address);
        }

        public override void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer)
        {
            base.Pack(parent, writer);
            writer.Write(Address);
        }

        public override void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            base.Unpack(parent, reader);
            Address = reader.ReadInt64();
        }
    }
}