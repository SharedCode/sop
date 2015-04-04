// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using Sop.Persistence;

namespace Sop.OnDisk.Algorithm.BTree
{
    /// <summary>
    /// BTree Item On Disk interface
    /// </summary>
    internal interface IBTreeItemOnDisk
    {
        /// <summary>
        /// Clone the item
        /// </summary>
        /// <returns></returns>
        object Clone();

        /// <summary>
        /// Hint for the Item's Size on disk
        /// </summary>
        int HintSizeOnDisk { get; }

        /// <summary>
        /// true means item is dirty and needs to be written to disk,
        /// otherwise false
        /// </summary>
        bool IsDirty { get; set; }

        /// <summary>
        /// Pack the item
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="writer"></param>
        void Pack(IInternalPersistent parent, System.IO.BinaryWriter writer);

        /// <summary>
        /// Unpack the item
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader);

        /// <summary>
        /// true means item is loaded, otherwise false
        /// </summary>
        bool ValueLoaded { get; set; }
    }
}