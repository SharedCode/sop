// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

namespace Sop.OnDisk.Algorithm.BTree
{
    /// <summary>
    /// BTree Node On Disk interface
    /// </summary>
    internal interface IBTreeNodeOnDisk
    {
        BTreeItemOnDisk[] Slots { get; set; }
        short Count { get; set; }
        string ToString();
    }
}