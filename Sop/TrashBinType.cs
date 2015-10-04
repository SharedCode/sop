// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

namespace Sop
{
    /// <summary>
    /// Trash Bin type enumeration
    /// </summary>
    public enum TrashBinType
    {
        /// <summary>
        /// FileWide is one trash bin per File object.
        /// </summary>
        //FileWide,

        /// <summary>
        /// Default is one trash bin per Collection object.
        /// </summary>
        Default,

        /// <summary>
        /// Nothing means no trash bin. Each data block deleted can't be recycled and will be a wasted space.
        /// NOTE: this is useful for specialized applications where after index/data file build up, 
        /// it switch to read only mode thus, not needing any trash or recycle bins.
        /// </summary>
        Nothing
    }
}
