// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

namespace Sop.Transaction
{
    /// <summary>
    /// Action Type enumeration
    /// </summary>
    public enum ActionType
    {
        /// <summary>
        /// Add item to collection
        /// </summary>
        Add,

        /// <summary>
        /// Remove item from collection
        /// </summary>
        Remove,

        /// <summary>
        /// Save changes of a collection
        /// </summary>
        Save,

        /// <summary>
        /// Grow the collection
        /// </summary>
        Grow,

        /// <summary>
        /// Recycle removed block(s)
        /// </summary>
        Recycle,

        /// <summary>
        /// Recycle a removed collection
        /// </summary>
        RecycleCollection
    }
}