// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

namespace Sop.OnDisk.File
{
    /// <summary>
    /// Entity interface
    /// </summary>
    internal interface IEntity
    {
        /// <summary>
        /// true means this entity is new, otherwise false
        /// </summary>
        bool IsNew { get; set; }

        /// <summary>
        /// Name of the entity
        /// </summary>
        string Name { get; }
    }
}