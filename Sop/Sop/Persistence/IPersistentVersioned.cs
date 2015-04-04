// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

namespace Sop.Persistence
{
    /// <summary>
    /// Versioned Persistent Object interface
    /// </summary>
    public interface IPersistentVersioned : IPersistent
    {
        /// <summary>
        /// Version Number
        /// </summary>
        long VersionNumber { get; set; }
    }
}
