using System;

namespace Sop.Mru
{
    /// <summary>
    /// SaveTypes enumeration
    /// </summary>
    [Flags]
    public enum SaveTypes
    {
        /// <summary>
        /// Default
        /// </summary>
        Default,

        /// <summary>
        /// Collection Save function triggered the Save session
        /// </summary>
        CollectionSave = 1,

        /// <summary>
        /// Save session was triggered by the Data Pool getting into its Max capacity
        /// </summary>
        DataPoolInMaxCapacity = CollectionSave << 1
    }
}