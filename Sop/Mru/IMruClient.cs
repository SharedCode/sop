using System.Collections;

namespace Sop.Mru
{
    /// <summary>
    /// MRU Client interface
    /// </summary>
    public interface IMruClient
    {
        /// <summary>
        /// OnMaxCapacity will be invoked when MRU reached maximum count of items
        /// and removing some of them for persistence/swap to more permanent
        /// store like disk.
        /// </summary>
        /// <param name="nodes"></param>
        int OnMaxCapacity(IEnumerable nodes);

        /// <summary>
        /// OnMaxCapacity will be invoked when MRU reached maximum capacity
        /// </summary>
        void OnMaxCapacity();
    }
}