// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

namespace Sop.OnDisk.Algorithm.LinkedList
{
    /// <summary>
    /// List On Disk interface
    /// </summary>
    internal interface IListOnDisk : Collection.ICollectionOnDisk
    {
        // Summary:
        //     Adds an item to the System.Collections.IList.
        //
        // Parameters:
        //   value:
        //     The System.Object to add to the System.Collections.IList.
        //
        // Returns:
        //     The position into which the new element was inserted.
        //
        // Exceptions:
        //   System.NotSupportedException:
        //     The System.Collections.IList is read-only.-or- The System.Collections.IList
        //     has a fixed size.
        long Add(object value);

        /// <summary>
        /// Address(ie - file offset) of this Object on Disk
        /// </summary>
        //long DataAddress { get; set; }
        //bool IsDirty { get; set; }
        // Summary:
        //     Gets or sets the element at the specified index.
        //
        // Parameters:
        //   index:
        //     The zero-based index of the element to get or set.
        //
        // Returns:
        //     The element at the specified index.
        //
        // Exceptions:
        //   System.ArgumentOutOfRangeException:
        //     index is not a valid index in the System.Collections.IList.
        //
        //   System.NotSupportedException:
        //     The property is set and the System.Collections.IList is read-only.
        object this[int index] { get; set; }

        //
        // Summary:
        //     Removes all items from the System.Collections.IList.
        //
        // Exceptions:
        //   System.NotSupportedException:
        //     The System.Collections.IList is read-only.
        void Clear();
        //
        // Summary:
        //     Determines whether the System.Collections.IList contains a specific value.
        //
        // Parameters:
        //   value:
        //     The System.Object to locate in the System.Collections.IList.
        //
        // Returns:
        //     true if the System.Object is found in the System.Collections.IList; otherwise,
        //     false.
        bool Contains(object value);

        //
        // Summary:
        //     Removes the first occurrence of a specific object from the System.Collections.IList.
        //
        // Parameters:
        //   value:
        //     The System.Object to remove from the System.Collections.IList.
        //
        // Exceptions:
        //   System.NotSupportedException:
        //     The System.Collections.IList is read-only.-or- The System.Collections.IList
        //     has a fixed size.
        void Remove(object value);
        //
        // Summary:
        //     Removes the System.Collections.IList item at the specified index.
        //
        // Parameters:
        //   index:
        //     The zero-based index of the item to remove.
        //
        // Exceptions:
        //   System.ArgumentOutOfRangeException:
        //     index is not a valid index in the System.Collections.IList.
        //
        //   System.NotSupportedException:
        //     The System.Collections.IList is read-only.-or- The System.Collections.IList
        //     has a fixed size.
        void RemoveAt(int index);
    }
}