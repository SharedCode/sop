// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;

namespace Sop.OnDisk.Geometry
{
    /// <summary>
    /// Use Region to allow mathematical operations on 
    /// a disk segment. Given a certain region on disk, one
    /// can subtract disk area(s) from it.
    /// The remaining areas(disk address and size) are 
    /// returned via an enumerator.
    /// </summary>
    internal class Region
    {
        /// <summary>
        /// Expects a file Offset (a.k.a. data Address) and
        /// region size. The region starts from Address onto
        /// Address + Size.
        /// </summary>
        /// <param name="address"></param>
        /// <param name="size"></param>
        public Region(long address, int size)
        {
            _diskAreas.Add(address, size);
        }

        public override string ToString()
        {
            if (_diskAreas.Count > 0)
            {
                foreach (KeyValuePair<long, int> k in _diskAreas)
                    return string.Format("{0}:{1}", k.Key, k.Value);
            }
            return base.ToString();
        }

        /// <summary>
        /// Count of Segments in this Disk Region
        /// </summary>
        public int Count
        {
            get { return _diskAreas.Count; }
        }

        /// <summary>
        /// Returns an enumerator for iterating through each disk area on this Region
        /// </summary>
        /// <returns></returns>
        public IEnumerator<KeyValuePair<long, int>> GetEnumerator()
        {
            return _diskAreas.GetEnumerator();
        }

        /// <summary>
        /// Subtract a disk area from this region
        /// </summary>
        /// <param name="address"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public bool Subtract(long address, int size)
        {
            bool r = false;
            var newAreas = new SortedList<long, int>();
            long origAddress = address;
            int origSize = size;
            var forRemoval = new List<long>();
            foreach (KeyValuePair<long, int> area in _diskAreas)
            {
                address = origAddress;
                size = origSize;
                /* Cases:
				 * 1) Address + Size == DiskAreas
				 * 2) Address + Size is within DiskAreas
				 * 3) Address + Size has intersection in front
				 * 4) Address + Size has intersection in back
				 */
                if (_region.FirstWithinSecond(area.Key, area.Value, address, size))
                {
                    //** if this area is totally within the Address + Size segment, remove the area
                    forRemoval.Add(area.Key);
                    r = true;
                }
                else if (_region.Intersect(area.Key, area.Value, address, size))
                {
                    r = true;
                    if (address <= area.Key && address + size < area.Key + area.Value)
                    {
                        long newAddress = address + size;
                        int newSize = (int) (area.Key + area.Value - newAddress);
                        newAreas.Add(newAddress, newSize);
                    }
                    else if (address > area.Key && address + size >= area.Key + area.Value)
                    {
                        long newAddress = area.Key;
                        int newSize = (int) (address - area.Key);
                        newAreas.Add(newAddress, newSize);
                    }
                    else if (address > area.Key && address + size < area.Key + area.Value)
                    {
                        long newAddress = area.Key;
                        int newSize = (int) (address - area.Key);
                        newAreas.Add(newAddress, newSize);

                        newAddress = address + size;
                        newSize = (int) (area.Key + area.Value - newAddress);
                        newAreas.Add(newAddress, newSize);
                    }
                    else
                        throw new InvalidOperationException("Unknown Disk Region Operation.");
                }
                else
                    newAreas.Add(area.Key, area.Value);
            }
            if (r)
            {
                if (forRemoval.Count > 0)
                {
                    foreach (long addr in forRemoval)
                        _diskAreas.Remove(addr);
                }
                else if (newAreas.Count > 0)
                    _diskAreas = newAreas;
            }
            return r;
        }

        private SortedList<long, int> _diskAreas = new SortedList<long, int>();
        private readonly RegionLogic _region = new RegionLogic();
    }
}