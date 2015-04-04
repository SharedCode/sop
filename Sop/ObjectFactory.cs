// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using Sop.OnDisk;
using System.Collections;
using Sop.OnDisk.Algorithm.SortedDictionary;

namespace Sop
{
    /// <summary>
    /// For internal use only.
    /// ObjectFactory is a utility class used to standardize in one
    /// place instantiation of SOP built-in entities such as Sorted Dictionary on Disk.
    /// </summary>
    public class ObjectFactory
    {
        protected ObjectFactory(){}

        /// <summary>
        /// Singleton Instance
        /// </summary>
        public static ObjectFactory Instance
        {
            get
            {
                if (_threadInstance == null)
                {
                    _threadInstance = new ObjectFactory();
                    if (_instance == null)
                        _instance = _threadInstance;
                }
                return _threadInstance;
            }
            private set { _threadInstance = value; }
        }
        [ThreadStatic]
        private static ObjectFactory _threadInstance;

        private static ObjectFactory _instance = new ObjectFactory();

        /// <summary>
        /// Reset singleton instance to default.
        /// </summary>
        public void Reset()
        {
            Instance = _instance;
        }

        /// <summary>
        /// Create Dictionary On Disk.
        /// </summary>
        /// <param name="file"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        /// <returns></returns>
        public virtual ISortedDictionaryOnDisk CreateDictionaryOnDisk(IFile file, IComparer comparer, string name,
                                                                      bool isDataInKeySegment)
        {
            return new SortedDictionaryOnDisk((OnDisk.File.IFile) file, comparer, name, isDataInKeySegment);
        }
    }
}