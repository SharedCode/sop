using System;
using System.Collections.Generic;
using System.Text;

namespace Sop.Collections
{
    namespace BTree
    {
        /// <summary>
        /// BTree item type enumeration
        /// </summary>
        public enum ItemType
        {
            /// <summary>
            /// Default
            /// </summary>
            Default,

            /// <summary>
            /// Key item type
            /// </summary>
            Key,

            /// <summary>
            /// Value item type
            /// </summary>
            Value
        }

        /// <summary>
        /// Child nodes enumeration
        /// </summary>
        public enum ChildNodes
        {
            /// <summary>
            /// Left child
            /// </summary>
            LeftChild,

            /// <summary>
            /// Right child
            /// </summary>
            RightChild
        }
    }
}