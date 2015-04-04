namespace Sop.Collections.Generic.BTree
{
    internal class BTreeSlotComparer<TKey, TValue> : System.Collections.Generic.IComparer<BTreeItem<TKey, TValue>>
    {
        public BTreeSlotComparer(System.Collections.Generic.IComparer<TKey> comparer)
        {
            KeyComparer = comparer;
        }

        public System.Collections.Generic.IComparer<TKey> KeyComparer;

        public int Compare(BTreeItem<TKey, TValue> x, BTreeItem<TKey, TValue> y)
        {
            if (x == null && y == null)
                return 0;
            if (x == null)
                return -1;
            if (y == null)
                return 1;
            return KeyComparer.Compare(x.Key, y.Key);
        }
    }
}