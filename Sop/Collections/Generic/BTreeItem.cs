namespace Sop.Collections.Generic
{
    /// <summary>
    /// In-Memory B-Tree Item
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class BTreeItem<TKey, TValue>
    {
        public BTreeItem(TKey k, TValue v)
        {
            Key = k;
            Value = v;
        }

        public TKey Key;
        public TValue Value;
    }
}
