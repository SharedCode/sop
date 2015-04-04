namespace Sop.Mru.Generic
{
    internal class Node<TKey, TValue>
    {
        public Node(MruItem<TKey, TValue> data)
        {
            this.Data = data;
        }

        public MruItem<TKey, TValue> Data;
        public Node<TKey, TValue> Next = null;
        public Node<TKey, TValue> Previous = null;
    }
}