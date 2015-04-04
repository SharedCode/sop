namespace Sop.Mru
{
    /// <summary>
    /// MRU Node.
    /// </summary>
    internal class Node
    {
        public Node(MruItem data)
        {
            this.Data = data;
        }

        public MruItem Data;
        public Node Next = null;
        public Node Previous = null;
    }
}