namespace Sop.Mru
{
    /// <summary>
    /// MRU Item
    /// </summary>
    internal class MruItem
    {
        public MruItem(object key, object value,
                       Transaction.ITransactionLogger transaction)
        {
            this.Key = key;
            this.Value = value;
            this._transaction = transaction;
        }

        public object Key;
        public object Value;
        public Node IndexToMruList = null;

        /// <summary>
        /// Transaction this block belongs to
        /// </summary>
        public Transaction.ITransactionLogger Transaction
        {
            get
            {
                if (_transaction != null &&
                    (int) _transaction.CurrentCommitPhase >= (int) Sop.Transaction.CommitPhase.SecondPhase)
                    _transaction = null;
                return _transaction;
            }
            set { _transaction = value; }
        }

        private Transaction.ITransactionLogger _transaction = null;
    }
}