namespace Sop.Mru.Generic
{
    public class MruItem<TKey, TValue>
    {
        public MruItem(TKey key, TValue value,
                       Transaction.ITransactionLogger transaction)
        {
            this.Key = key;
            this.Value = value;
            this._transaction = transaction;
        }

        public TKey Key;
        public TValue Value;
        internal Node<TKey, TValue> IndexToMruList = null;

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

        private Transaction.ITransactionLogger _transaction;
    }
}