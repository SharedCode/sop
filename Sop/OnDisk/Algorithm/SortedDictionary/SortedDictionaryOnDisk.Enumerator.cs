// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections;
using Sop.Collections.BTree;
using System.Collections.Generic;

namespace Sop.OnDisk.Algorithm.SortedDictionary
{
    /// <summary>
    /// SortedDictionaryOnDisk uses balanced m-way tree (B-Tree) algorithm.
    /// </summary>
    internal partial class SortedDictionaryOnDisk
    {
        internal class DictionaryEnumerator<TKey, TValue> : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            public void Dispose()
            {
                if (BTree == null) return;
                BTree.Locker.Invoke(() => { BTree.Dispose(); }, OperationType.Read);
                BTree = null;
            }

            /// <summary>
            /// Constructor. Pass the B-Tree instance you want to enumerate its items/elements on.
            /// </summary>
            /// <param name="bTree">BTree instance items will be enumerated</param>
            public DictionaryEnumerator(SortedDictionaryOnDisk bTree)
            {
                BTree = (SortedDictionaryOnDisk)bTree.Clone();
                Reset();
            }

            /// <summary>
            /// Returns current BTree entry/record
            /// </summary>
            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    return BTree.Locker.Invoke(() =>
                    {
                        return new KeyValuePair<TKey, TValue>((TKey)BTree.CurrentKey, (TValue)BTree.CurrentValue);
                    }, OperationType.Read);
                }
            }

            /// <summary>
            /// Returns current BTree entry/record
            /// </summary>
            object IEnumerator.Current
            {
                get
                {
                    return BTree.Locker.Invoke(() => { return BTree.CurrentEntry; }, OperationType.Read);
                }
            }

            /// <summary>
            /// Make the next record current
            /// </summary>
            /// <returns>Returns true if successul, false otherwise</returns>
            public bool MoveNext()
            {
                if (!_bWasReset)
                    return BTree.Locker.Invoke(() => { return BTree.MoveNext(); }, OperationType.Read);
                else
                {
                    if (BTree.Locker.Invoke(() => { return BTree.Count == 0; }, OperationType.Read))
                        return false;
                    _bWasReset = false;
                    return true;
                }
            }

            /// <summary>
            /// Reset enumerator. You will need to call MoveNext to get to first record.
            /// </summary>
            public void Reset()
            {
                BTree.Locker.Invoke(() =>
                {
                    if (BTree.Count > 0)
                        BTree.MoveFirst();
                    BTree.HintSequentialRead = true;
                }, OperationType.Read);
                _bWasReset = true;
            }

            internal SortedDictionaryOnDisk BTree;
            private bool _bWasReset;
        }
        /// <summary>
        /// The B-Tree enumerator
        /// </summary>
        internal class DictionaryEnumerator : IDictionaryEnumerator, IDisposable
        {
            public void Dispose()
            {
                if (BTree == null) return;
                BTree.Locker.Invoke(() => { BTree.Dispose(); }, OperationType.Read);
                BTree = null;
            }

            /// <summary>
            /// Constructor. Pass the B-Tree instance you want to enumerate its items/elements on.
            /// </summary>
            /// <param name="bTree">BTree instance items will be enumerated</param>
            public DictionaryEnumerator(SortedDictionaryOnDisk bTree)
            {
                this.BTree = (SortedDictionaryOnDisk)bTree.Clone();
                this.Reset();
            }

            /// <summary>
            /// Returns current BTree entry/record
            /// </summary>
            public DictionaryEntry Entry
            {
                get { return BTree.Locker.Invoke(() => { return (DictionaryEntry)BTree.CurrentEntry; }, OperationType.Read); }
            }

            /// <summary>
            /// Returns Key of the current record
            /// </summary>
            public object Key
            {
                get { return BTree.Locker.Invoke(() => { return BTree.CurrentKey; }, OperationType.Read); }
            }

            /// <summary>
            /// Returns Value of the current record
            /// </summary>
            public object Value
            {
                get { return BTree.Locker.Invoke(() => { return BTree.CurrentValue; }, OperationType.Read); }
            }

            /// <summary>
            /// Make the next record current
            /// </summary>
            /// <returns>Returns true if successul, false otherwise</returns>
            public bool MoveNext()
            {
                if (!_bWasReset)
                    return BTree.Locker.Invoke(() => { return BTree.MoveNext(); }, OperationType.Read);
                else
                {
                    if (BTree.Locker.Invoke(() => { return BTree.Count == 0; }, OperationType.Read))
                        return false;
                    _bWasReset = false;
                    return true;
                }
            }

            /// <summary>
            /// Reset enumerator. You will need to call MoveNext to get to first record.
            /// </summary>
            public void Reset()
            {
                BTree.Locker.Invoke(() =>
                {
                    if (BTree.Count > 0)
                        BTree.MoveFirst();
                    BTree.HintSequentialRead = true;
                }, OperationType.Read);
                _bWasReset = true;
            }

            /// <summary>
            /// Returns Current record
            /// </summary>
            /// <exception cref="InvalidOperationException">Throws InvalidOperationException exception if Reset was called without calling MoveNext</exception>
            public object Current
            {
                get
                {
                    if (!_bWasReset)
                    {
                        return BTree.Locker.Invoke(() =>
                        {
                            switch (BTree.ItemType)
                            {
                                case ItemType.Default:
                                    return BTree.CurrentEntry;
                                case ItemType.Key:
                                    return BTree.CurrentKey;
                                case ItemType.Value:
                                default:
                                    return BTree.CurrentValue;
                            };
                        }, OperationType.Read);
                    }
                    throw new InvalidOperationException(
                        "SortedDictionaryOnDisk.Enumerator got Reset. Call one of Move functions before getting 'Current' object.");
                }
            }

            internal SortedDictionaryOnDisk BTree;
            private bool _bWasReset;
        }
    }
}