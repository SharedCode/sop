
package cassandra

// TODO: Transaction log will be used for logging each action in a transaction commit, and
// then, entries will be used to rollback if needed.
// Not sure yet, what backing store to use, easiest is Cassandra, but we need to device
// it so physical deletes will not impact the system. As Cassandra is not geared for heavy
// or frequent adds & deletes.
