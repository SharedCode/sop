package in_cas_s3

import (
// "github.com/SharedCode/sop/btree"
// "github.com/SharedCode/sop/in_cas_s3/redis"
// "github.com/SharedCode/sop/in_memory"
)

// TODO: methods on implicit transaction wrapper will create & begin a transaction, invoke the actual
// B-tree function then end the transaction(commit or rollback).
// These methods will allow code to use B-tree without bothering with transactions. Caveat, since
// transaction is per method, then there is no guarantee that multiple calls to these methods will
// give you the outcome that you expect, specially if there are other transaction(s) that managed
// the same item in the store.
// It is still best to use an explicit transaction.
