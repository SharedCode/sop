package database_test

import (
    "context"
    "testing"
    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/ai/database"
)

func TestBeginTx_Double(t *testing.T) {
    db := database.NewDatabase(sop.DatabaseOptions{})
    tx1, err1 := db.BeginTransaction(context.Background(), sop.ForWriting)
    if err1 != nil { t.Fatal(err1) }
    
    tx2, err2 := db.BeginTransaction(context.Background(), sop.ForWriting)
    if err2 != nil { t.Fatal(err2) }

    if tx1 == tx2 {
        t.Fatal("tx1 and tx2 are the exact same transaction instance!")
    } else {
        t.Log("tx1 and tx2 are different instances")
    }
}
