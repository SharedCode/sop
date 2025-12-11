package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestManageDatabase_Extended(t *testing.T) {
	ctxID := CreateContextForTest()
	defer RemoveContextForTest(ctxID)
	dir := t.TempDir()

	// 1. NewDatabase with Cassandra (Mocked/Invalid Config but triggers branch)
	cassPayload := fmt.Sprintf(`{
		"cache_type": 1, 
		"stores_folders": ["%s"],
		"cassandra_config": {"hosts": ["localhost"], "keyspace": "sop"}
	}`, dir)
	dbID := ManageDatabaseForTest(ctxID, NewDatabase, "", cassPayload)
	if len(dbID) > 0 {
		t.Log("NewDatabase with Cassandra succeeded")
	}

	// 2. BeginTransaction with Int payload
	// First create a valid DB
	dbPayload := fmt.Sprintf(`{"cache_type": 1, "stores_folders": ["%s"]}`, dir)
	validDBID := ManageDatabaseForTest(ctxID, NewDatabase, "", dbPayload)

	// Payload as int (Mode)
	transPayload := "1" // ReadWrite
	transID := ManageDatabaseForTest(ctxID, BeginTransaction, validDBID, transPayload)
	if len(transID) == 0 {
		t.Error("BeginTransaction with int payload failed")
	}

	// 3. NewBtree with invalid DB ID
	invalidDBID := "00000000-0000-0000-0000-000000000000"
	btreeOpts := `{"name": "test_btree_invalid_db", "transaction_id": "00000000-0000-0000-0000-000000000000"}`
	res := ManageDatabaseForTest(ctxID, NewBtree, invalidDBID, btreeOpts)
	if res != "Database not found" {
		t.Errorf("Expected 'Database not found', got '%s'", res)
	}

	// 4. OpenBtree with invalid DB ID
	res = ManageDatabaseForTest(ctxID, OpenBtree, invalidDBID, btreeOpts)
	if res != "Database not found" {
		t.Errorf("Expected 'Database not found', got '%s'", res)
	}
}

func TestManageDatabase_ErrorPaths_Extended(t *testing.T) {
	ctxID := CreateContextForTest()
	defer RemoveContextForTest(ctxID)
	dir := t.TempDir()

	// 1. NewDatabase: Invalid JSON payload
	res := ManageDatabaseForTest(ctxID, NewDatabase, "", "invalid-json")
	if !strings.Contains(res, "invalid options") {
		t.Errorf("NewDatabase with invalid JSON should fail, got: %s", res)
	}

	// Setup a valid DB for subsequent tests
	dbPayload := fmt.Sprintf(`{"cache_type": 1, "stores_folders": ["%s"]}`, dir)
	dbID := ManageDatabaseForTest(ctxID, NewDatabase, "", dbPayload)

	// 2. BeginTransaction: Invalid database UUID
	res = ManageDatabaseForTest(ctxID, BeginTransaction, "invalid-uuid", "")
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("BeginTransaction with invalid UUID should fail, got: %s", res)
	}

	// 3. BeginTransaction: Database not found
	res = ManageDatabaseForTest(ctxID, BeginTransaction, "00000000-0000-0000-0000-000000000000", "")
	if res != "Database not found" {
		t.Errorf("BeginTransaction with non-existent UUID should fail, got: %s", res)
	}

	// 4. BeginTransaction: Failure (Context Cancelled)
	ctxID2 := CreateContextForTest()
	CancelContextForTest(ctxID2)
	res = ManageDatabaseForTest(ctxID2, BeginTransaction, dbID, "")
	// Note: BeginTransaction might not check context immediately depending on implementation,
	// but let's see if it fails. If not, we might need another way to force failure.
	// Actually, sopdb.BeginTransaction takes ctx.
	if !strings.Contains(res, "context canceled") && res != "" {
		// If it returns a UUID, it succeeded unexpectedly.
		// If it returns empty string, it might be an error but we expect an error message.
		// Let's check if it's a UUID (len 36)
		if len(res) == 36 {
			t.Error("BeginTransaction with cancelled context should fail")
		}
	}

	// 5. NewBtree: Invalid JSON payload
	res = ManageDatabaseForTest(ctxID, NewBtree, dbID, "invalid-json")
	if !strings.Contains(res, "error Unmarshal BtreeOptions") {
		t.Errorf("NewBtree with invalid JSON should fail, got: %s", res)
	}

	// 6. NewBtree: Invalid database UUID
	// Use a valid JSON payload for BtreeOptions
	btreePayload := `{"name": "test_btree", "is_unique": false, "is_user_data_segment": false, "is_user_data_segment_loaded": false, "is_user_data_segment_modified": false, "index_specification": ""}`
	res = ManageDatabaseForTest(ctxID, NewBtree, "invalid-uuid", btreePayload)
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("NewBtree with invalid DB UUID should fail, got: %s", res)
	}

	// 7. NewBtree: Database not found
	res = ManageDatabaseForTest(ctxID, NewBtree, "00000000-0000-0000-0000-000000000000", btreePayload)
	if res != "Database not found" {
		t.Errorf("NewBtree with non-existent DB UUID should fail, got: %s", res)
	}

	// 8. OpenBtree: Invalid JSON payload
	res = ManageDatabaseForTest(ctxID, OpenBtree, dbID, "invalid-json")
	if !strings.Contains(res, "error Unmarshal BtreeOptions") {
		t.Errorf("OpenBtree with invalid JSON should fail, got: %s", res)
	}

	// 9. OpenModelStore: Invalid JSON payload
	res = ManageDatabaseForTest(ctxID, OpenModelStore, dbID, "invalid-json")
	if !strings.Contains(res, "invalid options") {
		t.Errorf("OpenModelStore with invalid JSON should fail, got: %s", res)
	}

	// 10. OpenVectorStore: Invalid JSON payload
	res = ManageDatabaseForTest(ctxID, OpenVectorStore, dbID, "invalid-json")
	if !strings.Contains(res, "invalid options") {
		t.Errorf("OpenVectorStore with invalid JSON should fail, got: %s", res)
	}

	// 11. OpenSearch: Invalid JSON payload
	res = ManageDatabaseForTest(ctxID, OpenSearch, dbID, "invalid-json")
	if !strings.Contains(res, "invalid options") {
		t.Errorf("OpenSearch with invalid JSON should fail, got: %s", res)
	}

	// 12. RemoveBtree: Invalid Database UUID
	res = ManageDatabaseForTest(ctxID, RemoveBtree, "invalid-uuid", "btree_name")
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("RemoveBtree with invalid DB UUID should fail, got: %s", res)
	}

	// 13. BeginTransaction: Valid TransactionOptions JSON
	// We need a valid DB ID.
	transOptsPayload := `{"mode": 1, "max_time": 30}` // ReadWrite, 30 mins
	res = ManageDatabaseForTest(ctxID, BeginTransaction, dbID, transOptsPayload)
	if len(res) != 36 {
		t.Errorf("BeginTransaction with valid options should succeed, got: %s", res)
	}
	// Cleanup transaction
	ManageTransactionForTest(ctxID, Rollback, res)

	// 14. BeginTransaction: Valid TransactionMode (int) JSON
	transModePayload := `1` // ReadWrite
	res = ManageDatabaseForTest(ctxID, BeginTransaction, dbID, transModePayload)
	if len(res) != 36 {
		t.Errorf("BeginTransaction with valid mode should succeed, got: %s", res)
	}
	// Cleanup transaction
	ManageTransactionForTest(ctxID, Rollback, res)

	// 15. NewBtree: Transaction not found
	// We need a valid UUID format but not existing in registry
	nonExistentTransID := "11111111-1111-1111-1111-111111111111"
	btreePayloadWithTrans := fmt.Sprintf(`{"name": "test_btree_fail", "transaction_id": "%s"}`, nonExistentTransID)
	res = ManageDatabaseForTest(ctxID, NewBtree, dbID, btreePayloadWithTrans)
	if !strings.Contains(res, "can't find Transaction") {
		t.Errorf("NewBtree with non-existent transaction should fail, got: %s", res)
	}

	// 16. OpenBtree: Invalid Database UUID
	res = ManageDatabaseForTest(ctxID, OpenBtree, "invalid-uuid", btreePayloadWithTrans)
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("OpenBtree with invalid DB UUID should fail, got: %s", res)
	}

	// 17. OpenBtree: Database not found
	res = ManageDatabaseForTest(ctxID, OpenBtree, "00000000-0000-0000-0000-000000000000", btreePayloadWithTrans)
	if res != "Database not found" {
		t.Errorf("OpenBtree with non-existent DB UUID should fail, got: %s", res)
	}

	// 18. OpenBtree: Transaction not found
	res = ManageDatabaseForTest(ctxID, OpenBtree, dbID, btreePayloadWithTrans)
	if !strings.Contains(res, "can't find Transaction") {
		t.Errorf("OpenBtree with non-existent transaction should fail, got: %s", res)
	}

	// 19. OpenModelStore: Invalid Database UUID
	modelPayload := fmt.Sprintf(`{"name": "test_model", "transaction_id": "%s"}`, nonExistentTransID)
	res = ManageDatabaseForTest(ctxID, OpenModelStore, "invalid-uuid", modelPayload)
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("OpenModelStore with invalid DB UUID should fail, got: %s", res)
	}

	// 20. OpenModelStore: Database not found
	res = ManageDatabaseForTest(ctxID, OpenModelStore, "00000000-0000-0000-0000-000000000000", modelPayload)
	if res != "Database not found" {
		t.Errorf("OpenModelStore with non-existent DB UUID should fail, got: %s", res)
	}

	// 21. OpenModelStore: Transaction not found
	res = ManageDatabaseForTest(ctxID, OpenModelStore, dbID, modelPayload)
	if !strings.Contains(res, "Transaction not found") {
		t.Errorf("OpenModelStore with non-existent transaction should fail, got: %s", res)
	}

	// 22. OpenVectorStore: Invalid Database UUID
	vectorPayload := fmt.Sprintf(`{"name": "test_vector", "transaction_id": "%s"}`, nonExistentTransID)
	res = ManageDatabaseForTest(ctxID, OpenVectorStore, "invalid-uuid", vectorPayload)
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("OpenVectorStore with invalid DB UUID should fail, got: %s", res)
	}

	// 23. OpenVectorStore: Database not found
	res = ManageDatabaseForTest(ctxID, OpenVectorStore, "00000000-0000-0000-0000-000000000000", vectorPayload)
	if res != "Database not found" {
		t.Errorf("OpenVectorStore with non-existent DB UUID should fail, got: %s", res)
	}

	// 24. OpenVectorStore: Transaction not found
	res = ManageDatabaseForTest(ctxID, OpenVectorStore, dbID, vectorPayload)
	if !strings.Contains(res, "Transaction not found") {
		t.Errorf("OpenVectorStore with non-existent transaction should fail, got: %s", res)
	}

	// 25. OpenSearch: Invalid Database UUID
	searchPayload := fmt.Sprintf(`{"name": "test_search", "transaction_id": "%s"}`, nonExistentTransID)
	res = ManageDatabaseForTest(ctxID, OpenSearch, "invalid-uuid", searchPayload)
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("OpenSearch with invalid DB UUID should fail, got: %s", res)
	}

	// 26. OpenSearch: Database not found
	res = ManageDatabaseForTest(ctxID, OpenSearch, "00000000-0000-0000-0000-000000000000", searchPayload)
	if res != "Database not found" {
		t.Errorf("OpenSearch with non-existent DB UUID should fail, got: %s", res)
	}

	// 27. OpenSearch: Transaction not found
	// 27. OpenSearch: Transaction not found
	res = ManageDatabaseForTest(ctxID, OpenSearch, dbID, searchPayload)
	if !strings.Contains(res, "Transaction not found") {
		t.Errorf("OpenSearch with non-existent transaction should fail, got: %s", res)
	}

	// 27b. OpenSearch: Invalid Transaction UUID
	searchPayloadInvalidTrans := `{"name": "test_search", "transaction_id": "invalid-uuid"}`
	res = ManageDatabaseForTest(ctxID, OpenSearch, dbID, searchPayloadInvalidTrans)
	if !strings.Contains(res, "Invalid transaction UUID") {
		t.Errorf("OpenSearch with invalid Trans UUID should fail, got: %s", res)
	}

	// 13. RemoveModelStore: Invalid Database UUID
	res = ManageDatabaseForTest(ctxID, RemoveModelStore, "invalid-uuid", "store_name")
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("RemoveModelStore with invalid DB UUID should fail, got: %s", res)
	}

	// 14. RemoveVectorStore: Invalid Database UUID
	res = ManageDatabaseForTest(ctxID, RemoveVectorStore, "invalid-uuid", "store_name")
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("RemoveVectorStore with invalid DB UUID should fail, got: %s", res)
	}

	// 15. RemoveSearch: Invalid Database UUID
	res = ManageDatabaseForTest(ctxID, RemoveSearch, "invalid-uuid", "store_name")
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("RemoveSearch with invalid DB UUID should fail, got: %s", res)
	}

	// 16. NewBtree: Transaction not found
	// Need valid DB and valid JSON with random TransID
	btreePayload2 := `{"name": "test_btree_no_trans", "transaction_id": "00000000-0000-0000-0000-000000000000"}`
	res = ManageDatabaseForTest(ctxID, NewBtree, dbID, btreePayload2)
	if !strings.Contains(res, "can't find Transaction") {
		t.Errorf("NewBtree with non-existent Transaction should fail, got: %s", res)
	}

	// 17. OpenBtree: Invalid Database UUID
	res = ManageDatabaseForTest(ctxID, OpenBtree, "invalid-uuid", btreePayload2)
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("OpenBtree with invalid DB UUID should fail, got: %s", res)
	}

	// 18. OpenBtree: Database not found
	res = ManageDatabaseForTest(ctxID, OpenBtree, "00000000-0000-0000-0000-000000000000", btreePayload2)
	if res != "Database not found" {
		t.Errorf("OpenBtree with non-existent DB UUID should fail, got: %s", res)
	}

	// 19. OpenBtree: Transaction not found
	res = ManageDatabaseForTest(ctxID, OpenBtree, dbID, btreePayload2)
	if !strings.Contains(res, "can't find Transaction") {
		t.Errorf("OpenBtree with non-existent Transaction should fail, got: %s", res)
	}

	// 20. OpenModelStore: Invalid Database UUID
	msPayload := `{"path": "test_ms", "transaction_id": "00000000-0000-0000-0000-000000000000"}`
	res = ManageDatabaseForTest(ctxID, OpenModelStore, "invalid-uuid", msPayload)
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("OpenModelStore with invalid DB UUID should fail, got: %s", res)
	}

	// 21. OpenModelStore: Database not found
	res = ManageDatabaseForTest(ctxID, OpenModelStore, "00000000-0000-0000-0000-000000000000", msPayload)
	if res != "Database not found" {
		t.Errorf("OpenModelStore with non-existent DB UUID should fail, got: %s", res)
	}

	// 22. OpenModelStore: Invalid Transaction UUID
	msPayloadInvalidTrans := `{"path": "test_ms", "transaction_id": "invalid-uuid"}`
	res = ManageDatabaseForTest(ctxID, OpenModelStore, dbID, msPayloadInvalidTrans)
	if !strings.Contains(res, "Invalid transaction UUID") {
		t.Errorf("OpenModelStore with invalid Trans UUID should fail, got: %s", res)
	}

	// 23. OpenModelStore: Transaction not found
	res = ManageDatabaseForTest(ctxID, OpenModelStore, dbID, msPayload)
	if !strings.Contains(res, "Transaction not found") {
		t.Errorf("OpenModelStore with non-existent Transaction should fail, got: %s", res)
	}

	// 24. OpenVectorStore: Invalid Database UUID
	vsPayload := `{"name": "test_vs", "transaction_id": "00000000-0000-0000-0000-000000000000"}`
	res = ManageDatabaseForTest(ctxID, OpenVectorStore, "invalid-uuid", vsPayload)
	if !strings.Contains(res, "invalid database UUID") {
		t.Errorf("OpenVectorStore with invalid DB UUID should fail, got: %s", res)
	}

	// 25. OpenVectorStore: Database not found
	res = ManageDatabaseForTest(ctxID, OpenVectorStore, "00000000-0000-0000-0000-000000000000", vsPayload)
	if res != "Database not found" {
		t.Errorf("OpenVectorStore with non-existent DB UUID should fail, got: %s", res)
	}

	// 26. OpenVectorStore: Invalid Transaction UUID
	vsPayloadInvalidTrans := `{"name": "test_vs", "transaction_id": "invalid-uuid"}`
	res = ManageDatabaseForTest(ctxID, OpenVectorStore, dbID, vsPayloadInvalidTrans)
	if !strings.Contains(res, "Invalid transaction UUID") {
		t.Errorf("OpenVectorStore with invalid Trans UUID should fail, got: %s", res)
	}

	// 27. OpenVectorStore: Transaction not found
	res = ManageDatabaseForTest(ctxID, OpenVectorStore, dbID, vsPayload)
	if !strings.Contains(res, "Transaction not found") {
		t.Errorf("OpenVectorStore with non-existent Transaction should fail, got: %s", res)
	}

	// 32. RemoveBtree: Database not found
	res = ManageDatabaseForTest(ctxID, RemoveBtree, "00000000-0000-0000-0000-000000000000", "btree_name")
	if res != "Database not found" {
		t.Errorf("RemoveBtree with non-existent DB UUID should fail, got: %s", res)
	}

	// 33. RemoveModelStore: Database not found
	res = ManageDatabaseForTest(ctxID, RemoveModelStore, "00000000-0000-0000-0000-000000000000", "store_name")
	if res != "Database not found" {
		t.Errorf("RemoveModelStore with non-existent DB UUID should fail, got: %s", res)
	}

	// 34. RemoveVectorStore: Database not found
	res = ManageDatabaseForTest(ctxID, RemoveVectorStore, "00000000-0000-0000-0000-000000000000", "store_name")
	if res != "Database not found" {
		t.Errorf("RemoveVectorStore with non-existent DB UUID should fail, got: %s", res)
	}

	// 35. RemoveSearch: Database not found
	res = ManageDatabaseForTest(ctxID, RemoveSearch, "00000000-0000-0000-0000-000000000000", "store_name")
	if res != "Database not found" {
		t.Errorf("RemoveSearch with non-existent DB UUID should fail, got: %s", res)
	}

}
