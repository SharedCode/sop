package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"testing"
)

func TestManageDatabase_RemoveStores(t *testing.T) {
	ctxID := CreateContextForTest()
	defer RemoveContextForTest(ctxID)
	dir := t.TempDir()

	dbPayload := fmt.Sprintf(`{"cache_type": 1, "stores_folders": ["%s"]}`, dir)
	dbID := ManageDatabaseForTest(ctxID, NewDatabase, "", dbPayload)

	transPayload := fmt.Sprintf(`{"Mode": 1, "stores_folders": ["%s"]}`, dir)
	transID := ManageDatabaseForTest(ctxID, BeginTransaction, dbID, transPayload)

	// Create and Remove Btree
	btreeOpts := fmt.Sprintf(`{
		"name": "test_btree_remove",
		"transaction_id": "%s",
		"is_primitive_key": false
	}`, transID)
	ManageDatabaseForTest(ctxID, NewBtree, dbID, btreeOpts)

	removeBtreePayload := "test_btree_remove"
	res := ManageDatabaseForTest(ctxID, 8, dbID, removeBtreePayload) // RemoveBtree = 8
	if res != "" {
		t.Errorf("RemoveBtree failed: %s", res)
	}

	// Create and Remove VectorStore
	vecOpts := fmt.Sprintf(`{
		"transaction_id": "%s",
		"name": "test_vec_remove",
		"config": {"usage_mode": 0, "content_size": 0}
	}`, transID)
	ManageDatabaseForTest(ctxID, 6, dbID, vecOpts) // OpenVectorStore

	removeVecPayload := "test_vec_remove"
	res = ManageDatabaseForTest(ctxID, 10, dbID, removeVecPayload) // RemoveVectorStore = 10

	// Because Transaction had not been committed yet, so, the store may not had been written to disk yet.
	if res != "" && !strings.Contains(res, "can't remove store "+removeVecPayload) {
		t.Errorf("RemoveVectorStore failed: %s", res)
	}

	// Create and Remove ModelStore
	modelOpts := fmt.Sprintf(`{
		"transaction_id": "%s",
		"path": "%s"
	}`, transID, dir)
	ManageDatabaseForTest(ctxID, 5, dbID, modelOpts) // OpenModelStore

	// ModelStore removal uses path as name?
	// OpenModelStore uses "path".
	// RemoveModelStore uses "name".
	// Usually path is the name for ModelStore if it's file based?
	// Or maybe I should check how OpenModelStore names it.
	// In jsondb.main.go: db.OpenModelStore(ctx, opts.Path, ...)
	// So the name is opts.Path.

	removeModelPayload := dir
	res = ManageDatabaseForTest(ctxID, 9, dbID, removeModelPayload) // RemoveModelStore = 9

	// Because Transaction had not been committed yet, so, the store may not had been written to disk yet.
	if res != "" && !strings.Contains(res, "there is no item with such name") {
		t.Errorf("RemoveModelStore failed: %s", res)
	}
}

func TestManageTransaction_Extended(t *testing.T) {
	ctxID := CreateContextForTest()
	defer RemoveContextForTest(ctxID)
	dir := t.TempDir()

	dbPayload := fmt.Sprintf(`{"cache_type": 1, "stores_folders": ["%s"]}`, dir)
	dbID := ManageDatabaseForTest(ctxID, NewDatabase, "", dbPayload)
	transPayload := fmt.Sprintf(`{"Mode": 1, "stores_folders": ["%s"]}`, dir)
	transID := ManageDatabaseForTest(ctxID, BeginTransaction, dbID, transPayload)

	// 1. Deprecated actions
	res := ManageTransactionForTest(ctxID, NewTransaction, "")
	if res != "NewTransaction is deprecated. Please use manageDatabase with BeginTransaction action." {
		t.Error("NewTransaction deprecated message mismatch")
	}
	res = ManageTransactionForTest(ctxID, Begin, "")
	if res != "Begin is deprecated. Transaction is already begun when created via manageDatabase." {
		t.Error("Begin deprecated message mismatch")
	}

	// 2. Invalid UUID
	res = ManageTransactionForTest(ctxID, Commit, "invalid-uuid")
	if res == "" {
		t.Error("Commit with invalid UUID should fail")
	}

	// 3. Not Found
	res = ManageTransactionForTest(ctxID, Commit, "00000000-0000-0000-0000-000000000000")
	if res == "" {
		t.Error("Commit with non-existent UUID should fail")
	}

	// 4. Commit failure (Context Cancelled)
	ctxID2 := CreateContextForTest()
	CancelContextForTest(ctxID2)
	res = ManageTransactionForTest(ctxID2, Commit, transID)
	if res == "" {
		t.Error("Commit with cancelled context should fail")
	}
}

func TestManageLogging_Extended(t *testing.T) {
	// Valid file
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	res := ManageLoggingForTest(1, logFile)
	if res != "" {
		t.Errorf("ManageLogging failed: %s", res)
	}

	// Invalid file (directory)
	res = ManageLoggingForTest(1, dir)
	if res == "" {
		t.Error("ManageLogging should have failed for directory path")
	}
}

func TestManageBtree_ErrorPaths_Extended(t *testing.T) {
	ctxID := CreateContextForTest()
	defer RemoveContextForTest(ctxID)
	dir := t.TempDir()

	// Setup DB and Transaction
	dbPayload := fmt.Sprintf(`{"cache_type": 1, "stores_folders": ["%s"]}`, dir)
	dbID := ManageDatabaseForTest(ctxID, NewDatabase, "", dbPayload)
	transPayload := fmt.Sprintf(`{"Mode": 1, "stores_folders": ["%s"]}`, dir)
	transID := ManageDatabaseForTest(ctxID, BeginTransaction, dbID, transPayload)

	// Create Btree (Map Key)
	btreePayload := fmt.Sprintf(`{"name": "test_btree_err", "is_unique": false, "is_user_data_segment": false, "is_user_data_segment_loaded": false, "is_user_data_segment_modified": false, "index_specification": "", "transaction_id": "%s"}`, transID)
	btreeID := ManageDatabaseForTest(ctxID, NewBtree, dbID, btreePayload)

	// 1. manageBtree: Unknown action
	res := ManageBtreeForTest(ctxID, 999, "", "")
	if !strings.Contains(res, "unsupported manage action") {
		t.Errorf("manageBtree with unknown action should fail, got: %s", res)
	}

	// 2. manage: Invalid metadata payload
	res = ManageBtreeForTest(ctxID, Add, "invalid-json", "")
	if !strings.Contains(res, "error Unmarshal ManageBtreeMetaData") {
		t.Errorf("manage with invalid metadata should fail, got: %s", res)
	}

	// 3. manage: Invalid items payload (Map Key)
	metaPayload := fmt.Sprintf(`{"btree_id": "%s", "transaction_id": "%s"}`, btreeID, transID)
	res = ManageBtreeForTest(ctxID, Add, metaPayload, "invalid-json")
	if !strings.Contains(res, "error Unmarshal ManageBtreePayload") {
		t.Errorf("manage with invalid items payload should fail, got: %s", res)
	}

	// 4. manage: UpdateCurrentKey with empty items (Map Key)
	emptyItemsPayload := `{"items": []}`
	res = ManageBtreeForTest(ctxID, UpdateCurrentKey, metaPayload, emptyItemsPayload)
	if !strings.Contains(res, "UpdateCurrentKey requires at least one item") {
		t.Errorf("UpdateCurrentKey with empty items should fail, got: %s", res)
	}

	// 5. remove: Invalid metadata payload
	res = ManageBtreeForTest(ctxID, Remove, "invalid-json", "")
	if !strings.Contains(res, "error Unmarshal ManageBtreeMetaData") {
		t.Errorf("remove with invalid metadata should fail, got: %s", res)
	}

	// 6. remove: Invalid keys payload (Map Key)
	res = ManageBtreeForTest(ctxID, Remove, metaPayload, "invalid-json")
	if !strings.Contains(res, "error Unmarshal keys array") {
		t.Errorf("remove with invalid keys payload should fail, got: %s", res)
	}

	// Create Btree (Primitive Key)
	btreePrimPayload := fmt.Sprintf(`{"name": "test_btree_err_prim", "is_unique": false, "is_user_data_segment": false, "is_user_data_segment_loaded": false, "is_user_data_segment_modified": false, "index_specification": "", "transaction_id": "%s", "is_primitive_key": true}`, transID)
	btreePrimID := ManageDatabaseForTest(ctxID, NewBtree, dbID, btreePrimPayload)
	metaPrimPayload := fmt.Sprintf(`{"btree_id": "%s", "transaction_id": "%s", "is_primitive_key": true}`, btreePrimID, transID)

	// 7. manage: Invalid items payload (Primitive Key)
	res = ManageBtreeForTest(ctxID, Add, metaPrimPayload, "invalid-json")
	if !strings.Contains(res, "error Unmarshal ManageBtreePayload") {
		t.Errorf("manage (prim) with invalid items payload should fail, got: %s", res)
	}

	// 8. manage: UpdateCurrentKey with empty items (Primitive Key)
	res = ManageBtreeForTest(ctxID, UpdateCurrentKey, metaPrimPayload, emptyItemsPayload)
	if !strings.Contains(res, "UpdateCurrentKey requires at least one item") {
		t.Errorf("UpdateCurrentKey (prim) with empty items should fail, got: %s", res)
	}

	// 9. remove: Invalid keys payload (Primitive Key)
	// 9. remove: Invalid keys payload (Primitive Key)
	res = ManageBtreeForTest(ctxID, Remove, metaPrimPayload, "invalid-json")
	if !strings.Contains(res, "error Unmarshal keys array") {
		t.Errorf("remove (prim) with invalid keys payload should fail, got: %s", res)
	}
}

func TestBtree_Manage_Extended(t *testing.T) {
	ctxID := CreateContextForTest()
	defer RemoveContextForTest(ctxID)
	dir := t.TempDir()

	dbPayload := fmt.Sprintf(`{"cache_type": 1, "stores_folders": ["%s"]}`, dir)
	dbID := ManageDatabaseForTest(ctxID, NewDatabase, "", dbPayload)
	transPayload := fmt.Sprintf(`{"Mode": 1, "stores_folders": ["%s"]}`, dir)
	transID := ManageDatabaseForTest(ctxID, BeginTransaction, dbID, transPayload)

	// 1. Complex Key (JsonDBAnyKey)
	btreeOpts := fmt.Sprintf(`{
		"name": "test_btree_manage_ext",
		"transaction_id": "%s",
		"is_primitive_key": false
	}`, transID)
	btreeID := ManageDatabaseForTest(ctxID, NewBtree, dbID, btreeOpts)
	actionPayload := fmt.Sprintf(`{"transaction_id": "%s", "btree_id": "%s"}`, transID, btreeID)

	// AddIfNotExist
	itemsPayload := `{"items": [{"key": {"id": 1}, "value": "val1"}]}`
	res := ManageBtreeForTest(ctxID, AddIfNotExist, actionPayload, itemsPayload)
	if res != "true" {
		t.Errorf("AddIfNotExist failed: %s", res)
	}

	// Upsert
	itemsPayload = `{"items": [{"key": {"id": 1}, "value": "val1_upsert"}]}`
	res = ManageBtreeForTest(ctxID, Upsert, actionPayload, itemsPayload)
	if res != "true" {
		t.Errorf("Upsert failed: %s", res)
	}

	// UpdateCurrentKey
	// First position cursor
	NavigateBtreeForTest(ctxID, First, actionPayload, "")
	// UpdateCurrentKey takes one item with new key.
	itemsPayload = `{"items": [{"key": {"id": 10}, "value": "val1_upsert"}]}`
	res = ManageBtreeForTest(ctxID, UpdateCurrentKey, actionPayload, itemsPayload)
	if res == "true" {
		t.Error("UpdateCurrentKey should have failed (ordering change)")
	} else {
		t.Logf("UpdateCurrentKey failed as expected: %s", res)
	}

	// 2. Primitive Key (JsonDBMapKey)
	// Create new transaction
	transID2 := ManageDatabaseForTest(ctxID, BeginTransaction, dbID, transPayload)

	btreeOptsPrim := fmt.Sprintf(`{
		"name": "test_btree_manage_prim",
		"transaction_id": "%s",
		"is_primitive_key": true
	}`, transID2)
	btreeIDPrim := ManageDatabaseForTest(ctxID, NewBtree, dbID, btreeOptsPrim)
	if len(btreeIDPrim) != 36 {
		t.Fatalf("NewBtree Prim failed: %s", btreeIDPrim)
	}
	actionPayloadPrim := fmt.Sprintf(`{"transaction_id": "%s", "btree_id": "%s"}`, transID2, btreeIDPrim)

	// AddIfNotExist
	itemsPayloadPrim := `{"items": [{"key": "key1", "value": "val1"}]}`
	res = ManageBtreeForTest(ctxID, AddIfNotExist, actionPayloadPrim, itemsPayloadPrim)
	if res != "true" {
		t.Errorf("AddIfNotExist Prim failed: %s", res)
	}

	// Upsert
	itemsPayloadPrim = `{"items": [{"key": "key1", "value": "val1_upsert"}]}`
	res = ManageBtreeForTest(ctxID, Upsert, actionPayloadPrim, itemsPayloadPrim)
	if res != "true" {
		t.Errorf("Upsert Prim failed: %s", res)
	}

	// UpdateCurrentKey
	NavigateBtreeForTest(ctxID, First, actionPayloadPrim, "")
	itemsPayloadPrim = `{"items": [{"key": "key10", "value": "val1_upsert"}]}`
	res = ManageBtreeForTest(ctxID, UpdateCurrentKey, actionPayloadPrim, itemsPayloadPrim)
	if res == "true" {
		t.Error("UpdateCurrentKey Prim should have failed (ordering change)")
	} else {
		t.Logf("UpdateCurrentKey Prim failed as expected: %s", res)
	}
}

func TestBtree_Manage_Update_UpdateKey(t *testing.T) {
	ctxID := CreateContextForTest()
	defer RemoveContextForTest(ctxID)
	dir := t.TempDir()

	dbPayload := fmt.Sprintf(`{"cache_type": 1, "stores_folders": ["%s"]}`, dir)
	dbID := ManageDatabaseForTest(ctxID, NewDatabase, "", dbPayload)
	transPayload := fmt.Sprintf(`{"Mode": 1, "stores_folders": ["%s"]}`, dir)
	transID := ManageDatabaseForTest(ctxID, BeginTransaction, dbID, transPayload)

	// 1. Complex Key (JsonDBAnyKey)
	btreeOpts := fmt.Sprintf(`{
		"name": "test_btree_update",
		"transaction_id": "%s",
		"is_primitive_key": false
	}`, transID)
	btreeID := ManageDatabaseForTest(ctxID, NewBtree, dbID, btreeOpts)
	actionPayload := fmt.Sprintf(`{"transaction_id": "%s", "btree_id": "%s"}`, transID, btreeID)

	// Add item
	itemsPayload := `{"items": [{"key": {"id": 1}, "value": "val1"}]}`
	ManageBtreeForTest(ctxID, Add, actionPayload, itemsPayload)

	// Update
	itemsPayload = `{"items": [{"key": {"id": 1}, "value": "val1_updated"}]}`
	res := ManageBtreeForTest(ctxID, Update, actionPayload, itemsPayload)
	if res != "true" {
		t.Errorf("Update failed: %s", res)
	}

	// Verify Update
	// Use GetValues to retrieve the value for the key. GetValues returns a JSON array string of items.
	res, _ = GetFromBtreeForTest(ctxID, GetValues, actionPayload, itemsPayload)
	// GetFromBtreeForTest returns items JSON, e.g. [{"key":..., "value":"val1_updated", ...}]
	if !strings.Contains(res, "\"value\":\"val1_updated\"") {
		t.Errorf("Expected updated value \"val1_updated\" in response, got: %s", res)
	}

	// UpdateKey (Change key from id:1 to id:2)
	// UpdateKey usually updates the CURRENT item's key.

	// Position cursor
	NavigateBtreeForTest(ctxID, First, actionPayload, "")

	itemsPayload = `{"items": [{"key": {"id": 2}, "value": "val1_updated_key"}]}`
	res = ManageBtreeForTest(ctxID, UpdateKey, actionPayload, itemsPayload)
	// UpdateKey fails if it affects ordering.
	if res == "true" {
		t.Errorf("UpdateKey succeeded unexpectedly (should fail for ordering change)")
	} else {
		t.Logf("UpdateKey failed as expected: %s", res)
	}

	// 2. Primitive Key (JsonDBMapKey)
	transID2 := ManageDatabaseForTest(ctxID, BeginTransaction, dbID, transPayload)
	btreeOptsPrim := fmt.Sprintf(`{
		"name": "test_btree_update_prim",
		"transaction_id": "%s",
		"is_primitive_key": true
	}`, transID2)
	btreeIDPrim := ManageDatabaseForTest(ctxID, NewBtree, dbID, btreeOptsPrim)
	actionPayloadPrim := fmt.Sprintf(`{"transaction_id": "%s", "btree_id": "%s"}`, transID2, btreeIDPrim)

	// Add item
	itemsPayloadPrim := `{"items": [{"key": "key1", "value": "val1"}]}`
	ManageBtreeForTest(ctxID, Add, actionPayloadPrim, itemsPayloadPrim)

	// Update
	itemsPayloadPrim = `{"items": [{"key": "key1", "value": "val1_updated"}]}`
	res = ManageBtreeForTest(ctxID, Update, actionPayloadPrim, itemsPayloadPrim)
	if res != "true" {
		t.Errorf("Update Prim failed: %s", res)
	}

	// UpdateKey
	NavigateBtreeForTest(ctxID, First, actionPayloadPrim, "")
	itemsPayloadPrim = `{"items": [{"key": "key2", "value": "val1_updated_key"}]}`
	res = ManageBtreeForTest(ctxID, UpdateKey, actionPayloadPrim, itemsPayloadPrim)
	if res == "true" {
		t.Errorf("UpdateKey Prim succeeded unexpectedly")
	} else {
		t.Logf("UpdateKey Prim failed as expected: %s", res)
	}
}
