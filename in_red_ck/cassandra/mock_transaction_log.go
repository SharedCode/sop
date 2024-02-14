package cassandra

import (
	"context"

	"github.com/SharedCode/sop"
)

type mockTransactionLog struct {
	datesLogs map[string]map[sop.UUID]map[string]interface{}
}

func NewMockTransactionLog() TransactionLog {
	return &mockTransactionLog{
		datesLogs: make(map[string]map[sop.UUID]map[string]interface{}),
	}
}

// GetOne fetches a blob from blob table.
func (tl *mockTransactionLog) GetOne(ctx context.Context) (sop.UUID, []sop.KeyValuePair[string, interface{}], error) {
	return sop.NilUUID, nil, nil
}

func (tl *mockTransactionLog) Initiate(ctx context.Context, tid sop.UUID, commitFunctionName string, payload interface{}) error {
	date := Now().Format("02-01-2006")
	dayLogs, ok := tl.datesLogs[date]
	if !ok {
		dayLogs = make(map[sop.UUID]map[string]interface{})
		dayLogs[tid] = make(map[string]interface{})
	}
	dayLogs[tid][commitFunctionName] = payload
	tl.datesLogs[date] = dayLogs
	return nil
}

// Add blob(s) to the Blob store.
func (tl *mockTransactionLog) Add(ctx context.Context, tid sop.UUID, commitFunctionName string, payload interface{}) error {
	date := Now().Format("02-01-2006")
	dayLogs := tl.datesLogs[date]
	dayLogs[tid][commitFunctionName] = payload
	tl.datesLogs[date] = dayLogs
	return nil
}

// Remove will delete(non-logged) node records from different Blob stores(node tables).
func (tl *mockTransactionLog) Remove(ctx context.Context, tid sop.UUID) error {
	date := Now().Format("02-01-2006")
	dayLogs := tl.datesLogs[date]
	delete(dayLogs, tid)
	tl.datesLogs[date] = dayLogs
	return nil
}
