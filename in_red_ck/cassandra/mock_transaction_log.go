package cassandra

import (
	"context"
	"encoding/json"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_memory"
)

type mockTransactionLog struct {
	datesLogs in_memory.BtreeInterface[string, map[sop.UUID][]sop.KeyValuePair[int, []byte]]
	logsDates map[sop.UUID]string
}

func NewMockTransactionLog() TransactionLog {
	return &mockTransactionLog{
		datesLogs: in_memory.NewBtree[string, map[sop.UUID][]sop.KeyValuePair[int, []byte]](true),
		logsDates: make(map[sop.UUID]string),
	}
}

// GetOne returns the oldest transaction ID.
func (tl *mockTransactionLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, interface{}], error) {
	if tl.datesLogs.First() {
		kt, _ := time.Parse(dateHour, tl.datesLogs.GetCurrentKey())
		// Cap the returned entries to older than an hour to safeguard ongoing transactions.
		nt, _ := time.Parse(dateHour, Now().Format(dateHour))
		cappedTime := nt.Add(-time.Duration(1*time.Hour))
		if kt.Unix() < cappedTime.Unix() {
			v := tl.datesLogs.GetCurrentValue()
			for kk, vv := range v {
				r := make([]sop.KeyValuePair[int, interface{}], len(vv))
				for ii := range vv {
					var target interface{}
					json.Unmarshal(vv[ii].Value, &target)	
					r[ii].Key = vv[ii].Key
					r[ii].Value = target
				}
				return kk, tl.datesLogs.GetCurrentKey(), r, nil
			}
		}
	}
	return sop.NilUUID, "", nil, nil
}

func (tl *mockTransactionLog) Initiate(ctx context.Context, tid sop.UUID, commitFunction int, payload interface{}) (string, error) {
	date := Now().Format(dateHour)
	var dayLogs map[sop.UUID][]sop.KeyValuePair[int, []byte]
	if !tl.datesLogs.FindOne(date, false) {
		dayLogs = make(map[sop.UUID][]sop.KeyValuePair[int, []byte])
		dayLogs[tid] = make([]sop.KeyValuePair[int, []byte], 0, 13)
	} else {
		dayLogs = tl.datesLogs.GetCurrentValue()
	}
	ba, _ :=  json.Marshal(payload)
	dayLogs[tid] = append(dayLogs[tid], sop.KeyValuePair[int, []byte]{
		Key: commitFunction,
		Value: ba,
	})
	tl.datesLogs.Add(date, dayLogs)
	tl.logsDates[tid] = date
	return date, nil
}

// Add blob(s) to the Blob store.
func (tl *mockTransactionLog) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload interface{}) error {
	date := Now().Format(dateHour)
	tl.datesLogs.FindOne(date, false)
	dayLogs := tl.datesLogs.GetCurrentValue()
	ba, _ := json.Marshal(payload)
	dayLogs[tid] = append(dayLogs[tid], sop.KeyValuePair[int, []byte]{
		Key: commitFunction,
		Value: ba,
	})
	tl.datesLogs.UpdateCurrentItem(dayLogs)
	tl.logsDates[tid] = date
	return nil
}

// Remove will delete(non-logged) node records from different Blob stores(node tables).
func (tl *mockTransactionLog) Remove(ctx context.Context, tid sop.UUID, hour string) error {
	date := tl.logsDates[tid]
	if tl.datesLogs.FindOne(date, false) {
		for {
			dayLogs := tl.datesLogs.GetCurrentValue()
			if _, ok := dayLogs[tid]; ok {
				delete(dayLogs, tid)
				if len(dayLogs) == 0 {
					tl.datesLogs.RemoveCurrentItem()
				} else {
					tl.datesLogs.UpdateCurrentItem(dayLogs)
				}
				return nil
			}
			if !tl.datesLogs.Next() {
				break
			}
		}
	}
	return nil
}
