package cassandra

import (
	"context"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_memory"
)

// DateHourLayout format mask string.
const DateHourLayout = "2006-01-02T15"

// Now lambda to allow unit test to inject replayable time.Now.
var Now = time.Now

type MockTransactionLog struct {
	datesLogs in_memory.BtreeInterface[string, map[sop.UUID][]sop.KeyValuePair[int, []byte]]
	logsDates map[sop.UUID]string
}

func NewMockTransactionLog() sop.TransactionLog {
	return &MockTransactionLog{
		datesLogs: in_memory.NewBtree[string, map[sop.UUID][]sop.KeyValuePair[int, []byte]](true),
		logsDates: make(map[sop.UUID]string),
	}
}

// GetOne returns the oldest transaction ID.
func (tl *MockTransactionLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	if tl.datesLogs.First() {
		kt, _ := time.Parse(DateHourLayout, tl.datesLogs.GetCurrentKey())
		// Cap the returned entries to older than an hour to safeguard ongoing transactions.
		nt, _ := time.Parse(DateHourLayout, Now().Format(DateHourLayout))
		cappedTime := nt.Add(-time.Duration(1 * time.Hour))
		if kt.Unix() < cappedTime.Unix() {
			v := tl.datesLogs.GetCurrentValue()
			for kk, vv := range v {
				r := make([]sop.KeyValuePair[int, []byte], len(vv))
				for ii := range vv {
					r[ii].Key = vv[ii].Key
					r[ii].Value = vv[ii].Value
				}
				return kk, tl.datesLogs.GetCurrentKey(), r, nil
			}
		}
	}
	return sop.NilUUID, "", nil, nil
}

func (tl *MockTransactionLog) GetLogsDetails(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	if !tl.datesLogs.FindOne(hour, false) {
		return sop.NilUUID, nil, nil
	}
	v := tl.datesLogs.GetCurrentValue()
	for kk, vv := range v {
		r := make([]sop.KeyValuePair[int, []byte], len(vv))
		for ii := range vv {
			r[ii].Key = vv[ii].Key
			r[ii].Value = vv[ii].Value
		}
		return kk, r, nil
	}
	return sop.NilUUID, nil, nil
}

func (tl *MockTransactionLog) GetTIDLogs(tid sop.UUID) []sop.KeyValuePair[int, []byte] {
	if tl.datesLogs.First() {
		for {
			v := tl.datesLogs.GetCurrentValue()
			for kk, vv := range v {
				if kk != tid {
					continue
				}
				r := make([]sop.KeyValuePair[int, []byte], len(vv))
				for ii := range vv {
					r[ii].Key = vv[ii].Key
					r[ii].Value = vv[ii].Value
				}
				return r
			}
			if !tl.datesLogs.Next() {
				break
			}
		}
	}
	return nil
}

// Add blob(s) to the Blob store.
func (tl *MockTransactionLog) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	date := Now().Format(DateHourLayout)
	found := tl.datesLogs.FindOne(date, false)
	dayLogs := tl.datesLogs.GetCurrentValue()
	if dayLogs == nil {
		dayLogs = make(map[sop.UUID][]sop.KeyValuePair[int, []byte])
	}
	dayLogs[tid] = append(dayLogs[tid], sop.KeyValuePair[int, []byte]{
		Key:   commitFunction,
		Value: payload,
	})
	if found {
		tl.datesLogs.UpdateCurrentItem(dayLogs)
	} else {
		tl.datesLogs.Add(date, dayLogs)
	}
	tl.logsDates[tid] = date
	return nil
}

// Remove will delete(non-logged) node records from different Blob stores(node tables).
func (tl *MockTransactionLog) Remove(ctx context.Context, tid sop.UUID) error {
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
