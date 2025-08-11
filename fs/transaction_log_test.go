package fs

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

type payload struct {
	Abc string
	Xyc int
}

var uuid2, _ = sop.ParseUUID("6ba7b810-9dad-11d1-80b4-00c04fd430c9")

func TestTransactionLogAdd(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	ba, _ := json.Marshal(payload{
		Abc: "abc", Xyc: 123,
	})
	err := tl.Add(ctx, uuid, 1, ba)
	if err != nil {
		t.Errorf("error got on tl.Add, details: %v", err)
	}
}

func TestTransactionLogGetOne(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	// Make freshly-written files eligible for selection.
	ageLimit = 0
	// Seed a record so GetOne has something to return.
	baSeed, _ := json.Marshal(payload{Abc: "abc", Xyc: 123})
	if err := tl.Add(ctx, uuid, 1, baSeed); err != nil {
		t.Fatalf("seed Add failed: %v", err)
	}
	uid, hour, tlogdata, err := tl.GetOne(ctx)
	if uid.IsNil() {
		return
	}
	if err != nil {
		t.Errorf("error got on tl.GetOne, details: %v", err)
	}
	if uid != uuid {
		t.Errorf("expected: %v, got: %v", uuid, uid)
	}
	if tlogdata[0].Key != 1 {
		t.Errorf("log data key expected: %d, got: %d", 1, tlogdata[0].Key)
	}
	var p payload
	json.Unmarshal(tlogdata[0].Value, &p)

	if p.Abc != "abc" {
		t.Errorf("Abc expected: abc, got: %s", p.Abc)
	}

	uid, tlogdata2, err := tl.GetOneOfHour(ctx, hour)

	if err != nil {
		t.Errorf("error got on tl.GetOne, details: %v", err)
	}
	if uid != uuid {
		t.Errorf("expected: %v, got: %v", uuid, uid)
	}
	if tlogdata2[0].Key != 1 {
		t.Errorf("log data key expected: %d, got: %d", 1, tlogdata2[0].Key)
	}
	json.Unmarshal(tlogdata2[0].Value, &p)

	if p.Abc != "abc" {
		t.Errorf("Abc expected: abc, got: %s", p.Abc)
	}
}

func TestTransactionLogAdd2(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	ba, _ := json.Marshal(payload{
		Abc: "abc", Xyc: 123,
	})
	ba2, _ := json.Marshal(payload{
		Abc: "xyz", Xyc: 456,
	})
	err := tl.Add(ctx, uuid2, 1, ba)
	if err != nil {
		t.Errorf("error got on tl.Add, details: %v", err)
	}
	err = tl.Add(ctx, uuid2, 2, ba2)
	if err != nil {
		t.Errorf("error got on tl.Add, details: %v", err)
	}
}

func TestTransactionLogGetOne2(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	// Make freshly-written files eligible for selection.
	ageLimit = 0
	// Seed two records in order.
	ba1, _ := json.Marshal(payload{Abc: "abc", Xyc: 123})
	ba2, _ := json.Marshal(payload{Abc: "xyz", Xyc: 456})
	if err := tl.Add(ctx, uuid2, 1, ba1); err != nil {
		t.Fatalf("seed Add failed: %v", err)
	}
	if err := tl.Add(ctx, uuid2, 2, ba2); err != nil {
		t.Fatalf("seed Add failed: %v", err)
	}
	uid, _, tlogdata, err := tl.GetOne(ctx)
	if uid.IsNil() {
		return
	}
	if err != nil {
		t.Errorf("error got on tl.GetOne, details: %v", err)
	}
	if uid != uuid2 {
		t.Errorf("expected: %v, got: %v", uuid, uid)
	}
	if tlogdata[0].Key != 1 {
		t.Errorf("log data key expected: %d, got: %d", 1, tlogdata[0].Key)
	}
	var p payload
	json.Unmarshal(tlogdata[0].Value, &p)

	if p.Abc != "abc" {
		t.Errorf("Abc expected: abc, got: %s", p.Abc)
	}

	if tlogdata[1].Key != 2 {
		t.Errorf("log data key expected: %d, got: %d", 2, tlogdata[0].Key)
	}

	json.Unmarshal(tlogdata[1].Value, &p)

	if p.Abc != "xyz" {
		t.Errorf("Abc expected: xyz, got: %s", p.Abc)
	}
}

func TestTransactionLogRemove2(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	// Seed a log entry to ensure a file exists before removal.
	payloadBytes, _ := json.Marshal(payload{Abc: "seed", Xyc: 1})
	if err := tl.Add(ctx, uuid2, 99, payloadBytes); err != nil {
		t.Fatalf("seed Add failed: %v", err)
	}
	err := tl.Remove(ctx, uuid2)
	if err != nil {
		t.Errorf("Remove err: %v", err)
	}
}

func TestTransactionLogAddRemove(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	ba, _ := json.Marshal(payload{
		Abc: "abc", Xyc: 123,
	})
	err := tl.Add(ctx, uuid, 1, ba)
	if err != nil {
		t.Errorf("error got on tl.Add, details: %v", err)
	}
	err = tl.Remove(ctx, uuid)
	if err != nil {
		t.Errorf("error got on tl.Add, details: %v", err)
	}
}

type fi struct {
	name  string
	isDir bool
	ty    os.FileMode
	info  os.FileInfo
}

func (f fi) Name() string {
	return f.name
}

func (f fi) IsDir() bool {
	return f.isDir
}

func (f fi) Type() os.FileMode {
	return f.ty
}

func (f fi) Info() (os.FileInfo, error) {
	return f.info, nil
}

func TestGetFilesSorted(t *testing.T) {
	fileInfoWithTimes := make([]FileInfoWithModTime, 0, 5)

	f := fi{
		name: "foo",
		ty:   os.ModeExclusive,
	}

	fileInfoWithTimes = append(fileInfoWithTimes, FileInfoWithModTime{f, time.Now().Add(-5 * time.Minute)})
	f = fi{
		name: "foo2",
		ty:   os.ModeExclusive,
	}
	fileInfoWithTimes = append(fileInfoWithTimes, FileInfoWithModTime{f, time.Now().Add(-10 * time.Minute)})
	f = fi{
		name: "bar",
		ty:   os.ModeExclusive,
	}
	fileInfoWithTimes = append(fileInfoWithTimes, FileInfoWithModTime{f, time.Now().Add(-1 * time.Minute)})
	f = fi{
		name: "hello",
		ty:   os.ModeExclusive,
	}
	fileInfoWithTimes = append(fileInfoWithTimes, FileInfoWithModTime{f, time.Now().Add(-15 * time.Minute)})
	f = fi{
		name: "world",
		ty:   os.ModeExclusive,
	}
	fileInfoWithTimes = append(fileInfoWithTimes, FileInfoWithModTime{f, time.Now().Add(-3 * time.Minute)})

	sort.Sort(ByModTime(fileInfoWithTimes))

	if fileInfoWithTimes[0].Name() != "hello" {
		t.Errorf("got %s expected hello", fileInfoWithTimes[0].Name())
	}
	if fileInfoWithTimes[1].Name() != "foo2" {
		t.Errorf("got %s expected hello", fileInfoWithTimes[0].Name())
	}

	fmt.Printf("\nsorted data: %v", fileInfoWithTimes)

}
