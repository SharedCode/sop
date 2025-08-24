package erasure

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/klauspost/reedsolomon"
)

func Test_Encode_Decode(t *testing.T) {
	e, _ := NewErasure(4, 2)
	d := []byte{1, 2, 3, 4, 5}
	shards, err := e.Encode(d)
	if err != nil {
		t.Error(err)
	}
	sm := make([][]byte, len(shards))
	var stuffZeroCount int
	for i := range shards {
		md := e.ComputeShardMetadata(len(d), shards, 0)
		stuffZeroCount = int(md[0])
		sm[i] = md
	}
	if stuffZeroCount != 3 {
		t.Errorf("stuff 0 count got %d, expected 3", stuffZeroCount)
	}

	dr := e.Decode(shards, sm)
	if dr.Error != nil {
		t.Error(dr.Error)
	}

	if len(dr.DecodedData) != len(d) {
		t.Errorf("DecodedData got %d length, expected %d", len(dr.DecodedData), len(d))
	}
}

func Test_bitrot(t *testing.T) {
	e, _ := NewErasure(4, 2)
	d := []byte{1, 2, 3, 4, 5}
	shards, err := e.Encode(d)
	if err != nil {
		t.Error(err)
	}
	sm := make([][]byte, len(shards))
	for i := range shards {
		md := e.ComputeShardMetadata(len(d), shards, i)
		sm[i] = md
	}

	// Change one byte to simulate bitrot on that location losing a byte.
	shards[1][1] = 0

	dr := e.Decode(shards, sm)
	if dr.Error != nil {
		t.Error(dr.Error)
	}
	if dr.ReconstructedShardsIndeces[0] != 1 {
		t.Errorf("ReconstructedShardsIndeces got %v, expected 1", dr.ReconstructedShardsIndeces[0])
	}

	d = []byte{1, 2, 3, 4, 5}
	if !bytes.Equal(dr.DecodedData, d) {
		t.Errorf("DecodedData got %v, expected %v", dr.DecodedData, d)
	}
}

func Test_FailedWriteOnOne(t *testing.T) {
	e, _ := NewErasure(4, 2)
	d := []byte{1, 2, 3, 4, 5}
	shards, err := e.Encode(d)
	if err != nil {
		t.Error(err)
	}
	sm := make([][]byte, len(shards))
	for i := range shards {
		md := e.ComputeShardMetadata(len(d), shards, i)
		sm[i] = md
	}

	// Change one byte to simulate bitrot on that location losing a byte.
	shards[1][1] = 0

	dr := e.Decode(shards, sm)
	if dr.Error != nil {
		t.Error(dr.Error)
	}
	if dr.ReconstructedShardsIndeces[0] != 1 {
		t.Errorf("ReconstructedShardsIndeces got %v, expected 1", dr.ReconstructedShardsIndeces[0])
	}

	d = []byte{1, 2, 3, 4, 5}
	if !bytes.Equal(dr.DecodedData, d) {
		t.Errorf("DecodedData got %v, expected %v", dr.DecodedData, d)
	}
}

// encWrapper embeds a real reedsolomon.Encoder and allows overriding selected methods
// to exercise error branches deterministically.
type encWrapper struct {
	reedsolomon.Encoder
	splitErr           error
	encodeErr          error
	verifyOK           *bool
	verifyErr          error
	joinErr            error
	reconstructErr     error
	reconstructSomeErr error
}

func (w encWrapper) Split(data []byte) ([][]byte, error) {
	if w.splitErr != nil {
		return nil, w.splitErr
	}
	return w.Encoder.Split(data)
}
func (w encWrapper) Encode(shards [][]byte) error {
	if w.encodeErr != nil {
		return w.encodeErr
	}
	return w.Encoder.Encode(shards)
}
func (w encWrapper) Verify(shards [][]byte) (bool, error) {
	if w.verifyOK != nil {
		return *w.verifyOK, w.verifyErr
	}
	return w.Encoder.Verify(shards)
}
func (w encWrapper) Join(dst io.Writer, shards [][]byte, size int) error {
	if w.joinErr != nil {
		return w.joinErr
	}
	return w.Encoder.Join(dst, shards, size)
}
func (w encWrapper) Reconstruct(shards [][]byte) error {
	if w.reconstructErr != nil {
		return w.reconstructErr
	}
	return w.Encoder.Reconstruct(shards)
}
func (w encWrapper) ReconstructSome(shards [][]byte, dataOnly []bool) error {
	if w.reconstructSomeErr != nil {
		return w.reconstructSomeErr
	}
	return w.Encoder.ReconstructSome(shards, dataOnly)
}

func newWrappedErasure(t *testing.T, data, parity int, w encWrapper) *Erasure {
	t.Helper()
	e, err := NewErasure(data, parity)
	if err != nil {
		t.Fatalf("NewErasure err: %v", err)
	}
	// seed wrapper with a real encoder if none provided
	if w.Encoder == nil {
		real, _ := reedsolomon.New(data, parity)
		w.Encoder = real
	}
	e.encoder = w
	return e
}

func TestNewErasure_TooManyShards_Error(t *testing.T) {
	if _, err := NewErasure(200, 100); err == nil {
		t.Fatalf("expected error when data+parity > 256")
	}
}

func TestNewErasure_EncoderInitError(t *testing.T) {
	// reedsolomon.New requires data>0 and parity>0, so zero should error.
	if _, err := NewErasure(0, 2); err == nil {
		t.Fatalf("expected error when dataShards == 0")
	}
}

func TestEncode_Errors(t *testing.T) {
	data := []byte("hello world")
	// Split error
	e1 := newWrappedErasure(t, 4, 2, encWrapper{splitErr: errors.New("split fail")})
	if _, err := e1.Encode(data); err == nil {
		t.Fatalf("expected split error")
	}
	// Encode error
	e2 := newWrappedErasure(t, 4, 2, encWrapper{encodeErr: errors.New("encode fail")})
	if shards, err := e2.encoder.Split(data); err != nil {
		t.Fatalf("unexpected split err: %v", err)
	} else {
		_ = shards // ensure code path consistency
	}
	if _, err := e2.Encode(data); err == nil {
		t.Fatalf("expected encode error")
	}
}

func TestDecode_EmptyShards_ReturnsError(t *testing.T) {
	e := newWrappedErasure(t, 4, 2, encWrapper{})
	dr := e.Decode(nil, nil)
	if dr.Error == nil {
		t.Fatalf("expected error for empty shards")
	}
}

func TestDecode_ReconstructMissingShards_ErrorPropagates(t *testing.T) {
	// Force Verify=false to take the reconstruction path, and make ReconstructSome fail.
	v := false
	e := newWrappedErasure(t, 4, 2, encWrapper{verifyOK: &v, reconstructSomeErr: errors.New("recon some fail")})
	shards := make([][]byte, e.DataShardsCount+e.ParityShardsCount)
	// Simulate one missing shard
	shards[1] = nil
	sm := make([][]byte, len(shards))
	sm[0] = make([]byte, MetaDataSize) // ensure metadata slice non-nil
	dr := e.Decode(shards, sm)
	if dr.Error == nil {
		t.Fatalf("expected error bubbled up from ReconstructSome")
	}
}

func TestDecode_FinalAttemptFailed_WhenChecksumsMatch(t *testing.T) {
	// Verify=false both times, no missing shards, checksum check passes -> final attempt fails.
	v := false
	eReal, _ := NewErasure(4, 2)
	data := []byte{1, 2, 3, 4, 5}
	shards, _ := eReal.Encode(data)
	sm := make([][]byte, len(shards))
	for i := range shards {
		sm[i] = eReal.ComputeShardMetadata(len(data), shards, i)
	}
	e := newWrappedErasure(t, 4, 2, encWrapper{verifyOK: &v})
	dr := e.Decode(shards, sm)
	if dr.Error == nil || fmt.Sprint(dr.Error) == "" {
		t.Fatalf("expected final attempt failure error; got nil")
	}
}

func TestDecode_JoinError(t *testing.T) {
	v := true
	e := newWrappedErasure(t, 4, 2, encWrapper{verifyOK: &v, joinErr: errors.New("join fail")})
	// Build valid shards via the real encoder, but pass some nil metadata prefix to cover skip loop.
	eReal, _ := NewErasure(4, 2)
	data := []byte("abcdefg")
	shards, _ := eReal.Encode(data)
	sm := make([][]byte, len(shards))
	for i := range shards {
		sm[i] = eReal.ComputeShardMetadata(len(data), shards, i)
	}
	// Nil-out some leading metadata entries to exercise the search-for-first-non-nil branch later (on success runs).
	// Even though Join will fail early, we still verify the error path here.
	sm[0] = nil
	sm[1] = nil
	dr := e.Decode(shards, sm)
	if dr.Error == nil {
		t.Fatalf("expected join error")
	}
}

func TestDecode_SkipNilMetadata_Passes(t *testing.T) {
	// Success path ensuring Decode skips leading nil metadata entries before trimming zeros.
	e, _ := NewErasure(4, 2)
	data := []byte("hello world")
	shards, _ := e.Encode(data)
	sm := make([][]byte, len(shards))
	for i := range shards {
		sm[i] = e.ComputeShardMetadata(len(data), shards, i)
	}
	sm[0] = nil
	sm[1] = nil
	dr := e.Decode(shards, sm)
	if dr.Error != nil {
		t.Fatalf("unexpected decode error: %v", dr.Error)
	}
	if !bytes.Equal(dr.DecodedData, data) {
		t.Fatalf("decoded data mismatch")
	}
}

func Test_detectBadShardsThenReconstruct_ErrorPaths(t *testing.T) {
	// Prepare shards + metadata, then corrupt checksum expectations.
	base, _ := NewErasure(4, 2)
	data := []byte("xyz123")
	shards, _ := base.Encode(data)
	sm := make([][]byte, len(shards))
	for i := range shards {
		sm[i] = base.ComputeShardMetadata(len(data), shards, i)
	}

	// Case 1: Reconstruct returns error
	v := false
	e1 := newWrappedErasure(t, 4, 2, encWrapper{verifyOK: &v, reconstructErr: errors.New("reconstruct fail")})
	// Alter one shard so checksum mismatches and the function marks it nil then tries reconstruct
	shards1 := make([][]byte, len(shards))
	copy(shards1, shards)
	shards1[0] = append([]byte{}, shards1[0]...)
	shards1[0][0] ^= 0xFF
	dr1 := e1.detectBadShardsThenReconstruct(shards1, sm)
	if dr1.Error == nil {
		t.Fatalf("expected error from Reconstruct")
	}

	// Case 2: Verify returns false after successful reconstruct, propagate verify error
	// Use a wrapper that reconstructs OK but Verify returns false with a specific error.
	verifyErr := errors.New("verify fail")
	e2 := newWrappedErasure(t, 4, 2, encWrapper{verifyOK: &v, verifyErr: verifyErr})
	shards2 := make([][]byte, len(shards))
	copy(shards2, shards)
	shards2[1] = append([]byte{}, shards2[1]...)
	shards2[1][0] ^= 0xAA
	dr2 := e2.detectBadShardsThenReconstruct(shards2, sm)
	if dr2.Error == nil || !errors.Is(dr2.Error, verifyErr) {
		t.Fatalf("expected verify error to propagate; got %v", dr2.Error)
	}
}
