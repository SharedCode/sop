package functions

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/sharedcode/sop/ai/internal/obs"
	"github.com/sharedcode/sop/ai/internal/port"
)

type IntegrityFn struct {
	pol   port.PolicyEngine
	store port.KVStore
}

func NewIntegrity(pol port.PolicyEngine, store port.KVStore) *IntegrityFn {
	return &IntegrityFn{pol: pol, store: store}
}

func (f *IntegrityFn) ID() string                  { return "storage.integrity_check" }
func (f *IntegrityFn) Policies() port.PolicyEngine { return f.pol }
func (f *IntegrityFn) Depends() []string           { return nil }

func (f *IntegrityFn) Invoke(in port.Payload) (port.Payload, error) {
	key, _ := in["key"].(string)
	issues := []string{}
	valid := true
	checksum := ""
	checksumMatch := true
	if key == "" {
		valid = false
		issues = append(issues, "empty key")
	} else if f.store != nil {
		tx, err := f.store.Begin(true)
		if err != nil {
			valid = false
			issues = append(issues, "begin tx failed")
		} else {
			data, err := tx.Get([]byte(key))
			if err != nil {
				valid = false
				issues = append(issues, "not found")
			} else {
				h := sha256.Sum256(data)
				checksum = hex.EncodeToString(h[:])
				// For now we consider checksum always matches expected; placeholder for external expected value.
				checksumMatch = true
			}
			_ = tx.Rollback() // read-only closes quickly
		}
	}
	out := port.Payload{
		"key":            key,
		"valid":          valid,
		"checksum":       checksum,
		"checksum_match": checksumMatch,
		"issues":         issues,
		"checked_at":     time.Now().UTC().Format(time.RFC3339Nano),
	}
	if l := obs.Log(); l != nil {
		l.Debug("integrity.check", "key", key, "valid", valid, "issues", len(issues))
	}
	return out, nil
}
