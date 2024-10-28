package jobs

import (
	"context"
	"fmt"

	"blackicedata.com/hive"
)

type jobsRepository struct {
	lookup map[string]Job
}

// NewJobsRepository instantiates a new Jobs Repository.
func NewJobsRepository() hive.KeyValueStoreFetchAll[string, Job] {
	return &jobsRepository{
		lookup: make(map[string]Job),
	}
}

// Fetch entry(ies) with given Job ID(s).
func (jr *jobsRepository) Fetch(ctx context.Context, jobIDs ...string) hive.KeyValueStoreResponse[hive.KeyValuePair[string, Job]] {
	var r hive.KeyValueStoreResponse[hive.KeyValuePair[string, Job]]
	details := make([]hive.KeyValueStoreItemActionResponse[hive.KeyValuePair[string, Job]], 0, len(jobIDs))
	for _, jid := range jobIDs {
		if j, ok := jr.lookup[jid]; ok {
			details = append(details, hive.KeyValueStoreItemActionResponse[hive.KeyValuePair[string, Job]]{
				Payload: hive.KeyValuePair[string, Job]{
					Key:   jid,
					Value: j,
				},
			})
		}
	}
	r.Details = details
	if len(details) == 0 {
		r.Error = fmt.Errorf("did not find the item(s) to fetch")
	}
	return r
}

// FethAll retrieves all entries from the store.
func (jr *jobsRepository) FetchAll(ctx context.Context) hive.KeyValueStoreResponse[hive.KeyValuePair[string, Job]] {
	var r hive.KeyValueStoreResponse[hive.KeyValuePair[string, Job]]
	details := make([]hive.KeyValueStoreItemActionResponse[hive.KeyValuePair[string, Job]], len(jr.lookup))
	i := 0
	for key, value := range jr.lookup {
		details[i] = hive.KeyValueStoreItemActionResponse[hive.KeyValuePair[string, Job]]{
			Payload: hive.KeyValuePair[string, Job]{
				Key:   key,
				Value: value,
			},
		}
		i++
	}
	r.Details = details
	return r
}

// Add entry(ies) to the store.
func (jr *jobsRepository) Add(ctx context.Context, jobs ...hive.KeyValuePair[string, Job]) hive.KeyValueStoreResponse[hive.KeyValuePair[string, Job]] {
	for _, j := range jobs {
		jr.lookup[j.Key] = j.Value
	}
	return hive.KeyValueStoreResponse[hive.KeyValuePair[string, Job]]{}
}

// Update entry(ies) of the store.
func (jr *jobsRepository) Update(ctx context.Context, jobs ...hive.KeyValuePair[string, Job]) hive.KeyValueStoreResponse[hive.KeyValuePair[string, Job]] {
	for _, j := range jobs {
		jr.lookup[j.Key] = j.Value
	}
	return hive.KeyValueStoreResponse[hive.KeyValuePair[string, Job]]{}
}

// Remove entry(ies) from the store given their names.
func (jr *jobsRepository) Remove(ctx context.Context, jobIDs ...string) hive.KeyValueStoreResponse[string] {
	c := len(jr.lookup)
	for _, jid := range jobIDs {
		delete(jr.lookup, jid)
	}
	if len(jr.lookup) == c {
		return hive.KeyValueStoreResponse[string]{
			Error: fmt.Errorf("did not find any item to remove"),
		}
	}
	return hive.KeyValueStoreResponse[string]{}
}
