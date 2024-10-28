package jobs

import (
	"context"
	"fmt"

	"blackicedata.com/hive"
)

type jobRunRepository struct {
	lookup map[string]JobRun
}

func NewJobRunRepository() hive.KeyValueStoreFetchAll[string, JobRun] {
	return &jobRunRepository{
		lookup: make(map[string]JobRun),
	}
}

// Fetch entry(ies) with given Job Run ID(s).
func (jrr *jobRunRepository) Fetch(ctx context.Context, jobRunIDs ...string) hive.KeyValueStoreResponse[hive.KeyValuePair[string, JobRun]] {
	var r hive.KeyValueStoreResponse[hive.KeyValuePair[string, JobRun]]
	details := make([]hive.KeyValueStoreItemActionResponse[hive.KeyValuePair[string, JobRun]], 0, len(jobRunIDs))
	for _, jrid := range jobRunIDs {
		if j, ok := jrr.lookup[jrid]; ok {
			details = append(details, hive.KeyValueStoreItemActionResponse[hive.KeyValuePair[string, JobRun]]{
				Payload: hive.KeyValuePair[string, JobRun]{
					Key:   jrid,
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

// FetchAll retrieves all entries from the store.
func (jrr *jobRunRepository) FetchAll(ctx context.Context) hive.KeyValueStoreResponse[hive.KeyValuePair[string, JobRun]] {
	var r hive.KeyValueStoreResponse[hive.KeyValuePair[string, JobRun]]
	details := make([]hive.KeyValueStoreItemActionResponse[hive.KeyValuePair[string, JobRun]], len(jrr.lookup))
	i := 0
	for key, value := range jrr.lookup {
		details[i] = hive.KeyValueStoreItemActionResponse[hive.KeyValuePair[string, JobRun]]{
			Payload: hive.KeyValuePair[string, JobRun]{
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
func (jrr *jobRunRepository) Add(ctx context.Context, jobRuns ...hive.KeyValuePair[string, JobRun]) hive.KeyValueStoreResponse[hive.KeyValuePair[string, JobRun]] {
	for _, j := range jobRuns {
		jrr.lookup[j.Key] = j.Value
	}
	return hive.KeyValueStoreResponse[hive.KeyValuePair[string, JobRun]]{}
}

// Update entry(ies) of the store.
func (jrr *jobRunRepository) Update(ctx context.Context, jobRuns ...hive.KeyValuePair[string, JobRun]) hive.KeyValueStoreResponse[hive.KeyValuePair[string, JobRun]] {
	for _, j := range jobRuns {
		jrr.lookup[j.Key] = j.Value
	}
	return hive.KeyValueStoreResponse[hive.KeyValuePair[string, JobRun]]{}
}

// Remove entry(ies) from the store given their names.
func (jrr *jobRunRepository) Remove(ctx context.Context, jobRunIDs ...string) hive.KeyValueStoreResponse[string] {
	c := len(jrr.lookup)
	for _, jrid := range jobRunIDs {
		delete(jrr.lookup, jrid)
	}
	if len(jrr.lookup) == c {
		return hive.KeyValueStoreResponse[string]{
			Error: fmt.Errorf("did not find any item to remove"),
		}
	}
	return hive.KeyValueStoreResponse[string]{}
}
