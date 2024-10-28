package jobs

import (
	"blackicedata.com/hive"
)

type jobsDB struct {
	JobsRepository        hive.KeyValueStoreFetchAll[string, Job]
}

// Instantiate a global Jobs database.
var Database = jobsDB{
	JobsRepository:        NewJobsRepository(),
}
