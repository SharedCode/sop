package jobs

import (
	"blackicedata.com/hive"
)

type jobsDB struct {
	JobsRepository        hive.KeyValueStoreFetchAll[string, Job]
	PipelineRepository    hive.KeyValueStoreFetchAll[string, Pipeline]
	JobRunRepository      hive.KeyValueStoreFetchAll[string, JobRun]
	PipelineRunRepository hive.KeyValueStoreFetchAll[string, PipelineRun]
}

// Instantiate a global Jobs database.
var Database = jobsDB{
	JobsRepository:        NewJobsRepository(),
	JobRunRepository:      NewJobRunRepository(),
}
