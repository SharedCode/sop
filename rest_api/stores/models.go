package jobs

import (
	"time"
)

// Job run or Pipeline run status constants.
const (
	RUNNING   = "running"
	SUCCEEDED = "succeeded"
	FAILED    = "failed"
	CANCELLED = "cancelled"
	TIMEDOUT  = "timedout"
)

// Job basic details.
type Job struct {
	// Short name of the Job, needs to be unique.
	Name           string   `json:"name" minLength:"1" maxLength:"16"`
	Description    string   `json:"description" maxLength:"256"`
	// If -1 means no timeout, 0 means instantly timeout, > 0 means timeout that amount in seconds.
	TimeoutSeconds int      `json:"timeout_seconds" minimum:"-1"`
	RunAs          string   `json:"run_as" maxLength:"25"`
	Parameters     []string `json:"parameters"` // TODO: finalize Job parameters data types.
	// TODO: define Job schedule if we want to support scheduling running of jobs.
	IDofCodeToRun string `json:"id_of_code_to_run" minLength:"1" maxLength:"60"`
}

// PipelineJob is a specification describing a "run request" for a given Job or a Pipeline.
type PipelineJob struct {
	// Name of a Job or Pipeline.
	Name string `json:"name" minLength:"1" maxLength:"16"`
	// If true, then this Name pertains to a Job, otherwise to a Pipeline.
	IsJob bool `json:"is_job"`
	// If true then code runner can run the Job or Pipeline asynchronously, meaning, concurrently
	// with other unit(s). Code runner takes care of doing "wait for completion" for the entire Pipeline.
	Asynchronous bool `json:"asynchronous"`
}

// Pipeline is a group of Jobs that can be ran in a single host (pinned) or multiple host.
type Pipeline struct {
	// Short name of the Pipeline, needs to be unique.
	Name               string        `json:"name" minLength:"1" maxLength:"16"`
	Description        string        `json:"description" maxLength:"256"`
	Jobs               []PipelineJob `json:"jobs"`
	PinRunToSingleHost bool          `json:"pin_run_to_single_host"`
	// FlowState true means "code runner" will check output of a job and make that as input
	// to the next job in line. Otherwise will not. Asynchronous is recommended to be set to
	// false of PipelineJob where this data flow needs to occur.
	FlowState bool `json:"flow_state"`
}

// PipelineJobRun is a PipelineJobSpecification that has ID of the run.
type PipelineJobRun struct {
	// Name of a Job or Pipeline.
	Name string `json:"name" minLength:"1" maxLength:"16"`
	// If true, then this Name pertains to a Job, otherwise to a Pipeline.
	IsJob bool `json:"is_job"`
	// If true then code runner can run the Job or Pipeline asynchronously, meaning, multi-threaded
	// in concurrence with other unit(s). Code runner takes care of doing an "await" for the Pipeline.
	Asynchronous bool `json:"asynchronous"`

	// Job or Pipeline run ID.
	RunID string `json:"run_id" minLength:"1" maxLength:"60"`
}

// Pipeline Run contains each of the Jobs in the Pipeline Job run references (JobRun IDs).
type PipelineRun struct {
	ID                 string           `json:"id" minLength:"1" maxLength:"60"`
	PipelineName       string           `json:"pipeline_name" minLength:"1" maxLength:"16"`
	JobRuns            []PipelineJobRun `json:"job_runs"`
	PinRunToSingleHost bool             `json:"pin_run_to_single_host"`
	// FlowState true means "code runner" will check output of a job and make that as input
	// to the next job in line. Otherwise will not. Asynchronous is recommended to be set to
	// false of PipelineJob where this data flow needs to occur.
	FlowState bool
	// Pipeline run start time.
	StartDateTime time.Time `json:"start_date_time"`
	// Pipeline run end time.
	EndDateTime time.Time `json:"end_date_time"`
	// Status can be one of: succeeded, failed, running, cancelled, timedout, etc...
	Status string `json:"status" minLength:"1" maxLength:"25"`
	Error  string `json:"error,omitempty"`
}

// Job Run details.
type JobRun struct {
	ID      string `json:"id" minLength:"1" maxLength:"60"`
	JobName string `json:"job_name" minLength:"1" maxLength:"16"`
	// Job run start time.
	StartDateTime time.Time `json:"start_date_time"`
	// Job run end time.
	EndDateTime time.Time `json:"end_date_time"`

	// Should say either Java or Python. System can extract this from when it finds the code artifact to run,
	// IDofCodeToRun should specify an ID of a "code deployment" with details about language runtime.
	LanguageRuntime string `json:"language_runtime" minLength:"1" maxLength:"16"`
	// Status can be one of: succeeded, failed, running, cancelled, timedout, etc...
	Status string `json:"status" minLength:"1" maxLength:"25"`
	Error  string `json:"error,omitempty"`
	// Input data as passed in by the "code runner".
	Input []byte `json:"input,omitempty"`
	// Output data as returned by the job code that got ran and persisted by the "code runner".
	Output []byte `json:"output,omitempty"`
}
