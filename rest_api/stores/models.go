package jobs

import (
	"time"
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
