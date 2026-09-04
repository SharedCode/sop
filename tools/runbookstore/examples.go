package runbookstore

import "github.com/sharedcode/sop/ai/verify"

// DBMaintenanceWorkflow is the same scenario used throughout this repo's
// tests and docs: production database drop is forbidden without a validated
// backup, and rollback stays reachable even after the drop happens. Used by
// cmd/sop-mcp-server and cmd/sop-a2a-agent so both have something real to
// serve out of the box.
func DBMaintenanceWorkflow() (*verify.Workflow, error) {
	return verify.NewWorkflow(
		[]verify.Step{
			{ID: "take_backup", Establishes: []verify.State{"backup_taken"}},
			{ID: "validate_backup", Requires: []verify.State{"backup_taken"}, Establishes: []verify.State{"backup_validated"}},
			{ID: "drop_prod_db", Requires: []verify.State{"backup_validated"}, Establishes: []verify.State{"prod_db_dropped"}},
			{ID: "restore_from_backup", Requires: []verify.State{"backup_validated"}, Establishes: []verify.State{"rollback_complete"}},
			{ID: "restore_from_backup_post_drop", Requires: []verify.State{"prod_db_dropped"}, Establishes: []verify.State{"rollback_complete"}},
		},
		[]verify.SafetyRule{
			{Name: "no-drop-without-validated-backup", Forbidden: "prod_db_dropped", Requires: "backup_validated"},
		},
		[]verify.ReachabilityRule{
			{Name: "rollback-always-reachable", Target: "rollback_complete"},
		},
	)
}
