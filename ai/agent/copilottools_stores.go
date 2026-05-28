package agent

import (
	"context"
)

func (a *CopilotAgent) registerStoresTools(ctx context.Context) {
	a.registry.RegisterWithUI("select", "Selects data from a store using criteria.", SelectInstruction, "(store: string, key?: any, value?: any, fields?: Array<string>, limit?: number, direction?: \"asc\" | \"desc\", action?: \"delete\" | \"update\", update_values?: object)", a.toolSelect)

	a.registry.RegisterHidden("join", JoinInstruction, "(left_store: string, right_store: string, left_join_fields: Array<string>, right_join_fields: Array<string>, join_type?: \"inner\" | \"left\" | \"right\", fields?: Array<string>, limit?: number, direction?: \"asc\" | \"desc\", action?: \"delete_left\" | \"update_left\", update_values?: object)", a.toolJoin)
	a.registry.Register("explain_join", "Predicts the execution strategy (Index Scan vs Full Scan) for a join operation. Useful for performance debugging.", "(right_store: string, on: map, database?: string)", a.toolExplainJoin)

	a.registry.RegisterWithUI("add", "Adds data to a store.", AddInstruction, "(store: string, key: any, value: any)", a.toolAdd)
	a.registry.RegisterWithUI("update", "Updates data in a store.", UpdateInstruction, "(store: string, key: any, value: any)", a.toolUpdate)
	a.registry.RegisterWithUI("delete", "Deletes data from a store.", DeleteInstruction, "(store: string, key: any)", a.toolDelete)
	a.registry.RegisterWithUI("manage_transaction", "Manages transactions (begin, commit, rollback).", ManageTransactionInstruction, "(action: \"begin\" | \"commit\" | \"rollback\")", a.toolManageTransaction)
}
