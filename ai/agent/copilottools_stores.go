package agent

import (
	"context"
)

const manageTransactionArgsSchema = `{"type":"object","properties":{"database":{"type":"string","description":"Optional database override. Defaults to the active session database."},"action":{"type":"string","enum":["begin","commit","rollback"],"description":"Transaction action to execute."}},"required":["action"]}`

func (a *CopilotAgent) registerStoresTools(ctx context.Context) {
	a.registry.RegisterWithUI("select", "Selects data from a store using criteria. With action=delete or action=update, it mutates the selected records.", SelectInstruction, "(store: string, key?: any, value?: any, fields?: Array<string>, limit?: number, direction?: \"asc\" | \"desc\", action?: \"delete\" | \"update\", update_values?: object)", a.toolSelect)

	a.registry.RegisterHidden("join", JoinInstruction, "(left_store: string, right_store: string, left_join_fields: Array<string>, right_join_fields: Array<string>, join_type?: \"inner\" | \"left\" | \"right\", fields?: Array<string>, limit?: number, direction?: \"asc\" | \"desc\", action?: \"delete_left\" | \"update_left\", update_values?: object)", a.toolJoin)
	a.registry.RegisterWithUI("explain_join", "Explains whether a join will use indexes or a full scan.", ExplainJoinInstruction, "(right_store: string, on: map, database?: string)", a.toolExplainJoin)

	a.registry.RegisterWithUI("add", "Adds data to a store.", AddInstruction, "(store: string, key: any, value: any)", a.toolAdd)
	a.registry.RegisterWithUI("update", "Updates data in a store.", UpdateInstruction, "(store: string, key: any, value: any)", a.toolUpdate)
	a.registry.RegisterWithUI("delete", "Deletes data from a store by key.", DeleteInstruction, "(store: string, key: any)", a.toolDelete)
	a.registry.RegisterWithUI("manage_transaction", "Manages transactions (begin, commit, rollback).", ManageTransactionInstruction, manageTransactionArgsSchema, a.toolManageTransaction)
}
