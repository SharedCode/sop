package agent

import (
	"context"
)

const selectArgsSchema = `{"type":"object","properties":{"database":{"type":"string","description":"Optional database override. Defaults to the active session database."},"store":{"type":"string","description":"Target store name to read or mutate."},"key":{"type":"string","description":"Optional primitive key or JSON-encoded complex key used to constrain the selection."},"key_match":{"type":"string","description":"Optional alias of key for key-based matching."},"value":{"type":"object","description":"Optional value match object or explicit value payload."},"value_match":{"type":"object","description":"Optional alias of value used for value-based matching."},"filter":{"type":"object","description":"Optional filter object used as value-matching criteria."},"fields":{"type":"array","description":"Optional list of fields to project in the result.","items":{"type":"string"}},"limit":{"type":"number","description":"Maximum number of matching records to return or mutate."},"order_by":{"type":"string","description":"Optional order-by field or direction hint."},"direction":{"type":"string","enum":["asc","desc","ascending","descending"],"description":"Optional sort direction hint."},"action":{"type":"string","enum":["delete","update"],"description":"Optional mutation action applied to the matched rows."},"update_values":{"type":"object","description":"Fields to merge into matched records when action is update."}},"required":["store"]}`

const explainJoinArgsSchema = `{"type":"object","properties":{"database":{"type":"string","description":"Optional database override. Defaults to the active session database."},"right_store":{"type":"string","description":"Right-side store to analyze for the join."},"store":{"type":"string","description":"Legacy alias of right_store."},"on":{"type":"object","description":"Join mapping from left fields to right fields."},"left_join_fields":{"type":"array","description":"Optional left join fields when using the fallback join-style shape.","items":{"type":"string"}},"right_join_fields":{"type":"array","description":"Optional right join fields when using the fallback join-style shape.","items":{"type":"string"}}}}`

const addArgsSchema = `{"type":"object","properties":{"database":{"type":"string","description":"Optional database override. Defaults to the active session database."},"store":{"type":"string","description":"Target store name."},"key":{"type":"string","description":"Primitive key or JSON-encoded complex key string for the item to add."},"value":{"type":"object","description":"Value payload to add. Prefer an explicit object instead of relying on implicit field collection."}},"required":["store","key","value"]}`

const updateArgsSchema = `{"type":"object","properties":{"database":{"type":"string","description":"Optional database override. Defaults to the active session database."},"store":{"type":"string","description":"Target store name."},"key":{"type":"string","description":"Primitive key or JSON-encoded complex key string for the item to update."},"value":{"type":"object","description":"Replacement or update payload. Prefer an explicit object instead of relying on implicit field collection."}},"required":["store","key","value"]}`

const deleteArgsSchema = `{"type":"object","properties":{"database":{"type":"string","description":"Optional database override. Defaults to the active session database."},"store":{"type":"string","description":"Target store name."},"key":{"type":"string","description":"Primitive key or JSON-encoded complex key string for the item to delete."}},"required":["store","key"]}`

const manageTransactionArgsSchema = `{"type":"object","properties":{"database":{"type":"string","description":"Optional database override. Defaults to the active session database."},"action":{"type":"string","enum":["begin","commit","rollback"],"description":"Transaction action to execute."}},"required":["action"]}`

func (a *CopilotAgent) registerStoresTools(ctx context.Context) {
	a.registry.RegisterWithUI("select", "Selects data from a store using criteria. With action=delete or action=update, it mutates the selected records.", SelectInstruction, selectArgsSchema, a.toolSelect)

	a.registry.RegisterHidden("join", JoinInstruction, "(left_store: string, right_store: string, left_join_fields: Array<string>, right_join_fields: Array<string>, join_type?: \"inner\" | \"left\" | \"right\", fields?: Array<string>, limit?: number, direction?: \"asc\" | \"desc\", action?: \"delete_left\" | \"update_left\", update_values?: object)", a.toolJoin)
	a.registry.RegisterWithUI("explain_join", "Explains whether a join will use indexes or a full scan.", ExplainJoinInstruction, explainJoinArgsSchema, a.toolExplainJoin)

	a.registry.RegisterWithUI("add", "Adds data to a store.", AddInstruction, addArgsSchema, a.toolAdd)
	a.registry.RegisterWithUI("update", "Updates data in a store.", UpdateInstruction, updateArgsSchema, a.toolUpdate)
	a.registry.RegisterWithUI("delete", "Deletes data from a store by key.", DeleteInstruction, deleteArgsSchema, a.toolDelete)
	a.registry.RegisterWithUI("manage_transaction", "Manages transactions (begin, commit, rollback).", ManageTransactionInstruction, manageTransactionArgsSchema, a.toolManageTransaction)
}
