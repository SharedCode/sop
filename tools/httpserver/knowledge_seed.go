package main

// KnowledgeEntry defines the structure for seeding knowledge
type KnowledgeEntry struct {
	Category string
	Name     string
	Content  string
}

// UserDefinedKnowledge is a list of knowledge entries to be seeded into the system.
// Users can add their custom learnings here.
// When the server starts, these entries will be upserted into the llm_knowledge store.
var UserDefinedKnowledge = []KnowledgeEntry{
	// Example Entry:
	// {
	// 	Category: "workflow",
	// 	Name:     "user_registration",
	// 	Content:  "To register a user, first validate the email, then check if it exists in the 'users' table, and finally insert.",
	// },
	{
		Category: "data_generation",
		Name:     "uuid_strategy",
		Content:  "When creating new records for stores with UUID keys (users, products, orders, etc.), ALWAYS generate a new random UUID v4. Never use sequential integers or simple strings like 'id1'.",
	},
}
