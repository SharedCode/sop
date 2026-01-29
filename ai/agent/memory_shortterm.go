package agent

import (
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

// RunnerSession holds the state for the current agent execution session,
// including script drafting and transaction management.
// This represents the Short-Term / Working Memory of the Agent.
type RunnerSession struct {
	Playback              bool // True if a script is currently being executed
	AutoSave              bool // If true, the draft is saved to DB after every step
	CurrentScript         *ai.Script
	CurrentScriptName     string // Name of the script being drafted
	CurrentScriptCategory string // Category for the script being drafted
	Transaction           sop.Transaction
	CurrentDB             string         // The database the transaction is bound to
	Variables             map[string]any // Session-scoped variables (e.g. cached stores)
	LastStep              *ai.ScriptStep
	// LastInteractionSteps tracks the number of steps added/executed in the last user interaction.
	LastInteractionSteps int
	// LastInteractionToolCalls buffers the tool calls from the last interaction for refactoring.
	LastInteractionToolCalls []ai.ScriptStep

	// PendingRefinement holds the proposed changes for a script from /script refine
	PendingRefinement *RefinementProposal

	// Memory holds the structured Short-Term Memory of the session.
	// It replaces the flat History slice with threaded topics.
	Memory *ShortTermMemory
}

// ConversationRole enum
type ConversationRole string

const (
	RoleUser      ConversationRole = "user"
	RoleAssistant ConversationRole = "assistant"
	RoleSystem    ConversationRole = "system"
)

// Interaction represents a single message in the conversation.
type Interaction struct {
	Role      ConversationRole `json:"role"`
	Content   string           `json:"content"`
	Timestamp int64            `json:"timestamp"`
}

// ConversationThread represents a single conversational thread.
// It starts with a root prompt and spins up a Q&A exchange, eventually leading to a conclusion.
type ConversationThread struct {
	ID         sop.UUID `json:"id"`
	RootPrompt string   `json:"root_prompt"` // The seed sentence that started this thread

	// Transcribed Context (Managed by LLM)
	Label        string `json:"label"`         // Short Topic Name (e.g. "Defining Client")
	Category     string `json:"category"`      // e.g. "Business Logic", "Clarification"
	ContextNotes string `json:"context_notes"` // Important notes/context for the LLM

	// The linear exchange of Q&A within this topic
	Exchanges []Interaction `json:"exchanges"`

	// Termination
	Conclusion string `json:"conclusion"` // Summary or Agreement
	Status     string `json:"status"`     // "active", "concluded"
}

// ShortTermMemory manages the history of conversation threads.
type ShortTermMemory struct {
	Threads         map[sop.UUID]*ConversationThread
	Order           []sop.UUID // Maintains the sequence of threads
	CurrentThreadID sop.UUID   // The currently active thread
}

// NewShortTermMemory initializes the memory structure.
func NewShortTermMemory() *ShortTermMemory {
	return &ShortTermMemory{
		Threads: make(map[sop.UUID]*ConversationThread),
		Order:   make([]sop.UUID, 0),
	}
}

const MaxConversationThreads = 20

// AddThread adds a new thread to memory, enforcing the LRU limit.
func (stm *ShortTermMemory) AddThread(thread *ConversationThread) {
	if len(stm.Order) >= MaxConversationThreads {
		// Remove the pending/oldest thread (index 0)
		oldestID := stm.Order[0]
		stm.Order = stm.Order[1:]
		delete(stm.Threads, oldestID)
	}
	stm.Threads[thread.ID] = thread
	stm.Order = append(stm.Order, thread.ID)
	stm.CurrentThreadID = thread.ID
}

// PromoteThread moves the specified thread ID to the end of the order (most recent).
func (stm *ShortTermMemory) PromoteThread(id sop.UUID) {
	// Find index
	idx := -1
	for i, existingID := range stm.Order {
		if existingID == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}
	// Remove from current position
	stm.Order = append(stm.Order[:idx], stm.Order[idx+1:]...)
	// Append to end
	stm.Order = append(stm.Order, id)
	stm.CurrentThreadID = id
}

// GetCurrentThread returns the active conversation thread or nil.
func (stm *ShortTermMemory) GetCurrentThread() *ConversationThread {
	if len(stm.CurrentThreadID) == 0 {
		return nil
	}
	return stm.Threads[stm.CurrentThreadID]
}

// RefinementProposal holds the proposed changes for a script.
type RefinementProposal struct {
	ScriptName     string
	Category       string
	OriginalScript ai.Script
	NewScript      ai.Script
	Description    string   // The new summary description
	NewParams      []string // List of new parameters
	Replacements   []string // Human readable list of replacements
}

// NewRunnerSession creates a new runner session.
func NewRunnerSession() *RunnerSession {
	return &RunnerSession{
		Memory: NewShortTermMemory(),
	}
}
