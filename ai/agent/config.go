package agent

import (
	"encoding/json"
	"os"
)

// Config defines the structure of the JSON configuration file for an agent.
type Config struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	Description       string            `json:"description"`
	Synonyms          map[string]string `json:"synonyms"`
	SystemPrompt      string            `json:"system_prompt"`
	Policies          []PolicyConfig    `json:"policies"`
	Embedder          EmbedderConfig    `json:"embedder,omitempty"`           // Configuration for the embedder
	Generator         GeneratorConfig   `json:"generator,omitempty"`          // Configuration for the LLM generator
	Data              []DataItem        `json:"data"`                         // For seeding (MVP)
	StoragePath       string            `json:"storage_path,omitempty"`       // Optional: Override default storage path. Will be converted to absolute path.
	SkipDeduplication bool              `json:"skip_deduplication,omitempty"` // Optional: Skip deduplication phase
	Agents            []Config          `json:"agents,omitempty"`             // Optional: Define agents locally to be referenced by ID
	Pipeline          []PipelineStep    `json:"pipeline,omitempty"`           // Optional: Define a chain of agents
}

type PipelineStep struct {
	Agent    PipelineAgent `json:"agent"`
	OutputTo string        `json:"output_to,omitempty"` // "context", "next_step" (default)
}

type PipelineAgent struct {
	ID     string
	Config *Config
}

func (pa *PipelineAgent) UnmarshalJSON(data []byte) error {
	// Try string
	var id string
	if err := json.Unmarshal(data, &id); err == nil {
		pa.ID = id
		return nil
	}

	// Try config object
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err == nil {
		pa.Config = &cfg
		pa.ID = cfg.ID
		return nil
	}

	return nil // Or error if strict
}

func (pa PipelineAgent) MarshalJSON() ([]byte, error) {
	if pa.Config != nil {
		return json.Marshal(pa.Config)
	}
	return json.Marshal(pa.ID)
}

type GeneratorConfig struct {
	Type    string         `json:"type"`              // "ollama", "gemini", "chatgpt"
	Options map[string]any `json:"options,omitempty"` // Generator-specific options (model, api_key, etc.)
}

type EmbedderConfig struct {
	Type        string         `json:"type"`              // "simple" (default), "agent", or "ollama"
	AgentID     string         `json:"agent_id"`          // For "agent" type: ID of the agent to use
	Instruction string         `json:"instruction"`       // For "agent" type: Instruction for the agent
	Options     map[string]any `json:"options,omitempty"` // For "ollama" type: model, base_url
}

type PolicyConfig struct {
	ID         string `json:"id,omitempty"` // Optional: ID for referencing in pipeline
	Type       string `json:"type"`         // e.g. "profanity"
	MaxStrikes int    `json:"max_strikes"`  // e.g. 3
}

type DataItem struct {
	ID          string `json:"id"`
	Text        string `json:"text"`
	Description string `json:"description"`
}

// LoadConfigFromFile reads and parses a JSON configuration file.
func LoadConfigFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
