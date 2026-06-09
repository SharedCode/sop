package agent

import "github.com/sharedcode/sop/ai"

const (
	// SystemDBName is the name of the system database used for internal agent storage (scripts, history, etc).
	SystemDBName = "system"

	// DefaultHost is the default host for local AI providers.
	DefaultHost = "http://localhost:11434"

	// Environment Variables
	EnvOllamaHost = "OLLAMA_HOST"

	// Note: API keys are no longer read from environment variables.
	// All LLM configuration must be provided through the Config struct.

	// Providers
	ProviderGemini    = "gemini"
	ProviderChatGPT   = "chatgpt"
	ProviderAnthropic = "anthropic"
	ProviderOllama    = "ollama"
	ProviderLocal     = "local"

	// Default Models
	DefaultModelOpenAI    = ai.DefaultModelOpenAI
	DefaultModelGemini    = ai.DefaultModelGemini
	DefaultModelAnthropic = "claude-3-5-sonnet-20241022"
	DefaultModelOllama    = ai.DefaultModelOllama

	// Session Keys
	SessionPayloadKey = "session_payload"
	RunnerSessionKey  = "runner_session"
)
