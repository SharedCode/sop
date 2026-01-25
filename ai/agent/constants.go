package agent

const (
	// SystemDBName is the name of the system database used for internal agent storage (scripts, history, etc).
	SystemDBName = "system"

	// DefaultHost is the default host for local AI providers.
	DefaultHost = "http://localhost:11434"

	// Environment Variables
	EnvAIProvider   = "AI_PROVIDER"
	EnvGeminiAPIKey = "GEMINI_API_KEY"
	EnvOpenAIAPIKey = "OPENAI_API_KEY"
	EnvLLMAPIKey    = "LLM_API_KEY"
	EnvOpenAIModel  = "OPENAI_MODEL"
	EnvGeminiModel  = "GEMINI_MODEL"
	EnvOllamaModel  = "OLLAMA_MODEL"
	EnvOllamaHost   = "OLLAMA_HOST"

	// Providers
	ProviderGemini  = "gemini"
	ProviderChatGPT = "chatgpt"
	ProviderOllama  = "ollama"
	ProviderLocal   = "local"

	// Default Models
	DefaultModelOpenAI = "gpt-4o"
	DefaultModelGemini = "gemini-2.5-pro"
	DefaultModelOllama = "llama3"

	// Session Keys
	SessionPayloadKey = "session_payload"
)
