package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/sharedcode/sop/ai/internal/adapter/generator"
	"github.com/sharedcode/sop/ai/internal/adapter/storage"
	"github.com/sharedcode/sop/ai/internal/domain/vertical"
	"github.com/sharedcode/sop/ai/internal/embed"
	"github.com/sharedcode/sop/ai/internal/functions"
	"github.com/sharedcode/sop/ai/internal/index"
	"github.com/sharedcode/sop/ai/internal/obs"
	"github.com/sharedcode/sop/ai/internal/obs/stdout"
	"github.com/sharedcode/sop/ai/internal/policy"
	"github.com/sharedcode/sop/ai/internal/port"
)

type Config struct {
	Storage struct {
		Driver string
		Params map[string]any
	}
	Generator struct {
		Driver string
		Params map[string]any
	}
}

func loadConfig() Config {
	var c Config
	// Use SOP storage by default as requested
	c.Storage.Driver = "sop"
	c.Storage.Params = map[string]any{}

	// Use Gemini 3 by default as requested
	c.Generator.Driver = "gemini"
	c.Generator.Params = map[string]any{
		"model":   "gemini-pro", // Assuming this maps to Gemini 3 in our adapter
		"api_key": "YOUR_API_KEY",
	}
	return c
}

func initObs() { obs.Init(&stdout.Simple{}, obs.NoopMeter{}, obs.NoopTracer{}) }

func initFunctions(store port.KVStore, dom port.Domain) {
	functions.Register("storage.integrity_check", func(cfg map[string]any) (port.DomainFunction, error) {
		return functions.NewIntegrity(policy.NewAllow("integrity-fn"), store), nil
	})
	// Future: register additional functions (embedding, search, generate)
}

func main() {
	cfg := loadConfig()
	initObs()

	// Initialize Storage
	store, err := storage.Open(cfg.Storage.Driver, cfg.Storage.Params)
	if err != nil {
		obs.Log().Error("store.open.error", "err", err)
		// Fallback to flat if SOP fails (e.g. no Redis)
		obs.Log().Info("falling back to flat storage")
		store, err = storage.Open("flat", map[string]any{"root": "./data/registry"})
		if err != nil {
			panic(err)
		}
	}
	defer store.Close()

	// Initialize Generators
	genGemini, err := generator.New("gemini", map[string]any{"model": "gemini-pro"})
	if err != nil {
		obs.Log().Error("generator.gemini.error", "err", err)
		return
	}
	genGPT, err := generator.New("chatgpt", map[string]any{"model": "gpt-4"})
	if err != nil {
		obs.Log().Error("generator.chatgpt.error", "err", err)
		return
	}
	genLocal, err := generator.New("local-expert", nil)
	if err != nil {
		obs.Log().Error("generator.local-expert.error", "err", err)
		return
	}

	// Build verticals (domains) with ISOLATED resources
	emb := embed.NewSimple("simple-emb", 64)

	// Define Safety Policies
	// 1. Global Policy: No Profanity
	profanityClassifier := policy.NewRegexClassifier("profanity-filter", map[string]string{
		"profanity": "(?i)badword|ugly",
	})
	globalSafety := policy.NewThresholdPolicy("global-safety", 0.5, []string{"profanity"})

	// 2. Local Policy (Vertical A): No Competitor Mentions
	competitorClassifier := policy.NewRegexClassifier("competitor-filter", map[string]string{
		"competitor": "(?i)redis|mongo",
	})
	localCompliance := policy.NewThresholdPolicy("local-compliance", 0.5, []string{"competitor"})

	// Vertical 1: General Knowledge (Gemini + Index A + Global & Local Policy)
	// We chain the policies: Global -> Local
	chainPolicy := policy.NewChain(globalSafety, localCompliance)
	idxA := index.NewMemory()
	domA := vertical.New("general-knowledge", emb, idxA, chainPolicy, genGemini, profanityClassifier, competitorClassifier)
	domA.SetPrompt("greet", "Hello, I am your AI assistant powered by {{engine}}.")

	// Vertical 2: Coding Assistant (ChatGPT + Index B + Global Policy Only)
	idxB := index.NewMemory()
	domB := vertical.New("coding-assistant", emb, idxB, globalSafety, genGPT, profanityClassifier)

	// Vertical 3: Sentiment Analysis (Local Perceptron + No Index + No Policy)
	// This demonstrates a purely local, lightweight vertical.
	idxC := index.NewMemory()
	domC := vertical.New("sentiment-bot", emb, idxC, policy.NewAllow("allow-all"), genLocal)

	initFunctions(store, domA)

	// Demonstrate usage
	fn := functions.Get("storage.integrity_check")
	out, _ := fn.Invoke(map[string]any{"key": "sector-0001"})
	obs.Log().Info("integrity.result", "out", out)

	// Demonstrate Vertical Process (RAG) - Vertical A
	// Add data ONLY to Index A
	vecA, _ := emb.EmbedTexts([]string{"SOP is a Scalable Object Persistence system."})
	if len(vecA) > 0 {
		idxA.Upsert("doc1", vecA[0], map[string]any{"text": "SOP is a Scalable Object Persistence system."})
	}

	// Add data ONLY to Index B
	vecB, _ := emb.EmbedTexts([]string{"Go is a statically typed, compiled programming language."})
	if len(vecB) > 0 {
		idxB.Upsert("doc2", vecB[0], map[string]any{"text": "Go is a statically typed, compiled programming language."})
	}

	// Query Vertical A (Should find SOP info, use Gemini)
	ragOutA, err := domA.Process(context.Background(), "What is SOP?")
	if err == nil {
		obs.Log().Info("vertical.A.output", "text", ragOutA)
	}

	// Query Vertical A with BAD WORD (Should be BLOCKED by Global Policy)
	_, err = domA.Process(context.Background(), "You are a badword!")
	if err != nil {
		obs.Log().Info("vertical.A.global_safety", "status", "blocked", "err", err)
	} else {
		obs.Log().Error("vertical.A.global_safety", "status", "failed_to_block")
	}

	// Query Vertical A with COMPETITOR (Should be BLOCKED by Local Policy)
	_, err = domA.Process(context.Background(), "Is Redis better than SOP?")
	if err != nil {
		obs.Log().Info("vertical.A.local_compliance", "status", "blocked", "err", err)
	} else {
		obs.Log().Error("vertical.A.local_compliance", "status", "failed_to_block")
	}

	// Query Vertical B with COMPETITOR (Should be ALLOWED - No Local Policy)
	ragOutBComp, err := domB.Process(context.Background(), "Is Redis better than SOP?")
	if err == nil {
		obs.Log().Info("vertical.B.competitor_check", "status", "allowed", "text", ragOutBComp)
	} else {
		obs.Log().Error("vertical.B.competitor_check", "status", "blocked_unexpectedly", "err", err)
	}

	// Query Vertical C (Local Perceptron)
	// It doesn't use RAG (Index is empty), just the internal weights.
	outC, err := domC.Process(context.Background(), "SOP is a fast database")
	if err == nil {
		obs.Log().Info("vertical.C.output", "text", outC)
	}

	// Vertical 4: Doctor/Diagnosis (Gemini + Medical Index)
	idxDoctor := index.NewMemory()
	domDoctor := vertical.New("doctor-diagnosis", emb, idxDoctor, policy.NewAllow("allow-all"), genGemini)

	// Custom Prompt for Diagnosis
	domDoctor.SetPrompt("rag", `You are an expert medical diagnostician. 
Based on the following medical knowledge (Context) and the patient's reported symptoms, provide a differential diagnosis.
Start with the most likely condition. If the symptoms are too vague, ask clarifying questions.

Medical Knowledge:
{{context}}

Patient Symptoms: {{query}}

Diagnosis:`)

	// Populate Medical Knowledge Base (Simulated "Mayo Clinic" Data)
	medicalData := []string{
		"Common Cold: Symptoms include runny nose, sneezing, sore throat, mild cough, and low-grade fever.",
		"Influenza (Flu): Symptoms include sudden high fever, chills, severe body aches, headache, fatigue, and dry cough.",
		"Migraine: Symptoms include severe throbbing headache (often on one side), sensitivity to light/sound, nausea, and visual disturbances.",
		"Tension Headache: Symptoms include dull, aching head pain, sensation of tightness or pressure across the forehead or on the sides and back of the head.",
		"COVID-19: Symptoms include fever, dry cough, shortness of breath, loss of taste or smell, fatigue, and body aches.",
		"Strep Throat: Symptoms include sudden sore throat, pain when swallowing, fever, red and swollen tonsils, and swollen lymph nodes.",
	}

	for i, data := range medicalData {
		vec, _ := emb.EmbedTexts([]string{data})
		if len(vec) > 0 {
			idxDoctor.Upsert(fmt.Sprintf("med-%d", i), vec[0], map[string]any{"text": data})
		}
	}

	// Simulate a "Narrowing" Diagnosis Session
	fmt.Println("\n--- Starting Diagnosis Session ---")

	diagnose := func(v *vertical.Vertical, symptoms []string) {
		// Combine symptoms for the query to "narrow" the search
		query := strings.Join(symptoms, " ")
		fmt.Printf("\nPatient: %s\n", query)

		out, err := v.Process(context.Background(), query)
		if err != nil {
			fmt.Printf("Doctor: Error - %v\n", err)
			return
		}
		fmt.Printf("Doctor: %s\n", out)
	}

	// Case 1: Headache -> Migraine
	symptoms1 := []string{"I have a severe headache."}
	diagnose(domDoctor, symptoms1)

	symptoms1 = append(symptoms1, "I also feel nauseous and light hurts my eyes.")
	diagnose(domDoctor, symptoms1)

	// Case 2: Fever -> Flu
	symptoms2 := []string{"I have a sudden high fever and body aches."}
	diagnose(domDoctor, symptoms2)

	// Query Vertical B (Should find Go info, use ChatGPT)
	ragOutB, err := domB.Process(context.Background(), "What is Go?")
	if err == nil {
		obs.Log().Info("vertical.B.output", "text", ragOutB)
	} // Cross-Contamination Check: Query Vertical A for Go info (Should NOT find it)
	ragOutCross, err := domA.Process(context.Background(), "What is Go?")
	if err == nil {
		obs.Log().Info("vertical.A.cross_check", "text", ragOutCross)
	}

	fmt.Println("AI storage integrity vertical ready.")
}
