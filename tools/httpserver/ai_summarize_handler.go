package main

import (
	"encoding/json"
	"net/http"
)

func handleAISummarize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Text string `json:"text"`
		Max  int    `json:"max,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	summarizer := GetSummarizer(config.ProductionMode, GetConfiguredLLMClient(r))
	maxSummaries := req.Max
	if maxSummaries <= 0 {
		maxSummaries = 5 // default
	}

	summaries, err := summarizer.Summarize(ctx, req.Text, maxSummaries)
	if err != nil {
		// Return single chunk as fallback if model fails
		summaries = []string{req.Text}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"summaries": summaries,
	})
}
