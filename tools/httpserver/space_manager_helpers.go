package main

import (
	"encoding/json"
)

func parseIngestPayload(data []byte) ([]ingestChunk, error) {
	var env struct {
		Items []ingestChunk `json:"items"`
	}
	if err := json.Unmarshal(data, &env); err == nil && len(env.Items) > 0 {
		return env.Items, nil
	}
	var chunks []ingestChunk
	err := json.Unmarshal(data, &chunks)
	return chunks, err
}

func getVectorsToUse(chunk ingestChunk) [][]float32 {
	if len(chunk.SummariesVectors) > 0 {
		return chunk.SummariesVectors
	}
	return chunk.Vectors
}

func buildChunkData(cid string, chunk ingestChunk) map[string]any {
	if chunk.Data != nil && len(chunk.Data) > 0 {
		if _, exists := chunk.Data["original_id"]; !exists && cid != "" {
			chunk.Data["original_id"] = cid
		}
		return chunk.Data
	}
	return map[string]any{
		"text":        chunk.Text,
		"description": chunk.Description,
		"category":    chunk.Category,
		"original_id": cid,
	}
}
