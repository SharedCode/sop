// Copyright 2023 The OpenAI Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/sharedcode/sop/ai"
)

// openAIGenerator is a minimal OpenAI-compatible generator used by tests and examples.
type openAIGenerator struct {
	client *http.Client
}

func NewOpenAIGenerator(client *http.Client) *openAIGenerator {
	if client == nil {
		client = http.DefaultClient
	}
	return &openAIGenerator{client: client}
}

func (g *openAIGenerator) Name() string { return "openai" }

func (g *openAIGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	payload := map[string]any{
		"model": "gpt-4o-mini",
		"input": prompt,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/responses", bytes.NewBuffer(body))
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(req)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("openai request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return ai.GenOutput{}, fmt.Errorf("openai api returned status code %d: %s", resp.StatusCode, string(respBody))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to read response: %w", err)
	}

	return ai.GenOutput{Text: string(respBody)}, nil
}

func (g *openAIGenerator) EstimateCost(inTokens, outTokens int) float64 {
	return 0
}

func (g *openAIGenerator) PrewarmCache(ctx context.Context, opts ai.GenOptions) error {
	return nil
}
