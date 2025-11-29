package etl

import (
	"encoding/json"
	"fmt"
	"os"
)

// WorkflowStep represents a single step in the ETL pipeline.
type WorkflowStep struct {
	Name   string                 `json:"name"`
	Type   string                 `json:"type"` // "prepare" or "ingest"
	Params map[string]interface{} `json:"params"`
}

// Workflow represents the entire ETL pipeline configuration.
type Workflow struct {
	Steps []WorkflowStep `json:"steps"`
}

// RunWorkflow executes the steps defined in the workflow JSON file.
func RunWorkflow(workflowPath string) error {
	f, err := os.Open(workflowPath)
	if err != nil {
		return fmt.Errorf("failed to open workflow file: %w", err)
	}
	defer f.Close()

	var wf Workflow
	if err := json.NewDecoder(f).Decode(&wf); err != nil {
		return fmt.Errorf("failed to decode workflow JSON: %w", err)
	}

	fmt.Printf("Starting Workflow: %s\n", workflowPath)

	for i, step := range wf.Steps {
		fmt.Printf("\n[%d/%d] Step: %s (%s)\n", i+1, len(wf.Steps), step.Name, step.Type)

		switch step.Type {
		case "prepare":
			url, _ := step.Params["url"].(string)
			out, _ := step.Params["out"].(string)
			limitFloat, _ := step.Params["limit"].(float64)
			limit := int(limitFloat)

			if url == "" || out == "" {
				return fmt.Errorf("step '%s': missing required params 'url' or 'out'", step.Name)
			}

			if err := PrepareData(url, out, limit); err != nil {
				return fmt.Errorf("step '%s' failed: %w", step.Name, err)
			}

		case "ingest":
			config, _ := step.Params["config"].(string)
			data, _ := step.Params["data"].(string)
			agentID, _ := step.Params["agent"].(string)

			if config == "" {
				return fmt.Errorf("step '%s': missing required param 'config'", step.Name)
			}

			if err := IngestAgent(config, data, agentID); err != nil {
				return fmt.Errorf("step '%s' failed: %w", step.Name, err)
			}

		default:
			return fmt.Errorf("unknown step type: %s", step.Type)
		}
	}

	fmt.Println("\nWorkflow completed successfully.")
	return nil
}
