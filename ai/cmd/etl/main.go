package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/sharedcode/sop/ai/etl"
)

func main() {
	workflowPath := flag.String("workflow", "", "Path to the ETL workflow JSON file")
	configPath := flag.String("config", "", "Path to the agent configuration JSON file (Legacy mode)")
	dataFile := flag.String("data", "", "Path to the data file (Legacy mode)")
	targetAgentID := flag.String("agent", "", "Optional: ID of the specific agent (Legacy mode)")
	flag.Parse()

	if *workflowPath != "" {
		if err := etl.RunWorkflow(*workflowPath); err != nil {
			fmt.Printf("Workflow failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *configPath != "" {
		if err := etl.IngestAgent(*configPath, *dataFile, *targetAgentID); err != nil {
			fmt.Printf("ETL failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Println("Usage:")
	fmt.Println("  Run Workflow: go run ai/cmd/etl/main.go -workflow <path_to_workflow.json>")
	fmt.Println("  Run Single:   go run ai/cmd/etl/main.go -config <path_to_config.json> [-data <path_to_data.json>] [-agent <agent_id>]")
	os.Exit(1)
}
