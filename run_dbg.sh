#!/bin/bash
sed -i '' -e 's/results, err := idx.Query(ctx, \[\]float32{0.5, 0.5}, 10, nil)/fmt.Printf("Before Query, tx has begun: %v\\n", tx.HasBegun())\n\tresults, err := idx.Query(ctx, []float32{0.5, 0.5}, 10, nil)/' ai/agent/e2e_active_memory_test.go
go test ./ai/agent -run TestEndToEndConsolidatorPipeline -v | grep Before
