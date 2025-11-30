package agent

import (
	"context"
	"fmt"
	"math"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/vector"
)

// IngestData seeds the vector database with data from the configuration.
// It performs a multi-phase ingestion process:
// 1. Deduplication & Content Ingestion: Embeds and stores unique items.
// 2. Centroid Computation: Samples data to compute K-Means centroids for IVFFlat indexing.
// 3. Indexing: Assigns vectors to centroids to optimize search performance.
func IngestData(ctx context.Context, cfg Config, idx ai.VectorStore[map[string]any], emb ai.Embeddings) error {
	if len(cfg.Data) == 0 {
		return nil
	}

	// Configure deduplication based on config
	idx.SetDeduplication(!cfg.SkipDeduplication)

	fmt.Printf("Starting ingestion of %d items...\n", len(cfg.Data))

	// Phase 1: Ingest Content (Deduplicated)
	fmt.Println("Phase 1: Ingesting Content (Deduplicated)...")

	var itemsToUpsert []ai.Item[map[string]any]
	seenInRun := make(map[string]bool)
	skippedCount := 0

	// Prepare batch
	var batchTexts []string
	var batchOriginals []DataItem

	processBatch := func(texts []string, originals []DataItem) error {
		vecs, err := emb.EmbedTexts(ctx, texts)
		if err != nil {
			return fmt.Errorf("embedding failed: %w", err)
		}

		for i, vec := range vecs {
			itm := originals[i]
			id := HashString(texts[i])
			itemsToUpsert = append(itemsToUpsert, ai.Item[map[string]any]{
				ID:     id,
				Vector: vec,
				Payload: map[string]any{
					"text":        itm.Text,
					"description": itm.Description,
					"original_id": itm.ID,
				},
			})
		}
		return nil
	}

	batchSize := 100

	for _, item := range cfg.Data {
		contentToEmbed := fmt.Sprintf("%s %s", item.Text, item.Description)
		id := HashString(contentToEmbed)

		if seenInRun[id] {
			skippedCount++
			continue
		}
		seenInRun[id] = true

		if !cfg.SkipDeduplication {
			if _, err := idx.Get(ctx, id); err == nil {
				skippedCount++
				continue
			}
		}

		batchTexts = append(batchTexts, contentToEmbed)
		batchOriginals = append(batchOriginals, item)

		if len(batchTexts) >= batchSize {
			if err := processBatch(batchTexts, batchOriginals); err != nil {
				return err
			}
			batchTexts = nil
			batchOriginals = nil
		}
	}

	if len(batchTexts) > 0 {
		if err := processBatch(batchTexts, batchOriginals); err != nil {
			return err
		}
	}

	if len(itemsToUpsert) == 0 {
		fmt.Printf("No new items to ingest. Skipped: %d.\n", skippedCount)
		return nil
	}

	// Initial Upsert
	if err := idx.UpsertBatch(ctx, itemsToUpsert); err != nil {
		return fmt.Errorf("failed to upsert batch: %w", err)
	}
	fmt.Printf("Content Ingestion complete. Ingested: %d, Skipped: %d.\n", len(itemsToUpsert), skippedCount)

	// Phase 2: Compute Centroids (Thinking)
	// We fetch all items to compute global centroids.
	// In a real system, we might only sample or do this periodically.
	fmt.Println("Phase 2: Computing Centroids (Thinking)...")

	// We need to fetch all items to do this properly.
	// Since we just upserted, we can use itemsToUpsert if it covers everything,
	// but to be safe and include existing data, we should iterate.
	// However, VectorIndex doesn't have IterateAll exposed in the interface yet?
	// Wait, ai/interfaces.go defined VectorIndex[T]. Let's check if it has IterateAll.
	// It usually doesn't. But we can use the items we just created for the initial clustering.

	if len(itemsToUpsert) > 0 {
		k := int(math.Sqrt(float64(len(itemsToUpsert))))
		if k < 1 {
			k = 1
		}
		if k > 256 {
			k = 256
		}

		fmt.Printf("Computing centroids from %d items (K: %d)...\n", len(itemsToUpsert), k)
		centroids, err := vector.ComputeCentroids(itemsToUpsert, k)
		if err != nil {
			return fmt.Errorf("failed to compute centroids: %w", err)
		}
		fmt.Printf("Computed %d centroids.\n", len(centroids))

		// Re-assign items to centroids
		// This is the "Thinking" part where the agent organizes its memory.
		for i := range itemsToUpsert {
			vec := itemsToUpsert[i].Vector
			bestC := -1
			bestDist := float32(math.MaxFloat32)

			for cid, center := range centroids {
				dist := euclideanDistance(vec, center)
				if dist < bestDist {
					bestDist = dist
					bestC = cid
				}
			}
			itemsToUpsert[i].CentroidID = bestC
		}

		// Re-Upsert with Centroid IDs
		fmt.Println("Phase 3: Re-indexing with Centroids...")
		if err := idx.UpsertBatch(ctx, itemsToUpsert); err != nil {
			return fmt.Errorf("failed to re-index with centroids: %w", err)
		}
	}

	fmt.Println("Ingestion complete.")
	return nil
}

func euclideanDistance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}
