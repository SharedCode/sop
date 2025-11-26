package agent

import (
	"fmt"
	"math"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/vector"
)

// StagedVectorIndex defines the interface for staged ingestion.
type StagedVectorIndex interface {
	ai.VectorIndex
	UpsertContent(items []ai.Item) error
	IterateAll(cb func(item ai.Item) error) error
	SeedCentroids(centroids map[int][]float32) error
	IndexAll() error
}

// IngestData seeds the vector database with data from the configuration.
// It performs a multi-phase ingestion process:
// 1. Deduplication & Content Ingestion: Embeds and stores unique items.
// 2. Centroid Computation: Samples data to compute K-Means centroids for IVFFlat indexing.
// 3. Indexing: Assigns vectors to centroids to optimize search performance.
func IngestData(cfg Config, idx ai.VectorIndex, emb ai.Embeddings) error {
	if len(cfg.Data) == 0 {
		return nil
	}

	stagedIdx, ok := idx.(StagedVectorIndex)
	if !ok {
		return fmt.Errorf("index does not support staged ingestion")
	}

	fmt.Printf("Starting ingestion of %d items...\n", len(cfg.Data))

	// Phase 1: Ingest Content (Deduplicated)
	fmt.Println("Phase 1: Ingesting Content (Deduplicated)...")

	processBatch := func(texts []string, originals []DataItem) error {
		vecs, err := emb.EmbedTexts(texts)
		if err != nil {
			return fmt.Errorf("embedding failed: %w", err)
		}

		var items []ai.Item
		for i, vec := range vecs {
			itm := originals[i]
			items = append(items, ai.Item{
				ID:     HashString(texts[i]),
				Vector: vec,
				Meta: map[string]any{
					"text":        itm.Text,
					"description": itm.Description,
					"original_id": itm.ID,
				},
			})
		}
		return stagedIdx.UpsertContent(items)
	}

	batchSize := 100
	var batchTexts []string
	var batchOriginals []DataItem

	seenInRun := make(map[string]bool)
	skippedCount := 0
	ingestedCount := 0

	for _, item := range cfg.Data {
		contentToEmbed := fmt.Sprintf("%s %s", item.Text, item.Description)
		id := HashString(contentToEmbed)

		if seenInRun[id] {
			skippedCount++
			continue
		}
		seenInRun[id] = true

		if !cfg.SkipDeduplication {
			if _, err := idx.Get(id); err == nil {
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
			ingestedCount += len(batchTexts)
			batchTexts = nil
			batchOriginals = nil
		}
	}

	if len(batchTexts) > 0 {
		if err := processBatch(batchTexts, batchOriginals); err != nil {
			return err
		}
		ingestedCount += len(batchTexts)
	}
	fmt.Printf("Content Ingestion complete. Ingested: %d, Skipped: %d.\n", ingestedCount, skippedCount)

	// Phase 2: Compute Centroids from Clean Data
	fmt.Println("Phase 2: Computing Centroids from Clean Data...")

	categorySamples := make(map[string][]ai.Item)
	maxSamplesPerCategory := 50
	totalItems := 0

	err := stagedIdx.IterateAll(func(item ai.Item) error {
		totalItems++
		text := ""
		if t, ok := item.Meta["text"].(string); ok {
			text = t
		}

		// We need the vector. UpsertContent stored it in TempVectors.
		// IterateAll now transparently fetches it from TempVectors if available.
		var vec []float32
		if len(item.Vector) > 0 {
			vec = item.Vector
		}

		if len(vec) > 0 {
			if len(categorySamples[text]) < maxSamplesPerCategory {
				categorySamples[text] = append(categorySamples[text], ai.Item{
					ID:     item.ID,
					Vector: vec,
				})
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to iterate content: %w", err)
	}

	// Flatten samples
	var sampledItems []ai.Item
	for _, items := range categorySamples {
		sampledItems = append(sampledItems, items...)
	}

	if len(sampledItems) > 0 {
		k := int(math.Sqrt(float64(totalItems)))
		if k < 1 {
			k = 1
		}
		if len(categorySamples) > k {
			k = len(categorySamples)
		}

		fmt.Printf("Computing centroids from %d samples (Total items: %d, K: %d)...\n", len(sampledItems), totalItems, k)
		centroids, err := vector.ComputeCentroids(sampledItems, k)
		if err != nil {
			return fmt.Errorf("failed to compute centroids: %w", err)
		}
		fmt.Printf("Computed %d centroids.\n", len(centroids))

		if err := stagedIdx.SeedCentroids(centroids); err != nil {
			return fmt.Errorf("failed to seed centroids: %w", err)
		}
	}

	// Phase 3: Index Vectors
	fmt.Println("Phase 3: Indexing Vectors...")
	if err := stagedIdx.IndexAll(); err != nil {
		return fmt.Errorf("failed to index vectors: %w", err)
	}

	fmt.Println("Ingestion complete.")
	return nil
}
