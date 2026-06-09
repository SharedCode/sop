//go:build local_embedder

package embed

import "github.com/kelindar/search"

func init() {
	RegisterLocalEmbedder("kelindar", func(modelPath string, gpuLayers int) (localVectorizer, error) {
		resolvedPath, err := ensureLocalModelPath(modelPath)
		if err != nil {
			return nil, err
		}

		_, rollbackEnv := setupLocalHardwareThreads()
		defer rollbackEnv()

		return search.NewVectorizer(resolvedPath, gpuLayers)
	})
}
