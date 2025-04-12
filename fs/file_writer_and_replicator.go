package fs

type fileWriter struct {
	filenames            []string
	tempFilenames        []string
	tempNameSuffix       string
	replicate            bool
	replicateToFilenames []string
}

func newFileWriterAndReplicator(replicate bool) *fileWriter {
	return &fileWriter{
		tempNameSuffix: "tmp",
		replicate:      replicate,
	}
}

func (fw *fileWriter) writeToTemp(contents []byte, filename string, replicateToFilename string) error {
	return nil
}

func (fw *fileWriter) finalize() error {
	return nil
}
