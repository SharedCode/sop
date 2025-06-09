package fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/SharedCode/sop/encoding"
)

func (sr *StoreRepository) CopyToPassiveFolders(ctx context.Context) error {
	// Copy StoreRepositories to passive targets.
	// Copy Registries to passive targets.

	if storeList, err := sr.GetAll(ctx); err != nil {
		return err
	} else {

		oaf := sr.replicationTracker.replicationTrackedDetails.ActiveFolderToggler
		sr.replicationTracker.replicationTrackedDetails.ActiveFolderToggler = !oaf
		storeWriter := newFileIOWithReplication(sr.replicationTracker, sr.manageStore, false)

		// Restore the active folder toggler value upon out of scope.
		defer func() {
			sr.replicationTracker.replicationTrackedDetails.ActiveFolderToggler = oaf
		}()

		// Write the store list.
		ba, _ := encoding.Marshal(storeList)
		if err := storeWriter.createStore(""); err != nil {
			return err
		}
		if err := storeWriter.write(storeListFilename, ba); err != nil {
			return err
		}

		for _, storeName := range storeList {

			// Create the Store folder.
			if err := storeWriter.createStore(storeName); err != nil {
				return err
			}

			store, err := sr.Get(ctx, storeName)
			if err != nil {
				return err
			}
			// Write the store info.
			ba, err := encoding.Marshal(store[0])
			if err != nil {
				return err
			}
			if err := storeWriter.write(fmt.Sprintf("%c%s%c%s", os.PathSeparator, storeName, os.PathSeparator, storeInfoFilename), ba); err != nil {
				return err
			}

			// Copy this store's registry segment file(s).
			sf := sr.replicationTracker.formatPassiveFolderEntity(storeName)
			tf := sr.replicationTracker.formatActiveFolderEntity(storeName)
			if err := copyFilesByExtension(sf, tf, registryFileExtension); err != nil {
				return err
			}
		}
	}

	return nil
}

func copyFilesByExtension(sourceDir, targetDir, extension string) error {
	files, err := os.ReadDir(sourceDir)
	if err != nil {
		return fmt.Errorf("error reading source directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue // Skip directories
		}

		if strings.HasSuffix(file.Name(), extension) {
			sourcePath := filepath.Join(sourceDir, file.Name())
			targetPath := filepath.Join(targetDir, file.Name())

			if err := copyFile(sourcePath, targetPath); err != nil {
				return fmt.Errorf("error copying file %s: %w", file.Name(), err)
			}
		}
	}

	return nil
}

func copyFile(sourcePath, targetPath string) error {
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("error opening source file: %w", err)
	}
	defer sourceFile.Close()

	targetFile, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("error creating target file: %w", err)
	}
	defer targetFile.Close()

	_, err = io.Copy(targetFile, sourceFile)
	if err != nil {
		return fmt.Errorf("error copying file content: %w", err)
	}

	return nil
}
