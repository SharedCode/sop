package sop

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var versionFile string

// Version is the current version of the SOP library/application.
var Version = strings.TrimSpace(versionFile)
