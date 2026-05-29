package agent

import _ "embed"

//go:embed prompts/classify_discovery.md
var promptClassifyDiscovery string

//go:embed prompts/classify_focused.md
var promptClassifyFocused string

//go:embed prompts/classify_continuity.md
var promptClassifyContinuity string

//go:embed prompts/tools_scripts.md
var toolsScriptsManual string
