package main

import (
	"regexp"
	"strings"
)

var prefixRegex = regexp.MustCompile(`^(\d+(\.\d+)*[\.\)]?|[A-Za-z][\.\)]|\b[IVXLCDMivxlcdm]+[\.\)])(\s+|$)`)
var headingPrefixRegex = regexp.MustCompile(`(?m)^\s*#+\s*(?:\d+(?:\.\d+)*[\.\)]?|[A-Za-z][\.\)]|\b[IVXLCDMivxlcdm]+[\.\)])?\s*`)

func removePrefix(s string, prefix string) string {
	if prefix == "" {
		return s
	}
	if strings.HasPrefix(s, prefix) {
		s, _ = strings.CutPrefix(s, prefix)
	}
	return s
}

func cleanText(s string) string {
	s = headingPrefixRegex.ReplaceAllString(s, "")
	s = prefixRegex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func normalizeCategoryName(s string) string {
	s = strings.TrimLeft(s, "# ")
	return strings.TrimSpace(prefixRegex.ReplaceAllString(s, ""))
}
