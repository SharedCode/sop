package restapi

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

// HTTPVerb enumerates supported HTTP operations.
type HTTPVerb int

const (
	// Unknown represents an unspecified HTTP verb.
	Unknown HTTPVerb = iota
	// GET lists or retrieves resources.
	GET
	// GET_ONE retrieves a single resource.
	GET_ONE
	// DELETE removes resources.
	DELETE
	// POST creates resources.
	POST
	// PUT replaces resources.
	PUT
	// PATCH partially updates resources.
	PATCH
)

// RestMethod describes a REST route handler with HTTP verb, path and handler function.
type RestMethod struct {
	Verb    HTTPVerb
	Path    string
	Handler func(c *gin.Context)
}

var restMethods = make(map[string]RestMethod)

// RegisterMethod builds a RestMethod and registers it in the package registry.
func RegisterMethod(verb HTTPVerb, path string, h func(c *gin.Context)) error {
	m := RestMethod{
		Verb:    verb,
		Path:    path,
		Handler: h,
	}
	return Register(m)
}

// Register inserts a RestMethod into the global registry preventing duplicates of verb+path.
func Register(m RestMethod) error {
	key := fmt.Sprintf("%d_%s", m.Verb, m.Path)
	if _, exists := restMethods[key]; exists {
		return fmt.Errorf("can't add %s, an existing handler in REST method map exists", key)
	}
	restMethods[key] = m
	return nil
}

// RestMethods returns a copy-like view of registered REST methods.
func RestMethods() map[string]RestMethod {
	return restMethods
}
