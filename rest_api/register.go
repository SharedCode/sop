package rest_api

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

type HTTPVerb int
const(
	Unknown = iota
	GET
	GET_ONE
	DELETE
	POST
	PUT
	PATCH
)

type RestMethod struct {
	Verb HTTPVerb
	Path string
	Handler func(c *gin.Context)
}

var restMethods = make(map[string]RestMethod)

// RegisterMethod is a helper function for Register.
func RegisterMethod(verb HTTPVerb, path string, h func(c *gin.Context)) error {
	m := RestMethod{
		Verb: verb,
		Path: path,
		Handler: h,
	}
	return Register(m)
}

// Register your REST method using this function.
func Register(m RestMethod) error {
	key := fmt.Sprintf("%d_%s", m.Verb, m.Path)
	if _, exists := restMethods[key]; exists {
		return fmt.Errorf("can't add %s, an existing handler in REST method map exists", key)
	}
	restMethods[key] = m
	return nil
}

// Returns the registered REST Methods map.
func RestMethods() map[string]RestMethod {
	return restMethods
}
