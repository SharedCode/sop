// Package sop defines the core interfaces, types, and helpers used across the SOP codebase.
// It provides transactions, store options and metadata, key/handle abstractions, and shared
// error codes. Concrete backends live in subpackages such as fs (filesystem), cassandra,
// and redis, while higher-level features include B-Trees and streaming data helpers.
// It is designed to be extensible and modular, allowing for various storage backends
// to be implemented while sharing a common interface.
// This package is intended for internal use within the SOP project and is not meant for external use.
// It is a foundational package that other components build upon.
// It is not intended to be used directly by end-users, but rather serves as a base
// for more specific implementations and utilities in the SOP ecosystem.
// It is a foundational package that other components build upon.
// It is not intended to be used directly by end-users, but rather serves as a base
// for more specific implementations and utilities in the SOP ecosystem.
//
// See `inredfs package` for a concrete implementation of a File System-based store with built-in Redis caching.
package sop
