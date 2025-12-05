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
// See `infs.package` for a concrete implementation of a File System-based store with built-in Redis caching.
package sop

// Timeout model
//
// SOP operations (notably transaction commits) are bounded by two timers:
//  1. The caller-provided context deadline/cancellation which propagates across subsystems.
//  2. An operation-specific maximum duration (e.g., transaction maxTime) used for internal safety
//     limits and lock TTLs.
//
// The effective commit duration is the earlier of the context deadline and the transaction's maxTime.
// Locks use the transaction maxTime as their TTL so that locks are safely released even if the caller's
// context is canceled. If replication and cleanup should run within the caller's budget, prefer setting
// ctx.Deadline >= maxTime plus a small slack.
//
// Timeouts are normalized with ErrTimeout which wraps the underlying context error when applicable
// to preserve errors.Is(err, context.DeadlineExceeded) while providing consistent timeout detection.
