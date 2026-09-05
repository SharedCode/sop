package main

import "net/http"

// maxRequestBodySize caps the number of bytes any single request body can
// deliver before the server closes the connection. 10 MiB covers the JSON
// API payloads (typically < 1 KiB) and the knowledge-base file imports
// (handleImportSpace, handleIngestImportSpace) while still preventing a
// single client from pinning memory with an unbounded upload. Handlers
// that stream from r.Body (json.NewDecoder, io.ReadAll, r.FormFile) will
// see an error once this limit is exceeded, exactly the same as the
// per-handler MaxBytesReader pattern used in cmd/sop-daemon.
const maxRequestBodySize = 10 << 20 // 10 MiB

// bodySizeLimitMiddleware wraps every incoming request body with
// http.MaxBytesReader so no handler can be forced to buffer more than
// maxRequestBodySize bytes. This is applied once at the mux level rather
// than per-handler to avoid gaps.
func bodySizeLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
		next.ServeHTTP(w, r)
	})
}
