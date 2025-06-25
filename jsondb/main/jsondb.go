package main

/*
#include <stdlib.h> // For free
*/
import "C"
import "unsafe"

import "fmt"
import log "log/slog"
import "github.com/SharedCode/sop/redis"

//export FreeString
func FreeString(cString *C.char) {
	if cString != nil {
		C.free(unsafe.Pointer(cString))
	}
}

//export open_redis_connection
func open_redis_connection(host *C.char, port C.int, password *C.char) *C.char {
	_, err := redis.OpenConnection(redis.Options{
		Address:  fmt.Sprintf("%s:%d", C.GoString(host), int(port)),
		DB:       0,
		Password: C.GoString(password),
	})
	if err != nil {
		errMsg := fmt.Sprintf("error encountered opening Redis connection, details: %v", err)
		log.Error(errMsg)

		// Remember to deallocate errInfo.message!
		return C.CString(errMsg)
	}
	return nil
}

func main() {
	// main function is required for building a shared library, but can be empty
}
