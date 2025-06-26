package main

/*
#include <stdlib.h> // For free
*/
import "C"
import "unsafe"

import "fmt"
import log "log/slog"
import "github.com/SharedCode/sop/redis"

//export free_string
func free_string(cString *C.char) {
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

//export close_redis_connection
func close_redis_connection() *C.char {
	err := redis.CloseConnection()
	if err != nil {
		errMsg := fmt.Sprintf("error encountered closing Redis connection, details: %v", err)
		log.Error(errMsg)

		// Remember to deallocate errMsg!
		return C.CString(errMsg)
	}
	return nil
}

func main() {
	// main function is required for building a shared library, but can be empty
}
