//go:build !ignore_test_helpers

package main

/*
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Wrappers to allow testing C-exported functions from Go tests without cgo in test files

func CreateContextForTest() int64 {
	return int64(createContext())
}

func CancelContextForTest(id int64) {
	cancelContext(C.longlong(id))
}

func RemoveContextForTest(id int64) {
	removeContext(C.longlong(id))
}

func GetContextErrorForTest(id int64) string {
	cStr := contextError(C.longlong(id))
	if cStr == nil {
		return ""
	}
	// We need to free the string if the original function allocated it?
	// contextError returns C.CString, so yes.
	// But wait, contextError implementation:
	// return C.CString(ctx.Err().Error())
	// So the caller owns it.
	defer C.free(unsafe.Pointer(cStr))
	return C.GoString(cStr)
}

func ContextErrorReturnsNilForTest(id int64) bool {
	cStr := contextError(C.longlong(id))
	if cStr == nil {
		return true
	}
	C.free(unsafe.Pointer(cStr))
	return false
}

func ManageTransactionForTest(ctxID int64, action int, payload string) string {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))

	res := manageTransaction(C.longlong(ctxID), C.int(action), cPayload)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func ManageDatabaseForTest(ctxID int64, action int, targetID string, payload string) string {
	cTargetID := C.CString(targetID)
	defer C.free(unsafe.Pointer(cTargetID))
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))

	res := manageDatabase(C.longlong(ctxID), C.int(action), cTargetID, cPayload)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func ManageBtreeForTest(ctxID int64, action int, payload string, payload2 string) string {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	cPayload2 := C.CString(payload2)
	defer C.free(unsafe.Pointer(cPayload2))

	res := manageBtree(C.longlong(ctxID), C.int(action), cPayload, cPayload2)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func NavigateBtreeForTest(ctxID int64, action int, payload string, payload2 string) string {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	cPayload2 := C.CString(payload2)
	defer C.free(unsafe.Pointer(cPayload2))

	res := navigateBtree(C.longlong(ctxID), C.int(action), cPayload, cPayload2)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func GetFromBtreeForTest(ctxID int64, action int, payload string, payload2 string) (string, string) {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	cPayload2 := C.CString(payload2)
	defer C.free(unsafe.Pointer(cPayload2))

	res, errStr := getFromBtree(C.longlong(ctxID), C.int(action), cPayload, cPayload2)

	r := ""
	if res != nil {
		r = C.GoString(res)
		C.free(unsafe.Pointer(res))
	}
	e := ""
	if errStr != nil {
		e = C.GoString(errStr)
		C.free(unsafe.Pointer(errStr))
	}
	return r, e
}

func GetBtreeItemCountForTest(payload string) (int64, string) {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))

	count, errStr := getBtreeItemCount(cPayload)

	e := ""
	if errStr != nil {
		e = C.GoString(errStr)
		C.free(unsafe.Pointer(errStr))
	}
	return int64(count), e
}

func GetStoreInfoForTest(payload string) (string, string) {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))

	res, errStr := getStoreInfo(cPayload)

	r := ""
	if res != nil {
		r = C.GoString(res)
		C.free(unsafe.Pointer(res))
	}
	e := ""
	if errStr != nil {
		e = C.GoString(errStr)
		C.free(unsafe.Pointer(errStr))
	}
	return r, e
}

func ManageSearchForTest(ctxID int64, action int, payload string, payload2 string) string {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	cPayload2 := C.CString(payload2)
	defer C.free(unsafe.Pointer(cPayload2))

	res := manageSearch(C.longlong(ctxID), C.int(action), cPayload, cPayload2)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func ManageVectorDBForTest(ctxID int64, action int, payload string, payload2 string) string {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	cPayload2 := C.CString(payload2)
	defer C.free(unsafe.Pointer(cPayload2))

	res := manageVectorDB(C.longlong(ctxID), C.int(action), cPayload, cPayload2)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func ManageModelStoreForTest(ctxID int64, action int, payload string, payload2 string) string {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	cPayload2 := C.CString(payload2)
	defer C.free(unsafe.Pointer(cPayload2))

	res := manageModelStore(C.longlong(ctxID), C.int(action), cPayload, cPayload2)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func ManageLoggingForTest(level int, logPath string) string {
	cLogPath := C.CString(logPath)
	defer C.free(unsafe.Pointer(cLogPath))
	res := manageLogging(C.int(level), cLogPath)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func OpenRedisConnectionForTest(uri string) string {
	cUri := C.CString(uri)
	defer C.free(unsafe.Pointer(cUri))
	res := openRedisConnection(cUri)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func CloseRedisConnectionForTest() string {
	res := closeRedisConnection()
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func OpenCassandraConnectionForTest(payload string) string {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	res := openCassandraConnection(cPayload)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func CloseCassandraConnectionForTest() string {
	res := closeCassandraConnection()
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func IsUniqueBtreeForTest(payload string) string {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	res := isUniqueBtree(cPayload)
	if res == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(res))
	return C.GoString(res)
}

func GetValuesForTest(ctxID int64, payload, payload2 string) (string, string) {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	cPayload2 := C.CString(payload2)
	defer C.free(unsafe.Pointer(cPayload2))
	res, errStr := getValues(getContext(C.longlong(ctxID)), cPayload, cPayload2)

	r := ""
	if res != nil {
		r = C.GoString(res)
		C.free(unsafe.Pointer(res))
	}
	e := ""
	if errStr != nil {
		e = C.GoString(errStr)
		C.free(unsafe.Pointer(errStr))
	}
	return r, e
}

func GetFromBtreeOutForTest(ctxID int64, action int, payload string, payload2 string) (string, string) {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	cPayload2 := C.CString(payload2)
	defer C.free(unsafe.Pointer(cPayload2))

	var cOut *C.char
	var cErr *C.char

	getFromBtreeOut(C.longlong(ctxID), C.int(action), cPayload, cPayload2, &cOut, &cErr)

	r := ""
	if cOut != nil {
		r = C.GoString(cOut)
		C.free(unsafe.Pointer(cOut))
	}
	e := ""
	if cErr != nil {
		e = C.GoString(cErr)
		C.free(unsafe.Pointer(cErr))
	}
	return r, e
}

func GetBtreeItemCountOutForTest(btreeID string) (int64, string) {
	cPayload := C.CString(btreeID)
	defer C.free(unsafe.Pointer(cPayload))

	var cCount C.longlong
	var cErr *C.char

	getBtreeItemCountOut(cPayload, &cCount, &cErr)

	e := ""
	if cErr != nil {
		e = C.GoString(cErr)
		C.free(unsafe.Pointer(cErr))
	}
	return int64(cCount), e
}

func FreeStringForTest() {
	freeString(nil)
	s := C.CString("test")
	freeString(s)
}

func MainForTest() {
	main()
}

func ManageTransactionRawForTest(ctxID int64, action int, payload string) string {
	var cPayload *C.char
	if payload != "" {
		cPayload = C.CString(payload)
		defer C.free(unsafe.Pointer(cPayload))
	}
	res := manageTransaction(C.longlong(ctxID), C.int(action), cPayload)
	if res != nil {
		defer C.free(unsafe.Pointer(res))
		return C.GoString(res)
	}
	return ""
}
