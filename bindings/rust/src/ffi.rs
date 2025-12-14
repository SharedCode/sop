use libc::{c_char, c_int, c_longlong};

#[repr(C)]
pub struct GoResult {
    pub r0: *mut c_char,
    pub r1: *mut c_char,
}

#[repr(C)]
pub struct GetBtreeItemCountReturn {
    pub r0: c_longlong,
    pub r1: *mut c_char,
}

extern "C" {
    pub fn manageVectorDB(ctxID: c_longlong, action: c_int, targetID: *mut c_char, payload: *mut c_char) -> *mut c_char;
    pub fn manageModelStore(ctxID: c_longlong, action: c_int, targetID: *mut c_char, payload: *mut c_char) -> *mut c_char;
    pub fn navigateBtree(ctxID: c_longlong, action: c_int, payload: *mut c_char, payload2: *mut c_char) -> *mut c_char;
    // pub fn isUniqueBtree(payload: *mut c_char) -> *mut c_char;
    pub fn getFromBtree(ctxID: c_longlong, action: c_int, payload: *mut c_char, payload2: *mut c_char) -> GoResult;
    // pub fn getFromBtreeOut(ctxID: c_longlong, action: c_int, payload: *mut c_char, payload2: *mut c_char, result: *mut *mut c_char, error: *mut *mut c_char);
    pub fn getBtreeItemCount(payload: *mut c_char) -> GetBtreeItemCountReturn;
    // pub fn getBtreeItemCountOut(payload: *mut c_char, count: *mut c_longlong, error: *mut *mut c_char);
    pub fn createContext() -> c_longlong;
    // pub fn cancelContext(ctxID: c_longlong);
    pub fn removeContext(ctxID: c_longlong);
    pub fn contextError(ctxID: c_longlong) -> *mut c_char;
    pub fn openRedisConnection(uri: *mut c_char) -> *mut c_char;
    pub fn closeRedisConnection() -> *mut c_char;
    pub fn openCassandraConnection(payload: *mut c_char) -> *mut c_char;
    pub fn closeCassandraConnection() -> *mut c_char;
    pub fn manageLogging(level: c_int, logPath: *mut c_char) -> *mut c_char;
    pub fn manageTransaction(ctxID: c_longlong, action: c_int, payload: *mut c_char) -> *mut c_char;
    pub fn manageDatabase(ctxID: c_longlong, action: c_int, targetID: *mut c_char, payload: *mut c_char) -> *mut c_char;
    pub fn freeString(cString: *mut c_char);
    pub fn manageBtree(ctxID: c_longlong, action: c_int, payload: *mut c_char, payload2: *mut c_char) -> *mut c_char;
    pub fn manageSearch(ctxID: c_longlong, action: c_int, targetID: *mut c_char, payload: *mut c_char) -> *mut c_char;
}
