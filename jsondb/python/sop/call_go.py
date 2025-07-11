import ctypes
import os
import platform

# Determine the shared library extension based on the architecture & operating system
architecture = platform.machine()
if architecture == 'arm64' or architecture == 'aarch64':
    arch = "arm64"
elif architecture == 'x86_64' or architecture == 'AMD64':
    arch = "amd64"

uname = os.uname().sysname
print(uname)
if uname == "Darwin":
    ext = f"{arch}darwin.dylib"
elif uname == "Windows":
    ext = f"{arch}windows.dll"
else:
    ext = f"{arch}linux.so"

# Load the shared library
try:
    script_dir = os.path.dirname(__file__) # Get directory of the current script
    library_path = os.path.join(script_dir, f"libjsondb_{ext}") # Adjust filename
    lib = ctypes.CDLL(library_path)
except OSError as e:
    print(f"Error loading library: {e}")
    print("Ensure 'libjsondb_<arch><os>.so' (or .dll/.dylib) is in the same directory.")
    exit()

# Call the 'hello' function (no arguments, no return value)
print("Calling Go's open_redis_connection() function:")
_open_redis_conn = lib.openRedisConnection

# Call the 'open+_redis_connection' function with arguments and set argument/return types
_open_redis_conn.argtypes = [
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.c_char_p,
]  # Specify argument types
_open_redis_conn.restype = ctypes.POINTER(ctypes.c_char)  # Specify return type

_close_redis_conn = lib.closeRedisConnection
_close_redis_conn.restype = ctypes.POINTER(ctypes.c_char)  # Specify return type

# De-allocate backing memory for a string.
_free_string = lib.freeString
_free_string.argtypes = [ctypes.c_char_p]
_free_string.restype = None

_create_context = lib.createContext
_create_context.argtypes = None
_create_context.restype = ctypes.c_int64  # Specify return type
_remove_context = lib.removeContext
_remove_context.argtypes = [ctypes.c_int64]  # Specify return type
_remove_context.restype = None
_cancel_context = lib.cancelContext
_cancel_context.argtypes = [ctypes.c_int64]  # Specify return type
_cancel_context.restype = None

_manage_transaction = lib.manageTransaction

_manage_transaction.argtypes = [
    ctypes.c_int64,
    ctypes.c_int,
    ctypes.c_char_p,
]  # Specify argument types
_manage_transaction.restype = ctypes.POINTER(ctypes.c_char)  # Specify return type

_manage_btree = lib.manageBtree
_manage_btree.argtypes = [
    ctypes.c_int64,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_char_p,
]  # Specify argument types
_manage_btree.restype = ctypes.POINTER(ctypes.c_char)  # Specify return type


# Define the structure for return values
class ResultStruct(ctypes.Structure):
    _fields_ = [
        ("payload", ctypes.POINTER(ctypes.c_char)),  # First return value (C string)
        ("error", ctypes.POINTER(ctypes.c_char)),  # Second return value (C string)
    ]


_get_from_btree = lib.getFromBtree
_get_from_btree.argtypes = [
    ctypes.c_int64,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_char_p,
]  # Specify argument types
_get_from_btree.restype = ResultStruct  # Specify return type

_navigate_btree = lib.navigateBtree
_navigate_btree.argtypes = [
    ctypes.c_int64,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_char_p,
]  # Specify argument types
_navigate_btree.restype = ctypes.POINTER(ctypes.c_char)  # Specify return type

_is_unique_btree = lib.isUniqueBtree
_is_unique_btree.argtypes = [
    ctypes.c_char_p,
]  # Specify argument types
_is_unique_btree.restype = ctypes.POINTER(ctypes.c_char)  # Specify return type


class getBtreeCountResult(ctypes.Structure):
    _fields_ = [
        ("count", ctypes.c_int64),  # First return value (C long)
        ("error", ctypes.POINTER(ctypes.c_char)),  # Second return value (C string)
    ]


_get_btree_item_count = lib.getBtreeItemCount
_get_btree_item_count.argtypes = [
    ctypes.c_char_p,
]  # Specify argument types
_get_btree_item_count.restype = getBtreeCountResult  # Specify return type


def create_context() -> int:
    return ctypes.c_int64(_create_context()).value


def remove_context(ctxid: int):
    _remove_context(ctypes.c_int64(ctxid).value)


def cancel_context(ctxid: int):
    _cancel_context(ctypes.c_int64(ctxid).value)


def open_redis_connection(host: str, port: int, password: str) -> str:
    """
    Open the Redis connection.
    """

    res = _open_redis_conn(to_cstring(host), to_cint(port), to_cstring(password))
    if res is None or ctypes.cast(res, ctypes.c_char_p).value is None:
        return None

    s = to_str(res)
    # free the string allocated in C heap using malloc.
    _free_string(res)
    return s


def close_redis_connection() -> str:
    """
    Close the Redis connection.
    """

    res = _close_redis_conn()
    if res is None or ctypes.cast(res, ctypes.c_char_p).value is None:
        return None

    s = to_str(res)
    # free the string allocated in C heap using malloc.
    _free_string(res)
    return s


def manage_transaction(ctxID: int, action: int, payload: str) -> str:
    """
    Manage a SOP transaction.
    """

    res = _manage_transaction(
        ctypes.c_int64(ctxID).value, to_cint(action), to_cstring(payload)
    )
    if res is None or ctypes.cast(res, ctypes.c_char_p).value is None:
        return None

    s = to_str(res)
    # free the string allocated in C heap using malloc.
    _free_string(res)

    return s


def manage_btree(ctxID: int, action: int, payload: str, payload2: str) -> str:
    """
    Manage a SOP btree.
    """

    p2 = None
    if payload2 is not None:
        p2 = to_cstring(payload2)
    res = _manage_btree(
        ctypes.c_int64(ctxID).value, to_cint(action), to_cstring(payload), p2
    )
    if res is None or ctypes.cast(res, ctypes.c_char_p).value is None:
        return None

    s = to_str(res)
    # free the string allocated in C heap using malloc.
    _free_string(res)

    return s


def navigate_btree(ctxID: int, action: int, payload: str, payload2: str) -> str:
    """
    Navigate a SOP btree.
    """

    p2 = None
    if payload2 is not None:
        p2 = to_cstring(payload2)
    res = _navigate_btree(
        ctypes.c_int64(ctxID).value, to_cint(action), to_cstring(payload), p2
    )
    if res is None or ctypes.cast(res, ctypes.c_char_p).value is None:
        return None

    s = to_str(res)
    # free the string allocated in C heap using malloc.
    _free_string(res)

    return s


def get_from_btree(ctxID: int, action: int, payload: str, payload2: str):
    """
    Fetch/Navigate from SOP btree.
    """

    p2 = None
    if payload2 is not None:
        p2 = to_cstring(payload2)
    result = _get_from_btree(
        ctypes.c_int64(ctxID).value, to_cint(action), to_cstring(payload), p2
    )
    if (
        result.error is not None
        and ctypes.cast(result.error, ctypes.c_char_p).value is not None
    ):
        se = to_str(result.error)
        # free the string allocated in C heap using malloc.
        _free_string(result.error)
        if (
            result.payload is not None
            and ctypes.cast(result.payload, ctypes.c_char_p).value is not None
        ):
            sp = to_str(result.payload)
            _free_string(result.payload)
            return sp, se
        return None, se

    s = to_str(result.payload)
    # free the string allocated in C heap using malloc.
    _free_string(result.payload)

    return s, None


def is_unique_btree(payload: str) -> str:
    """
    IsUnique btree.
    """

    res = _is_unique_btree(to_cstring(payload))
    if res is None or ctypes.cast(res, ctypes.c_char_p).value is None:
        return None

    s = to_str(res)
    # free the string allocated in C heap using malloc.
    _free_string(res)

    return s


def get_btree_item_count(payload: str):
    """
    Get btree item count.
    """

    result = _get_btree_item_count(to_cstring(payload))
    if (
        result.error is not None
        and ctypes.cast(result.error, ctypes.c_char_p).value is not None
    ):
        se = to_str(result.error)
        # free the string allocated in C heap using malloc.
        _free_string(result.error)
        return 0, se

    return result.count, None


def to_str(s: ctypes.c_char_p) -> str:
    return ctypes.cast(s, ctypes.c_char_p).value.decode("utf-8")


def to_cstring(s: str) -> ctypes.c_char_p:
    return ctypes.c_char_p(s.encode("utf-8"))


def to_cint(i: int) -> ctypes.c_int:
    return ctypes.c_int(i)
