import ctypes
import os

# Determine the shared library extension based on the operating system
uname = os.uname().sysname
print(uname)
if uname == "Darwin":
    ext = ".dylib"
elif uname == "Windows":
    ext = ".dll"
else:
    ext = ".so"

# Load the shared library
try:
    lib = ctypes.CDLL(f"./jsondb{ext}")
except OSError as e:
    print(f"Error loading library: {e}")
    print("Ensure 'jsondb.so' (or .dll/.dylib) is in the same directory.")
    exit()

# Call the 'hello' function (no arguments, no return value)
print("Calling Go's open_redis_connection() function:")
_open_redis_conn = lib.open_redis_connection

# Call the 'open+_redis_connection' function with arguments and set argument/return types
_open_redis_conn.argtypes = [
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.c_char_p,
]  # Specify argument types
_open_redis_conn.restype = ctypes.c_char_p  # Specify return type

_close_redis_conn = lib.close_redis_connection
_close_redis_conn.restype = ctypes.c_char_p  # Specify return type

# De-allocate backing memory for a string.
_free_string = lib.free_string
_free_string.argtypes = [ctypes.c_char_p]
_free_string.restype = None

_manage_tran = lib.manage_transaction

_manage_tran.argtypes = [
    ctypes.c_int,
    ctypes.c_char_p,
]  # Specify argument types
_manage_tran.restype = ctypes.c_char_p  # Specify return type

_manage_btree = lib.manage_btree
_manage_btree.argtypes = [
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_char_p,
]  # Specify argument types
_manage_btree.restype = ctypes.c_char_p  # Specify return type


def open_redis_connection(host: str, port: int, password: str) -> str:
    """
    Open the Redis connection.
    """

    res = _open_redis_conn(to_cstring(host), to_cint(port), to_cstring(password))
    if res == None:
        return res

    s = to_str(res)
    # free the string allocated in C heap using malloc.
    # free_string(res)
    return s


def close_redis_connection() -> str:
    """
    Close the Redis connection.
    """

    res = _close_redis_conn()
    if res == None:
        return res

    s = to_str(res)
    # free the string allocated in C heap using malloc.
    # TODO: understand why we are getting "can't free, not allocated" error.
    # free_string(res)
    return s


def manage_transaction(action: int, payload: str) -> str:
    """
    Manage a SOP transaction.
    """

    res = _manage_tran(to_cint(action), to_cstring(payload))
    if res == None:
        return res

    s = to_str(res)
    # free the string allocated in C heap using malloc.
    # free_string(res)

    return s


def manage_btree(action: int, payload: str, payload2: str) -> str:
    """
    Manage a SOP btree.
    """

    res = _manage_btree(to_cint(action), to_cstring(payload), to_cstring(payload2))
    if res == None:
        return res

    s = to_str(res)
    # free the string allocated in C heap using malloc.
    # free_string(res)

    return s


def to_str(s: ctypes.c_char_p) -> str:
    return s.decode("utf-8")


def to_cstring(s: str) -> ctypes.c_char_p:
    return ctypes.c_char_p(s.encode("utf-8"))


def to_cint(i: int) -> ctypes.c_int:
    return ctypes.c_int(i)
