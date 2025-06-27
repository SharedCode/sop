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
open_redis_conn = lib.open_redis_connection

# Call the 'open+_redis_connection' function with arguments and set argument/return types
open_redis_conn.argtypes = [
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.c_char_p,
]  # Specify argument types
open_redis_conn.restype = ctypes.c_char_p  # Specify return type

close_redis_conn = lib.close_redis_connection
close_redis_conn.restype = ctypes.c_char_p  # Specify return type

# De-allocate backing memory for a string.
free_string = lib.free_string
free_string.argtypes = [ctypes.c_char_p]
free_string.restype = None

manage_tran = lib.manage_transaction

manage_tran.argtypes = [
    ctypes.c_int,
    ctypes.c_char_p,
]  # Specify argument types
manage_tran.restype = ctypes.c_char_p  # Specify return type


def open_redis_connection(host: str, port: int, password: str) -> str:
    """
    Open the Redis connection.
    """

    res = open_redis_conn(to_cstring(host), to_cint(port), to_cstring(password))
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

    res = close_redis_conn()
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

    res = manage_tran(to_cint(action), to_cstring(payload))
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
