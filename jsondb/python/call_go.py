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


def open_redis_connection(host: str, port: int, password: str) -> str:
    """
    Open the Redis connection.
    """

    res = open_redis_conn(host, port, password)
    if res == None:
        return res

    s = res.value.decode("utf-8")
    # free the string allocated in C heap using malloc.
    free_string(res)
    return s


def close_redis_connection() -> str:
    """
    Close the Redis connection.
    """

    res = close_redis_conn()
    if res == None:
        return res

    s = res.value.decode("utf-8")
    # free the string allocated in C heap using malloc.
    free_string(res)
    return s
