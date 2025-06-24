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
    lib = ctypes.CDLL(f"./hello{ext}")
except OSError as e:
    print(f"Error loading library: {e}")
    print("Ensure 'hello.so' (or .dll/.dylib) is in the same directory.")
    exit()

# Call the 'hello' function (no arguments, no return value)
print("Calling Go's hello() function:")
lib.hello()

# Call the 'add' function with arguments and set argument/return types
add_func = lib.add
add_func.argtypes = [ctypes.c_long, ctypes.c_long]  # Specify argument types
add_func.restype = ctypes.c_long  # Specify return type

num1 = 10
num2 = 15
result = add_func(num1, num2)
print(f"Calling Go's add({num1}, {num2}) function:")
print(f"Result: {result}")
