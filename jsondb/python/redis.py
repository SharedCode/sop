import ctypes
import call_go


class RedisOptions:
    """
    Redis connection (pool) options to be sent to SOP.
    """

    def __init__(self):
        self.host = "localhost"
        self.port = 6379
        self.password = ""


def open_redis_connection(options: RedisOptions):
    """
    Open the global Redis connection.
    """
    print("inside OpenRedisConnection")
    s1 = options.host.encode("utf-8")
    p = ctypes.c_int(options.port)
    pwd = options.password.encode("utf-8")
    errMsg = call_go.open_redis_connection(ctypes.c_char_p(s1), p, ctypes.c_char_p(pwd))
    if errMsg == None:
        print("Redis connection was successfully opened")
    else:
        raise Exception(f"Redis connection failed to open, details: {errMsg}")


# Open the Redis Connection in SOP Go side.
open_redis_connection(RedisOptions())


def close_redis_connection():
    """
    Close the global Redis connection.
    """
    errMsg = call_go.close_redis_connection()
    if errMsg == None:
        print("Redis connection was successfully closed")
    else:
        raise Exception(f"Redis connection failed to close, details: {errMsg}")
