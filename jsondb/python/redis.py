import ctypes
import os

import call_go


class RedisOptions:
    """
    Redis connection (pool) options to be sent to SOP.
    """

    def __init__(self):
        self.host = "localhost"
        self.port = 6379
        self.password = ""


def OpenRedisConnection(options: RedisOptions):
    print("inside OpenRedisConnection")
    s1 = options.host.encode("utf-8")
    p = ctypes.c_int(options.port)
    pwd = options.password.encode("utf-8")
    errMsg = call_go.open_redis_connection(ctypes.c_char_p(s1), p, ctypes.c_char_p(pwd))
    if errMsg == None:
        print("Redis connection was successfully opened")
    else:
        raise Exception(f"Redis connection failed to open, details: {errMsg}")


# Open the Redis Connection in SOP.
OpenRedisConnection(RedisOptions())
