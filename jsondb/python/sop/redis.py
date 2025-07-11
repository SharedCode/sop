from . import call_go


class RedisOptions:
    """
    Redis connection (pool) options to be sent to SOP.
    """

    def __init__(self):
        self.host = "localhost"
        self.port = 6379
        self.password = ""


class Redis:
    """Redis Python wrapper. Delegates API calls to the SOP library that does Direct IO to disk drives w/ built-in L1/L2 caching."""

    def open_connection(options: RedisOptions):
        """
        Open the global Redis connection.
        """
        print("inside OpenRedisConnection")
        errMsg = call_go.open_redis_connection(
            options.host, options.port, options.password
        )
        if errMsg == None:
            print("Redis connection was successfully opened")
        else:
            raise Exception(f"Redis connection failed to open, details: {errMsg}")

    def close_connection():
        """
        Close the global Redis connection.
        """
        errMsg = call_go.close_redis_connection()
        if errMsg == None:
            print("Redis connection was successfully closed")
        else:
            raise Exception(f"Redis connection failed to close, details: {errMsg}")
