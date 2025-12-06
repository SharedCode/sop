from . import call_go

class Redis:
    """Redis Python wrapper. Delegates API calls to the SOP library that does Direct IO to disk drives w/ built-in L1/L2 caching."""

    @staticmethod
    def initialize(connection_string: str):
        """
        Open the global Redis connection using a connection string (URI).
        Example: redis://:password@localhost:6379/0
        """
        errMsg = call_go.open_redis_connection(connection_string)
        if errMsg:
            raise Exception(f"Redis connection failed to open, details: {errMsg}")

    @staticmethod
    def close():
        """
        Close the global Redis connection.
        """
        errMsg = call_go.close_redis_connection()
        if errMsg:
            raise Exception(f"Redis connection failed to close, details: {errMsg}")
