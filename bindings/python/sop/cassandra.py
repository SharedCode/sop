import json
from typing import Dict
from . import call_go

class Cassandra:
    """
    Cassandra class provides static methods to initialize and close the global Cassandra connection.
    """

    @staticmethod
    def initialize(config: Dict[str, any]):
        """
        Initialize the global Cassandra connection.
        
        Args:
            config (Dict[str, any]): Configuration dictionary containing:
                - cluster_hosts (List[str]): List of Cassandra host addresses
                - consistency (int): Consistency level (default: LocalQuorum)
                - connection_timeout (int): Timeout in milliseconds
                - replication_clause (str): Replication strategy string
                - authenticator (Dict): Optional auth config with 'username' and 'password'
        """
        payload = json.dumps(config)
        res = call_go.open_cassandra_connection(payload)
        if res:
            raise Exception(res)

    @staticmethod
    def close():
        """Close the global Cassandra connection."""
        res = call_go.close_cassandra_connection()
        if res:
            raise Exception(res)
