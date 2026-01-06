# sesson class is a wrapper over the connection object and handles opening and closing a connection similar to the way 
# pyspark SparkSesson object.  This little bit of extra code is to make the code more Databricks pyspark and Snowflake Snowpark like so it can
# be ported to those use cases.

import duckdb
from duckdb import DuckDBPyConnection
from logging import Logger

class Session():
    """
    Docstring for Session
    """
    conn : DuckDBPyConnection
    def __init__(self, db_path_file : str, logger : Logger):
        """
        Docstring for __init__
        
        :param self: Description
        :param db_path_file: Description
        """
        self.logger = logger
        try:
            self.conn = duckdb.connect(db_path_file)
        except Exception as err:
            self.logger.error(err)
            raise err
        
    def close(self):
        """
        Docstring for close
        
        :param self: Description
        """
        self.conn.close()
        self.logger.debug("Connection closed.")