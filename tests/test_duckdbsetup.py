import sys
import os
import unittest
import logging
import duckdb
#may not need this....
#from duckdb import DuckDBPyConnection

repo_path = os.path.split(sys.path[0])[0]
  
# Now add the various levels for search purposes
# adding src to path for testing
sys.path.insert(1, os.path.join(repo_path, 'src'))

#sys.path.insert(1, os.path.join(repo_path, 'src', 'topname'))

from perstageutil.duckdb import setup
from perstageutil.duckdb.session import Session

class TestDuckDBLoad(unittest.TestCase):

    def setUp(self):
        #set up the test database and create the tables.
        self.test_db_path = os.path.join(repo_path, 'tests', 'data', 'test_persisted_stage_setup.duckdb')
        conn = duckdb.connect(self.test_db_path)
        sql = """
        CREATE or replace TABLE loan__land(loan_number VARCHAR,
        loan_amount DECIMAL(14, 2),
        loan_officer VARCHAR,
        create_timestamp TIMESTAMP,
        update_timestamp TIMESTAMP);
        """
        conn.execute(sql)
        conn.close()
    
    def tearDown(self):
        #we leave the test db alone for now...
        #self.myteardown()
        return super().tearDown()

    def myteardown(self):
        self.test_db_path = os.path.join(repo_path, 'tests', 'data', 'test_persisted_stage_setup.duckdb')
        conn = duckdb.connect(self.test_db_path)
        sql = """DROP TABLE IF EXISTS loan__land;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS loan__stage;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS loan;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS loan__hist;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS loan__cks;"""
        conn.execute(sql)
    
    def test_one(self):
        #setup logging config as one would when using the library
        print("Disconnect any other processes from the target duckdb database as connecting to it locks it.")
        logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d :: %(message)s', level = logging.DEBUG)
        logger = logging.getLogger("duckdb_ps_load")
        #test_db_path = os.path.join(repo_path, 'tests', 'data', 'test_persisted_stage.duckdb')
        #create a session.
        session = Session(self.test_db_path, logger)
        #run a persisted staging load
        
        setup.exec(session, "test_persisted_stage_setup.main.loan__land", "test_persisted_stage_setup.main.loan", "test_persisted_stage_setup.main.loan__hist", "test_persisted_stage_setup.main.loan__cks", ["loan_number"])

        #close the sessions connection.
        session.close()

if __name__ == '__main__':
    unittest.main()