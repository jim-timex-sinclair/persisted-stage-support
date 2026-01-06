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

from perstageutil.duckdb import load
from perstageutil.duckdb.session import Session

class TestDuckDBLoad(unittest.TestCase):

    def setUp(self):
        #set up the test database and create the tables.
        self.test_db_path = os.path.join(repo_path, 'tests', 'data', 'test_persisted_stage.duckdb')
        conn = duckdb.connect(self.test_db_path)
        sql = """
        CREATE or replace TABLE loan__land(loan_number VARCHAR,
        loan_amount DECIMAL(14, 2),
        loan_officer VARCHAR,
        create_timestamp TIMESTAMP,
        update_timestamp TIMESTAMP,
        __pstage_inserted_timestamp TIMESTAMP);
        """
        conn.execute(sql)
        sql = """
        CREATE or replace TABLE loan(loan_number VARCHAR not null,
        loan_amount DECIMAL(14,2),
        loan_officer VARCHAR,
        create_timestamp TIMESTAMP,
        update_timestamp TIMESTAMP,
        __pstage_inserted_timestamp TIMESTAMP not null,
        __pstage_updated_timestamp TIMESTAMP null,
        __pstage_deleted_indicator boolean not null,
        __pstage_hash_diff varchar(32),
        PRIMARY KEY(loan_number));
        """
        conn.execute(sql)

        sql = """
        CREATE or replace TABLE loan__hist(loan_number VARCHAR not null,
        loan_amount DECIMAL(14,2),
        loan_officer VARCHAR,
        create_timestamp TIMESTAMP,
        update_timestamp TIMESTAMP,
        __pstage_effective_timestamp TIMESTAMP not null,
        __pstage_expiration_timestamp TIMESTAMP,
        __pstage_current_version_indicator boolean,
        __pstage_inserted_timestamp TIMESTAMP not null,
        __pstage_updated_timestamp TIMESTAMP null,
        __pstage_deleted_indicator boolean not null,
        __pstage_hash_diff varchar(32),
        PRIMARY KEY(loan_number, __pstage_effective_timestamp));
        """
        conn.execute(sql)

        sql = """
        CREATE OR REPLACE TABLE loan__stage(loan_number VARCHAR,
        loan_amount DECIMAL(14,2),
        loan_officer VARCHAR,
        create_timestamp TIMESTAMP,
        update_timestamp TIMESTAMP);
        """
        conn.execute(sql)

        #code to load and use this not implemented yet, but set it up for later testing.
        sql = """
        CREATE or replace TABLE loan__cks(loan_number VARCHAR not null,
        __pstage_inserted_timestamp TIMESTAMP not null,
        PRIMARY KEY(loan_number));

        """
        conn.execute(sql)

        sql = """
        INSERT INTO main.loan__stage
        (loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp)
        VALUES('1', 100.00, 'john smith', '1992-09-20 11:30:00.123456789'::TIMESTAMP, NULL),
        ('2', 110.00, NULL, '1992-09-20 11:31:00.123456789'::TIMESTAMP, '1992-09-23 11:30:00.000000000'::TIMESTAMP),
        ('3', 130.00, 'bob willis', '1992-09-20 11:32:00.123456789'::TIMESTAMP, '1992-09-24 11:30:00.000000000'::TIMESTAMP);
        """
        conn.execute(sql)

        sql = """
        --ADD update.
        INSERT INTO main.loan__stage
        (loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp)
        VALUES('3', 130.00, 'bob willis', '1992-09-20 11:32:00.123456789'::TIMESTAMP, '1992-09-25 11:30:00.000000000'::TIMESTAMP);
        """
        conn.execute(sql)

        sql = """
        INSERT INTO main.loan__land
        (loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp, __pstage_inserted_timestamp)
        SELECT loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp, current_localtimestamp()
        FROM test_persisted_stage.main.loan__stage;
        """
        conn.execute(sql)
        conn.close()
    
    def tearDown(self):
        #we leave the test db alone for now...
        return super().tearDown()
    
    def test_one(self):
        #setup logging config as one would when using the library
        print("Disconnect any other processes from the target duckdb database as connecting to it locks it.")
        logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d :: %(message)s', level = logging.INFO)
        logger = logging.getLogger("duckdb_ps_load")
        #test_db_path = os.path.join(repo_path, 'tests', 'data', 'test_persisted_stage.duckdb')
        #create a session.
        session = Session(self.test_db_path, logger)
        #run a persisted staging load
        load.exec(session, "test_persisted_stage.main.loan__land", "test_persisted_stage.main.loan", "test_persisted_stage.main.loan__hist", ["update_timestamp", "create_timestamp"])
        #close the sessions connection.
        session.close()

if __name__ == '__main__':
    unittest.main()