# Introduction

This documentation is incomplete (and a bit of a mess) but I'm working on it...

Persisted Load Utilities are a number of different programs that use the internal metadata of the RDBMS to upsert data into a persisted staging area.  Rather than code lots of merge and insert processes, one can simply call the functions in this package to load data into tables in a persisted staging area. The project is starting with DuckDB as that is an "open-source, embedded, analytical SQL database management system (DBMS) designed for fast analytical queries on local data" and free and easy to test on.  Future packages are contemplated in other RDBMS such as Databricks and Snowflake.

## Acknowledgments

The author would like to acknowledge the direct contributions of the following people during the initial development of this method while working at Homepoint Financial.

James Newsom

Keith Barefoot

Tom Reddy

Avtar Singh

Saurabh Malhotra

As well the ideas from the following authors were contributed.

Roelant Voss for his writings regarding persisted staging in general.

David Knifton for general architecture guidance and defining the deterministic development approach.

## What is a persisted staging area?

A persisted staging area is optional area in the data solution design that records the transactions (events) that were received by the data solution over time. A Persistent Staging Area (PSA) can be considered a type of data warehouse insurance policy where all data is captured and has the following attributes.
    • Source system data is loaded into PSA without transformation (with minimal data type transformation--where there are no equivalent target data types for source data types)
    • Records are never deleted from PSA (archives may occur)
    • PSA stores all unique records loaded (tracks history)
    • Many more fields are stored in PSA than required by the data warehouse
The end goal here is to use this PSA to capture all the data that the data warehouse receives to ready it to be integrated in an integration data layer, and after integration load it into a dimensional modeled enterprise data warehouse layer to be used in OLAP based reporting. One of the benefits of this approach is that with all the data captured in persisted staging, the integration and enterprise data warehouse layers can be fully re-reloaded in the event of structural changes to the tables. This is something that is difficult using any of the older data warehouse staging approaches because of the temporal nature of the data.

Please see the Persisted Staging Design Reference in the documention area for more information...

But...some high level information from the doc is below:

## 3. Persisted Staging Entity and Attribute Structure

### 3.1. Persisted Staging Architecture Discussion

This method of Persisted staging is comprised of four target tables types for each source table; a landing table, a current table, a historical table, and in cases where deletes cannot be handled by any other means, a current key snapshot table. 

### 3.1.1. Landing

Landing tables are constraint free landing entities that are essentially the first stop for data when it is copied into the database environment. Any data type conversion issue will be found here when data is copied into these landing tables.  Note that you can repeatadly load data into this table and not run the persisted stage process as long as your process updates the __pstage_inserted_timestamp correctly.  The process is robust and will cycle throught the data in the order that it has been loaded FIFO style.

### 3.1.2. Current (Inmon ODS style)

The second stop in the persisted staging load process is the current table. These tables are essentially structural copies of source tables that are persisted in a Inmon ODS like manner; subject-oriented, current valued, volatile, and detailed, but not integrated. Data is first merged into these current tables to optimize raw data loading (the current tables will eventually be a lot smaller than the historical tables so merging data is more efficient), to aid current raw data reporting, and to optimize the historical loads.

### 3.1.3. Historical (Inmon EDW style)

The final stop in the persisted staging load process are the historical tables. These tables are also essentially structural copies of the source tables, but are persisted in a Inmon data warehouse like manner; subject-oriented, time-variant, nonvolatile, but not integrated.

### 3.1.4. Current Key Snapshot

The current key snapshot table is used to load a narrow version of the entire source row set for post load soft delete processing. Essentially, when a key is not in this table, the data in the current table is soft deleted (the __PSTAGE_DELETED_INDICATOR indicator is set to True). The processing should flow down into the historical table when this occurs as well, with a tombstone row inserted with an expiration date of 12/31/9999 and a deleted indicator set to true.

## Table structure needed.

Below is the noted table structure used for this process.  One can run the setup process to set this all up based on a landing table structure.  See more about that below. 

Note the __pstage_ metadata columns which are used by the process to help loading the data.

### Landing table

```{sql}
        CREATE or replace TABLE {name}__land(
        ...columns...
        __pstage_inserted_timestamp TIMESTAMP);
```

### Current table

```{sql}
        CREATE or replace TABLE {name}(
        ...columns...
        __pstage_inserted_timestamp TIMESTAMP not null,
        __pstage_updated_timestamp TIMESTAMP null,
        __pstage_deleted_indicator boolean not null,
        __pstage_hash_diff varchar(32),
        __pstage_dedupe_confidence_percent float not null,
        PRIMARY KEY({primary key columns}));
```
### Historical table

```{sql}
        CREATE or replace TABLE {name}__hist(
        ...columns...
        __pstage_effective_timestamp TIMESTAMP not null,
        __pstage_expiration_timestamp TIMESTAMP,
        __pstage_current_version_indicator boolean,
        __pstage_inserted_timestamp TIMESTAMP not null,
        __pstage_updated_timestamp TIMESTAMP null,
        __pstage_deleted_indicator boolean not null,
        __pstage_hash_diff varchar(32),
        __pstage_dedupe_confidence_percent float not null,
        PRIMARY KEY({primary key columns}, __pstage_effective_timestamp));
```

#### Current key snapshot table

```{sql}
        CREATE or replace TABLE {name}_cks(
        ...primary key columns only....
        __pstage_inserted_timestamp TIMESTAMP not null,
        PRIMARY KEY({primary key columns}));
```

## Load Process Summary

### General Information

The process supports multiple insert batches into the landing table which supports near real time processing.  That is, from one batch to the next, data can be inserted into the landing table.  All that is required is that for each new batch, the landing table’s insert time stamp (__pstage_inserted_timestamp) be updated to represent the insert date and time that the batch of data was inserted.

### Load Summary

1. Data is loaded into landing table using landing process.  That process should only do one thing: land the data and update the insert time stamp (__pstage_inserted_timestamp) to the current inserted date for the landed set of data.

2.  A temporary load table creation process is run which, using a CTE, does a number of things.

        2.1. Perfect dupes which include the inserted timestamp are removed by simply selecting the distinct values from the landing table.

        2.2. A main extract query is run against the perfect dupe query noted in 2.1 which:

            a) Derives a hash difference column from all of the non-key attributes

            b) Organizes the loads into a load order by the inserted timestamp and optionally but at a higher priority by any source system watermark columns.

        2.3. A second extract, the deduplication confidence extract, is run against the main extract created in 2.2 which derives a deduplication confidence number (see below for more details about this concept).

        2.4. The main extract and the deduplication confidence extract are joined together to create the temporary load table.

3. Distinct batch numbers are derived from the temporary load table.

4. For each batch number, which is loaded in descending order data is:

        4.1. Merged into the current table.

        4.2. Inserted into the historical table from the current table where the inserted or update timestamp in the current table is greater that the max timestamp of the historical table.

        4.3. The historical table is updated with the proper current version indicator to indicate the most current record, and the row expiration timestamps are adjusted to follow the previous rows timestamp for the key.

5. The data that was loaded is deleted from the landing table using the key and original timestamp values from the temporary load table which represents the current sets loaded.
6. The temporary table is dropped.

### Deduplication Confidence

In cases where duplicate records exist for the same key with different attribute values within the same batch load timestamp, rather than moving the rows to an error table, the rows are merged into the current table and inserted into the historical table with this confidence value calculated by the number of possible incorrect inserted and\or updated records.  The rows in the historical table then can be interrogated and compared with the source system to resolve the any discrepancy.

# What's new

## 1.0
Initial release.


# Getting Started
1.	Installation process

This is initially not uploaded to pypi.org as it is in a very alpha state.  I suggest building a local package and then installing that package into a local virtual environment to start testing and seeing if the templates and code can be used for other projects.  This readme just shows just that.

## Virtual envs
To create virtual environments in python....

I suggest opening up command prompt NON-ADMINISTRATIVE and creating a sub directory under you user.  For example, mine is jamoran\pyenvironments.

```{cmd}
md pyenvironments
```

To create the environment.

```{cmd}
python -m venv <environment name in a directory where you want it....>
```

Full example.  Note the environment name and the folder name are the same.s

```{cmd}
python -m venv test_environment
```

I created one locally under pyenvironments\test_environment

Mine...for an example...

```{cmd}
cd C:\Users\jamoran\pyenvironments\test_environment
```

Now activate the environment by running .\Scripts\activate.bat in the test_environment directory.

```{cmd}
.\Scripts\activate.bat
```

You deactive it by running .\Scripts\deactivate.bat.

```{cmd}
.\Scripts\deactivate.bat
```

You can copy the install gzip from our share.

```{cmd}
copy x-1.0.0.0.tar.gz C:\Users\<YOUR AD NAME>\pyenvironments\test_environment\x-1.0.0.0.tar.gz
```

You can then simply install it with pip.

```{cmd}
pip install x-1.0.0.0.tar.gz
```

1. Unistall Process

To uninstall the autodade software.

```{cmd}
pip uninstall hpfc_getinfa
```

To clean up your entire virtual environment you can run the following commands which will remove all installed modules.

```{cmd}
pip freeze > to-uninstall.txt
pip uninstall -r to-uninstall.txt
```

I suggest as well, closing down the cmdshell to clear out any cached values.  Again, make sure you activate your test environment first when re-starting cmdshell.

```{cmd}
exit
```
## Running
Once it is set up, then you can run the test to see how it works.

First create your landing table which should be a carbon copy of the source table.

You can then run the setup routine which will add the other needed tables and fix up your landing table with the needed metadata column.

E.g.

Say you run this...

```{sql}
CREATE or replace TABLE loan__land(loan_number VARCHAR,
loan_amount DECIMAL(14, 2),
loan_officer VARCHAR,
create_timestamp TIMESTAMP,
update_timestamp TIMESTAMP);
```

You can then run this to setup the other tables and add needed metadata columns.

```{python}
logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d :: %(message)s', level = logging.DEBUG)
logger = logging.getLogger("duckdb_ps_load")
test_db_path = 'path to DuckDB database'
# Create a session with the path and note you have to pass in a logger object.
session = Session(test_db_path, logger)
#run a setup.

setup.exec(session, "test_persisted_stage_setup.main.loan__land", "test_persisted_stage_setup.main.loan", "test_persisted_stage_setup.main.loan__hist", "test_persisted_stage_setup.main.loan__cks", ["loan_number"])

#close the sessions connection.
session.close()
```

I suggest running test_duckdbload.py.  That test sets up some test tables, inserts data, and then loads the data into the persisted staging area.

Example run below which is pulled out of the testing python code.

1.  Create a DuckDb session by passing in a path to your DuckDb instance.  This should have all of the needed tables set up in it.
2.  Call the load.exec function and pass in the session, the landing table name, the current table name, the historial table name, and any source watermark columns.  What are a watermark columns?  They are columns in the source that indicate when the data was created or updated.  They are used in the code in addition to landing inserted timestamp to organize and merge the data so that it is passed throught he pipeline FIFO style to maintain correctness.  If there are no watermark columns, then pass in an empty list like so: [].

```{python}
import logging
from perstageutil.duckdb import load
from perstageutil.duckdb.session import Session

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d :: %(message)s', level = logging.DEBUG)
logger = logging.getLogger("duckdb_ps_load")
test_db_path = 'path to DuckDB database'
# Create a session with the path and note you have to pass in a logger object.
session = Session(test_db_path, logger)
# Call the load.exec with the various names and the watermark columns.
load.exec(session, "test_persisted_stage.main.loan__land", "test_persisted_stage.main.loan", "test_persisted_stage.main.loan__hist", ["update_timestamp", "create_timestamp"])
```


3.	Software dependencies

The dependencies should be taken care of by the installer.

# Build and Test

## TO BUILD INSTALL ON YOUR COMPUTER

