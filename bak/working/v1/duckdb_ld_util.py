#
# Duckdb persisted staging load program
#

import secrets
import duckdb
from collections import namedtuple

from duckdb import DuckDBPyConnection 
import jinja2
from datetime import datetime
from dataclasses import dataclass
import pandas
import numpy

# Logging setup
import logging

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d :: %(message)s', level = logging.INFO)
logger = logging.getLogger("duckdb_ps_load")
#test the logging

# sesson class is a wrapper over the connection object and handles opening and closing a connection similar to the way 
# pyspark SparkSesson object.  This little bit of extra code is to make the code more Databricks pyspark and Snowflake Snowpark like so it can
# be ported to those use cases.
class Session():
    """
    Docstring for Session
    """
    conn : DuckDBPyConnection
    def __init__(self, db_path_file):
        """
        Docstring for __init__
        
        :param self: Description
        :param db_path_file: Description
        """
        try:
            self.conn = duckdb.connect(db_path_file)
        except Exception as err:
            logger.error(err)
            raise err
        
    def close(self):
        """
        Docstring for close
        
        :param self: Description
        """
        self.conn.close()
        logger.debug("Connection closed.")

DataObject = namedtuple("DataObject", "table_catalog table_schema table_name")

#The main exec of this function.  This mimics Snowflake python procedures a bit on that you need a main handeler to call so that
# this codd can then be used as a template to create procedures within cloud RDBMS environments.
def exec(session : Session, landing_table_full_name : str, current_table_full_name : str, hist_table_full_name : str, source_change_timestamp_columns : list):
    """
    Docstring for exec
    
    :param session: Description
    :type session: Session
    :param landing_table_full_name: Description
    :type landing_table_full_name: str
    :param current_table_full_name: Description
    :type current_table_full_name: str
    :param hist_table_full_name: Description
    :type hist_table_full_name: str
    """
    logger.info(f"Starting load for namespace {current_table_full_name}.")
    landing_table_object = split_three_part_name(landing_table_full_name)
    current_table_object = split_three_part_name(current_table_full_name)
    hist_table_object = split_three_part_name(hist_table_full_name)
    check_tables(session, landing_table_object, current_table_object, hist_table_object)
    land_to_current_map_df = return_column_map_df(session, landing_table_object, current_table_object)
    current_to_hist_map_df = return_column_map_df(session, current_table_object, hist_table_object)
    land_to_current_key_map_df = return_key_columns_df(land_to_current_map_df)
    current_to_hist_key_map_df = return_key_columns_df(current_to_hist_map_df)
    land_to_current_attribute_map_df = return_attribute_columns_only_df(land_to_current_map_df)
    current_to_hist_attribute_map_df = return_attribute_columns_only_df(current_to_hist_map_df)
    create_source_cte_table_sql = generate_create_source_cte_table_sql(landing_table_object, land_to_current_key_map_df, land_to_current_attribute_map_df, source_change_timestamp_columns)
    logger.debug(create_source_cte_table_sql)
    current_merge_sql = generate_current_merge_sql(landing_table_object, current_table_object, land_to_current_key_map_df, land_to_current_attribute_map_df)
    logger.debug(current_merge_sql)
    hist_insert_sql = generate_hist_insert_sql(current_table_object, hist_table_object, current_to_hist_key_map_df, current_to_hist_attribute_map_df)
    logger.debug(hist_insert_sql)
    hist_update_sql = generate_hist_update_sql(hist_table_object, current_to_hist_key_map_df)
    logger.debug(hist_update_sql)
    delete_loaded_sql = generate_delete_loaded_sql(landing_table_object, land_to_current_key_map_df)
    logger.debug(delete_loaded_sql)
    drop_source_cte_table_sql = generate_drop_source_cte_table_sql(landing_table_object)
    logger.debug(drop_source_cte_table_sql)
    run_load(session, landing_table_object, create_source_cte_table_sql, current_merge_sql, hist_insert_sql, hist_update_sql, delete_loaded_sql, drop_source_cte_table_sql)
    logger.info(f"Load completed for namespace {current_table_full_name}.")
    #test_select2(session)

def run_load(session : Session, landing_table_object : DataObject, create_source_cte_table_sql : str, current_merge_sql : str, 
             hist_insert_sql : str, hist_update_sql : str, delete_loaded_sql : str, drop_source_cte_table_sql : str):
    logger.info(f"Creating cte table.")
    exec_dml(session, create_source_cte_table_sql)
    logger.info(f"Running the batch merge, inserts, and upates.")
    run_batches(session, landing_table_object, current_merge_sql, hist_insert_sql, hist_update_sql)
    logger.info(f"Deleting loaded data from landing table.")
    exec_dml(session, delete_loaded_sql)
    logger.info(f"Dropping temporary cte table.")
    exec_dml(session, drop_source_cte_table_sql)

def split_three_part_name(three_part_name : str) -> DataObject:
    """
    Splits a database name db.schema.table and returns as DataObject tuple which 
    is a named tuple that contains the three part name.
    
    :param three_part_name: Description
    :type three_part_name: str
    :return: Description
    :rtype: DataObject
    """
    table_catalog, table_schema, table_name = tuple(three_part_name.split("."))
    data_object = DataObject(table_catalog, table_schema, table_name)
    return data_object

#TODO Impplement this
def check_tables(session : Session, landing_table_object : DataObject, current_table_object : DataObject, hist_table_object : DataObject) -> bool:
    """
    Check the tables to see if the proper metadata and keys are implemented.  Raise an error to stop processing in the event of missing 
    metadata columns or missing keys.
    
    :param session: Description
    :param landing_table_object: Description
    :type landing_table_object: DataObject
    :param current_table_object: Description
    :type current_table_object: DataObject
    :param hist_table_object: Description
    :type hist_table_object: DataObject
    """
    #landing just needs a load date timestamp
    check_landing_table_structure(session, landing_table_object)
    check_current_table_structure(session, current_table_object)
    check_hist_table_structure(session, hist_table_object)

def check_landing_table_structure(session : Session, landing_table_object : DataObject):
    df = return_columns_df(session, landing_table_object)
    if ((df["column_name"] == "__pstage_load_dts").any()) == False:
        raise Exception("Landing table is missing the needed __pstage_load_dts column.")

def check_current_table_structure(session : Session, current_table_object : DataObject):
    df = return_columns_df(session, current_table_object)
    if ((df["column_name"] == "__pstage_synced_timestamp").any()) == False:
        raise Exception("Current table is missing the needed __pstage_synced_timestamp column.")
    if ((df["column_name"] == "__pstage_deleted_indicator").any()) == False:
        raise Exception("Current table is missing the needed __pstage_deleted_indicator column.")
    if ((df["column_name"] == "__pstage_hash_diff").any()) == False:
        raise Exception("Current table is missing the needed __pstage_hash_diff column.")
    if ((df["primary_key_indicator"] == True).any()) == False:
        raise Exception("Current table is missing the needed primary key.")
    
def check_hist_table_structure(session : Session, hist_table_object : DataObject):
    df = return_columns_df(session, hist_table_object)
    if ((df["column_name"] == "__pstage_start_timestamp").any()) == False:
        raise Exception("Current table is missing the needed __pstage_start_timestamp column.")
    if ((df["column_name"] == "__pstage_end_timestamp").any()) == False:
        raise Exception("Current table is missing the needed __pstage_end_timestamp column.")
    if ((df["column_name"] == "__pstage_active_indicator").any()) == False:
        raise Exception("Current table is missing the needed __pstage_active_indicator column.")
    if ((df["column_name"] == "__pstage_synced_timestamp").any()) == False:
        raise Exception("Current table is missing the needed __pstage_synced_timestamp column.")
    if ((df["column_name"] == "__pstage_deleted_indicator").any()) == False:
        raise Exception("Current table is missing the needed __pstage_deleted_indicator column.")
    if ((df["column_name"] == "__pstage_hash_diff").any()) == False:
        raise Exception("Current table is missing the needed __pstage_hash_diff column.")
    if ((df["primary_key_indicator"] == True).any()) == False:
        raise Exception("Current table is missing the needed primary key.")

def return_columns_df(session : Session, table_object : DataObject) -> pandas.DataFrame:
    sql = f"""
    SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.ordinal_position, c.data_type,
    CASE
        WHEN kcu.constraint_name IS NULL THEN FALSE
        ELSE TRUE
    END AS primary_key_indicator
    FROM information_schema.columns c
    LEFT JOIN information_schema.table_constraints tc
    ON c.table_catalog = tc.table_catalog
    AND c.table_schema = tc.table_schema
    AND c.table_name = tc.table_name
    AND tc.constraint_type = 'PRIMARY KEY'
    LEFT JOIN information_schema.key_column_usage kcu
    ON c.table_catalog = kcu.table_catalog
    AND c.table_schema = kcu.table_schema
    AND c.table_name = kcu.table_name
    AND c.column_name = kcu.column_name
    AND tc.constraint_name = kcu.constraint_name
    WHERE c.table_catalog = '{table_object.table_catalog}'
    AND c.table_schema = '{table_object.table_schema}'
    AND c.table_name = '{table_object.table_name}'
    ORDER BY c.table_catalog, c.table_schema, c.table_name, c.ordinal_position;
    """
    df = exec_sql_return_df(session, sql)
    return df

def return_column_map_df(session : Session, source_table_object : DataObject, target_table_object : DataObject) -> pandas.DataFrame:
    sql = f"""
    SELECT s_columns.table_catalog source_table_catalog, s_columns.table_schema source_table_schema, 
    s_columns.table_name source_table_name, s_columns.column_name source_column_name, s_columns.ordinal_position source_ordinal_position, s_columns.data_type source_data_type,
    t_columns.table_catalog target_table_catalog, t_columns.table_schema target_table_schema, t_columns.table_name target_table_name, t_columns.column_name target_column_name, t_columns.ordinal_position target_ordinal_position, t_columns.data_type target_data_type,
    CASE
        WHEN kcu.constraint_name IS NULL THEN FALSE
        ELSE TRUE
    END AS primary_key_indicator
    FROM information_schema.columns t_columns
    INNER JOIN information_schema.columns s_columns
    ON s_columns.table_catalog = '{source_table_object.table_catalog}'
    AND s_columns.table_schema = '{source_table_object.table_schema}'
    AND s_columns.table_name = '{source_table_object.table_name}'
    AND t_columns.column_name = s_columns.column_name
    LEFT JOIN information_schema.table_constraints ttc
    ON t_columns.table_catalog = ttc.table_catalog
    AND t_columns.table_schema = ttc.table_schema
    AND t_columns.table_name = ttc.table_name
    AND ttc.constraint_type = 'PRIMARY KEY'
    LEFT JOIN information_schema.key_column_usage kcu
    ON t_columns.table_catalog = kcu.table_catalog
    AND t_columns.table_schema = kcu.table_schema
    AND t_columns.table_name = kcu.table_name
    AND t_columns.column_name = kcu.column_name
    AND ttc.constraint_name = kcu.constraint_name
    WHERE t_columns.table_catalog = '{target_table_object.table_catalog}'
    AND t_columns.table_schema = '{target_table_object.table_schema}'
    AND t_columns.table_name = '{target_table_object.table_name}'
    AND t_columns.column_name NOT LIKE '__pstage_%'
    ORDER BY t_columns.table_catalog, t_columns.table_schema, t_columns.table_name, t_columns.ordinal_position;
    """
    df = exec_sql_return_df(session, sql)
    return df

def return_key_columns_df(df : pandas.DataFrame) -> pandas.DataFrame:
    #df["column_sql"] = numpy.where(df["source_data_type"] == "VARCHAR", f"TRIM({df["source_column_name"]})", df["source_column_name"])
    return_df = df[(df['primary_key_indicator'] == True) & (~df['target_column_name'].str.startswith('__pstage_', na=False))]
    return return_df

def return_attribute_columns_only_df(df : pandas.DataFrame) -> pandas.DataFrame:
    """
    Returns non key columns and non-metadata columns (attribute columns only).
    
    :param df: Description
    :type df: pandas.DataFrame
    :return: Description
    :rtype: DataFrame
    """
    #df["column_sql"] = numpy.where(df["source_data_type"] == "VARCHAR", f"TRIM({df["source_column_name"]})", df["source_column_name"])
    #df["column_md5_sql"] = numpy.where(df["source_data_type"] == "VARCHAR", f"IFNULL(TRIM({df["source_column_name"]}, '')", f"IFNULL(CAST({df["source_column_name"]} AS VARCHAR(128)), '')")
    return_df = df[(df['primary_key_indicator'] == False) & (~df['target_column_name'].str.startswith('__pstage_', na=False))]
    return return_df

def generate_create_source_cte_table_sql(landing_table_object : DataObject, land_to_current_key_map_df : pandas.DataFrame, 
                                         land_to_current_attribute_map_df : pandas.DataFrame, source_change_timestamp_columns : list) -> str:
    land_to_current_key_map_records = convert_df_to_records(land_to_current_key_map_df)
    land_to_current_attribute_map_records = convert_df_to_records(land_to_current_attribute_map_df)
    source_change_timestamp_sql = ", ".join(source_change_timestamp_columns)
    for record in land_to_current_key_map_records:
        if record["source_data_type"].upper() == "VARCHAR":
            record["column_sql"] = f"TRIM({record["source_column_name"]})"
        else:
            record["column_sql"] = record["source_column_name"]

    for record in land_to_current_attribute_map_records:
        if record["source_data_type"].upper() == "VARCHAR":
            record["column_sql"] = f"TRIM({record["source_column_name"]})"
            record["column_md5_sql"] = f"TRIM({record["source_column_name"]})"
        else:
            record["column_sql"] = record["source_column_name"]
            record["column_md5_sql"] = f"IFNULL(CAST({record["source_column_name"]} AS VARCHAR(128)), '')"

    context = {"landing_table_catalog":landing_table_object.table_catalog, "landing_table_schema":landing_table_object.table_schema, "landing_table_name":landing_table_object.table_name,
               "land_to_current_key_map_records":land_to_current_key_map_records, "land_to_current_attribute_map_records":land_to_current_attribute_map_records,
               "source_change_timestamp_sql":source_change_timestamp_sql}
    template = "create_source_cte_table.jinja"
    sql = create_sql(template, context)
    return sql


def generate_current_merge_sql(landing_table_object : DataObject, current_table_object : DataObject, 
                               land_to_current_key_map_df : pandas.DataFrame, land_to_current_attribute_map_df : pandas.DataFrame) -> str:
    land_to_current_key_map_records = convert_df_to_records(land_to_current_key_map_df)
    land_to_current_attribute_map_records = convert_df_to_records(land_to_current_attribute_map_df)
    context = {"landing_table_catalog":landing_table_object.table_catalog, "landing_table_schema":landing_table_object.table_schema, "landing_table_name":landing_table_object.table_name,
               "current_table_catalog":current_table_object.table_catalog, "current_table_schema":current_table_object.table_schema, "current_table_name":current_table_object.table_name,
               "land_to_current_key_map_records":land_to_current_key_map_records, "land_to_current_attribute_map_records":land_to_current_attribute_map_records}
    template = "current_merge.jinja"
    sql = create_sql(template, context)
    return sql

def generate_delete_loaded_sql(landing_table_object : DataObject, land_to_current_key_map_df : pandas.DataFrame,):
    land_to_current_key_map_records = convert_df_to_records(land_to_current_key_map_df)
    context = {"landing_table_catalog":landing_table_object.table_catalog, "landing_table_schema":landing_table_object.table_schema,
               "landing_table_name":landing_table_object.table_name, "land_to_current_key_map_records":land_to_current_key_map_records}
    template = "delete_loaded.jinja"
    sql = create_sql(template, context)
    return sql

def generate_drop_source_cte_table_sql(landing_table_object : DataObject) -> str:
    context = {"table_catalog":landing_table_object.table_catalog, "table_schema":landing_table_object.table_schema, "table_name":landing_table_object.table_name}
    template = "drop_source_cte_table.jinja"
    sql = create_sql(template, context)
    return sql

def generate_hist_insert_sql(current_table_object : DataObject, hist_table_object : DataObject, 
                               current_to_hist_key_map_df : pandas.DataFrame, current_to_hist_attribute_map_df : pandas.DataFrame) -> str:
    current_to_hist_key_map_records = convert_df_to_records(current_to_hist_key_map_df)
    current_to_hist_attribute_map_records = convert_df_to_records(current_to_hist_attribute_map_df)
    context = {"current_table_catalog":current_table_object.table_catalog, "current_table_schema":current_table_object.table_schema, "current_table_name":current_table_object.table_name,
               "hist_table_catalog":hist_table_object.table_catalog, "hist_table_schema":hist_table_object.table_schema, "hist_table_name":hist_table_object.table_name,
               "current_to_hist_key_map_records":current_to_hist_key_map_records, "current_to_hist_attribute_map_records":current_to_hist_attribute_map_records}
    template = "hist_insert.jinja"
    sql = create_sql(template, context)
    return sql

def generate_hist_update_sql(hist_table_object : DataObject, current_to_hist_key_map_df : pandas.DataFrame) -> str:
    current_to_hist_key_map_records = convert_df_to_records(current_to_hist_key_map_df)
    context = {"hist_table_catalog":hist_table_object.table_catalog, "hist_table_schema":hist_table_object.table_schema, "hist_table_name":hist_table_object.table_name,
               "current_to_hist_key_map_records":current_to_hist_key_map_records}
    template = "hist_update.jinja"
    sql = create_sql(template, context)
    return sql

def run_batch(session : Session, load_order : int, current_merge_sql : str, hist_insert_sql : str, hist_update_sql : str):
    current_batch_merge_sql = current_merge_sql.replace("{load_order}", load_order)
    logger.info("Starting current merge.")
    exec_dml(session, current_batch_merge_sql)
    logger.info("Current merge completed.")
    logger.info("Starting hist insert.")
    exec_dml(session, hist_insert_sql)
    logger.info("Hist insert completed.")
    logger.info("Starting hist update.")
    exec_dml(session, hist_update_sql)
    logger.info("Hist update completed.")

def run_batches(session : Session, landing_table_object : DataObject, current_merge_sql : str, hist_insert_sql : str, hist_update_sql : str):
    batch_list = get_batch_list(session, landing_table_object)
    logger.info("Starting batch load.  Note: batches are run in reverse load order.")
    for batch in batch_list:
        load_order, = batch
        load_order = str(load_order)
        logger.info(f"Starting batch {load_order}.")
        run_batch(session, load_order, current_merge_sql, hist_insert_sql, hist_update_sql)
        logger.info(f"Finished batch {load_order}.")
    logger.info("Batch load complete.")

def get_batch_list(session : Session, landing_table_object : DataObject):
    sql = f"""
    SELECT DISTINCT __pstage_load_order
    FROM temp.main.temp__cte_{landing_table_object.table_name}
    ORDER BY __pstage_load_order DESC
    """
    batch_list = exec_sql_return(session, sql)
    return batch_list

def create_sql(template : str, context : dict):
    template : jinja2.Template
    env = jinja2.Environment(
        loader=jinja2.PackageLoader("duckdb_ld_util"),
        trim_blocks=True,
        lstrip_blocks = True,
        autoescape=jinja2.select_autoescape()
    )
    template = env.get_template(template)
    #,
    #    autoescape=jinja2.select_autoescape()
    sql = template.render(context)
    return sql

def exec_dml(session : Session, sql : str):
    """
    Executes data manipulation language statements in DuckDb and captures and logs the Updated rows output.
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    """
    try:
        logger.debug(f"SQL: {sql}")
        logger.info(f"Partial output of query being executed (first 256 characters):\n{sql[:256]}\n...")
        #session.conn.sql(sql)
        results = session.conn.execute(sql).fetchall()
        for result in results:
            updated_rows, = result
            updated_rows = str(updated_rows)
            logger.info(f"Updated rows: {updated_rows}.")
    except Exception as err:
        logger.error(err)
        raise err
    
def exec_sql_return(session : Session, sql : str):
    """
    Docstring for exec_sql_return
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    """
    try:
        logger.debug(f"SQL: {sql}")
        results = session.conn.execute(sql).fetchall()
        #results = session.conn.sql(sql).fetchall()
    except Exception as err:
        logger.error(err)
        raise err
    return results

def exec_sql_return_df(session : Session, sql : str) -> pandas.DataFrame:
    """
    Docstring for exec_sql_return_df
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    :return: Description
    :rtype: DataFrame
    """
    try:
        logger.debug(f"SQL: {sql}")
        results = session.conn.execute(sql).df()
        #results = session.conn.sql(sql).df()
    except Exception as err:
        logger.error(err)
        raise err
    return results

def convert_df_to_records(df : pandas.DataFrame):
    records = df.to_dict("records")
    return records

def main():
    print("\nstart test run")
    print("---------------")
    session = Session(r"C:\Users\james\Source\Repos\data-load-utilities\duckdb\data\test_persisted_stage.duckdb")
    #db_path_file =r"C:\Users\james\Source\Repos\data-load-utilities\duckdb\data\test_persisted_stage.duckdb"
    #conn = duckdb.connect(db_path_file)
    exec(session, "test_persisted_stage.main.loan__land", "test_persisted_stage.main.loan", "test_persisted_stage.main.loan__hist", ["update_timestamp", "create_timestamp"])
    session.close()
 
if __name__ == '__main__':
    main()