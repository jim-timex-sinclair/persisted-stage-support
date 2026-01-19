#
# Duckdb persisted staging load program
#

#import jinja2
import pandas

# need the below to be able to run this and pick up the local lib if testing this by itself.
# if not running this directly, this import will be skipped.
if __name__ == '__main__':
    import sys
    import os
    sys.path.insert(1, os.path.join(os.getcwd(), 'src'))

from perstageutil.duckdb.session import Session
import perstageutil.duckdb._common as _common
from perstageutil.duckdb._common import DataObject

#constant for setting the jinja package name
JINJA_PACKAGE_NAME = "perstageutil.duckdb.load"

#The main exec of this function.  This mimics Snowflake python procedures a bit on that you need a main handeler to call so that
# this codd can then be used as a template to create procedures within cloud RDBMS environments.
def exec(session : Session, landing_table_full_name : str, current_table_full_name : str, hist_table_full_name : str, source_change_timestamp_columns : list):
    """
    Executes a persisted staging load within a persisted staging namespace.
    
    :param session: Contains the connection information holds the session state for a load.
    :type session: Session
    :param landing_table_full_name: The landing table name that is the source for the persisted staging load data.  This table
    contains the imported data and is used as the source to merge into the current table and insert and update the historical table.  The table needs to have the needed metadata columns and the 
    load will error if they are not there.
    :type landing_table_full_name: str
    :param current_table_full_name: The current table which is a current valued volitile ODS style table.  The table needs to have the needed metadata columns and the 
    load will error if they are not there.
    :type current_table_full_name: str
    :param hist_table_full_name: The historical table to to be inserted into, which is a persisted table that tracks all changes.   The table needs to have the needed metadata columns and the 
    load will error if they are not there.
    :type hist_table_full_name: str
    :param source_change_timestamp_columns: These are columns in the source that indicate a time sequence that should be respected in the the data warehouse load.  These columns will be used in addition 
    to the __pstage_inserted_timestamp to organized the loads and they will be given a higher priority.
    :type source_change_timestamp_columns: list
    """
    session.logger.info(f"Starting load for namespace {current_table_full_name}.")
    landing_table_object = _common.split_three_part_name(landing_table_full_name)
    current_table_object = _common.split_three_part_name(current_table_full_name)
    hist_table_object = _common.split_three_part_name(hist_table_full_name)
    _common.check_tables(session, landing_table_object, current_table_object, hist_table_object)
    land_to_current_map_df = _return_column_map_df(session, landing_table_object, current_table_object)
    current_to_hist_map_df = _return_column_map_df(session, current_table_object, hist_table_object)
    land_to_current_key_map_df = _return_key_columns_df(land_to_current_map_df)
    current_to_hist_key_map_df = _return_key_columns_df(current_to_hist_map_df)
    land_to_current_attribute_map_df = _return_attribute_columns_only_df(land_to_current_map_df)
    current_to_hist_attribute_map_df = _return_attribute_columns_only_df(current_to_hist_map_df)
    create_source_cte_table_sql = _generate_create_source_cte_table_sql(landing_table_object, land_to_current_key_map_df, land_to_current_attribute_map_df, source_change_timestamp_columns)
    session.logger.debug(create_source_cte_table_sql)
    current_merge_sql = _generate_current_merge_sql(landing_table_object, current_table_object, land_to_current_key_map_df, land_to_current_attribute_map_df)
    session.logger.debug(current_merge_sql)
    hist_insert_sql = _generate_hist_insert_sql(current_table_object, hist_table_object, current_to_hist_key_map_df, current_to_hist_attribute_map_df)
    session.logger.debug(hist_insert_sql)
    hist_update_sql = _generate_hist_update_sql(hist_table_object, current_to_hist_key_map_df)
    session.logger.debug(hist_update_sql)
    delete_loaded_sql = _generate_delete_loaded_sql(landing_table_object, land_to_current_key_map_df)
    session.logger.debug(delete_loaded_sql)
    drop_source_cte_table_sql = _generate_drop_source_cte_table_sql(landing_table_object)
    session.logger.debug(drop_source_cte_table_sql)
    _run_load(session, landing_table_object, create_source_cte_table_sql, current_merge_sql, hist_insert_sql, hist_update_sql, delete_loaded_sql, drop_source_cte_table_sql)
    session.logger.info(f"Load completed for namespace {current_table_full_name}.")
    #test_select2(session)

def _run_load(session : Session, landing_table_object : DataObject, create_source_cte_table_sql : str, current_merge_sql : str, 
             hist_insert_sql : str, hist_update_sql : str, delete_loaded_sql : str, drop_source_cte_table_sql : str):
    """
    This function is an implementation detail and should not be used externally.
    
    :param session: Description
    :type session: Session
    :param landing_table_object: Description
    :type landing_table_object: DataObject
    :param create_source_cte_table_sql: Description
    :type create_source_cte_table_sql: str
    :param current_merge_sql: Description
    :type current_merge_sql: str
    :param hist_insert_sql: Description
    :type hist_insert_sql: str
    :param hist_update_sql: Description
    :type hist_update_sql: str
    :param delete_loaded_sql: Description
    :type delete_loaded_sql: str
    :param drop_source_cte_table_sql: Description
    :type drop_source_cte_table_sql: str
    """
    session.logger.info(f"Creating cte table.")
    _common.exec_dml(session, create_source_cte_table_sql)
    session.logger.info(f"Running the batch merge, inserts, and upates.")
    _run_batches(session, landing_table_object, current_merge_sql, hist_insert_sql, hist_update_sql)
    session.logger.info(f"Deleting loaded data from landing table.")
    _common.exec_dml(session, delete_loaded_sql)
    session.logger.info(f"Dropping temporary cte table.")
    _common.exec_dml(session, drop_source_cte_table_sql)

def _return_column_map_df(session : Session, source_table_object : DataObject, target_table_object : DataObject) -> pandas.DataFrame:
    """This function is an implementation detail and should not be used externally."""
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
    df = _common.exec_sql_return_df(session, sql)
    return df

def _return_key_columns_df(df : pandas.DataFrame) -> pandas.DataFrame:
    """This function is an implementation detail and should not be used externally."""
    #df["column_sql"] = numpy.where(df["source_data_type"] == "VARCHAR", f"TRIM({df["source_column_name"]})", df["source_column_name"])
    return_df = df[(df['primary_key_indicator'] == True) & (~df['target_column_name'].str.startswith('__pstage_', na=False))]
    return return_df

def _return_attribute_columns_only_df(df : pandas.DataFrame) -> pandas.DataFrame:
    """
    This function is an implementation detail and should not be used externally.
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

def _generate_create_source_cte_table_sql(landing_table_object : DataObject, land_to_current_key_map_df : pandas.DataFrame, 
                                         land_to_current_attribute_map_df : pandas.DataFrame, source_change_timestamp_columns : list) -> str:
    """This function is an implementation detail and should not be used externally."""
    land_to_current_key_map_records = _common.convert_df_to_records(land_to_current_key_map_df)
    land_to_current_attribute_map_records = _common.convert_df_to_records(land_to_current_attribute_map_df)
    source_change_timestamp_sql = ", ".join(source_change_timestamp_columns)
    for record in land_to_current_key_map_records:
        if record["source_data_type"].upper() == "VARCHAR":
            record["column_sql"] = f"TRIM({record["source_column_name"]})"
        else:
            record["column_sql"] = record["source_column_name"]

    for record in land_to_current_attribute_map_records:
        if record["source_data_type"].upper() == "VARCHAR":
            record["column_sql"] = f"TRIM({record["source_column_name"]})"
            record["column_md5_sql"] = f"IFNULL(TRIM({record["source_column_name"]}), '')"
        else:
            record["column_sql"] = record["source_column_name"]
            record["column_md5_sql"] = f"IFNULL(CAST({record["source_column_name"]} AS VARCHAR(128)), '')"

    context = {"landing_table_catalog":landing_table_object.table_catalog, "landing_table_schema":landing_table_object.table_schema, "landing_table_name":landing_table_object.table_name,
               "land_to_current_key_map_records":land_to_current_key_map_records, "land_to_current_attribute_map_records":land_to_current_attribute_map_records,
               "source_change_timestamp_sql":source_change_timestamp_sql}
    template = "create_source_cte_table.jinja"
    sql = _common.create_sql(template, context)
    return sql


def _generate_current_merge_sql(landing_table_object : DataObject, current_table_object : DataObject, 
                               land_to_current_key_map_df : pandas.DataFrame, land_to_current_attribute_map_df : pandas.DataFrame) -> str:
    """This function is an implementation detail and should not be used externally."""
    land_to_current_key_map_records = _common.convert_df_to_records(land_to_current_key_map_df)
    land_to_current_attribute_map_records = _common.convert_df_to_records(land_to_current_attribute_map_df)
    context = {"landing_table_catalog":landing_table_object.table_catalog, "landing_table_schema":landing_table_object.table_schema, "landing_table_name":landing_table_object.table_name,
               "current_table_catalog":current_table_object.table_catalog, "current_table_schema":current_table_object.table_schema, "current_table_name":current_table_object.table_name,
               "land_to_current_key_map_records":land_to_current_key_map_records, "land_to_current_attribute_map_records":land_to_current_attribute_map_records}
    template = "current_merge.jinja"
    sql = _common.create_sql(template, context)
    return sql

def _generate_delete_loaded_sql(landing_table_object : DataObject, land_to_current_key_map_df : pandas.DataFrame,):
    """
    This function is an implementation detail and should not be used externally.
    
    :param landing_table_object: Description
    :type landing_table_object: DataObject
    :param land_to_current_key_map_df: Description
    :type land_to_current_key_map_df: pandas.DataFrame
    """
    land_to_current_key_map_records = _common.convert_df_to_records(land_to_current_key_map_df)
    context = {"landing_table_catalog":landing_table_object.table_catalog, "landing_table_schema":landing_table_object.table_schema,
               "landing_table_name":landing_table_object.table_name, "land_to_current_key_map_records":land_to_current_key_map_records}
    template = "delete_loaded.jinja"
    sql = _common.create_sql(template, context)
    return sql

def _generate_drop_source_cte_table_sql(landing_table_object : DataObject) -> str:
    """
    This function is an implementation detail and should not be used externally.
    
    :param landing_table_object: Description
    :type landing_table_object: DataObject
    :return: Description
    :rtype: str
    """
    context = {"table_catalog":landing_table_object.table_catalog, "table_schema":landing_table_object.table_schema, "table_name":landing_table_object.table_name}
    template = "drop_source_cte_table.jinja"
    sql = _common.create_sql(template, context)
    return sql

def _generate_hist_insert_sql(current_table_object : DataObject, hist_table_object : DataObject, 
                               current_to_hist_key_map_df : pandas.DataFrame, current_to_hist_attribute_map_df : pandas.DataFrame) -> str:
    """
    This function is an implementation detail and should not be used externally.
    
    :param current_table_object: Description
    :type current_table_object: DataObject
    :param hist_table_object: Description
    :type hist_table_object: DataObject
    :param current_to_hist_key_map_df: Description
    :type current_to_hist_key_map_df: pandas.DataFrame
    :param current_to_hist_attribute_map_df: Description
    :type current_to_hist_attribute_map_df: pandas.DataFrame
    :return: Description
    :rtype: str
    """
    current_to_hist_key_map_records = _common.convert_df_to_records(current_to_hist_key_map_df)
    current_to_hist_attribute_map_records = _common.convert_df_to_records(current_to_hist_attribute_map_df)
    context = {"current_table_catalog":current_table_object.table_catalog, "current_table_schema":current_table_object.table_schema, "current_table_name":current_table_object.table_name,
               "hist_table_catalog":hist_table_object.table_catalog, "hist_table_schema":hist_table_object.table_schema, "hist_table_name":hist_table_object.table_name,
               "current_to_hist_key_map_records":current_to_hist_key_map_records, "current_to_hist_attribute_map_records":current_to_hist_attribute_map_records}
    template = "hist_insert.jinja"
    sql = _common.create_sql(template, context)
    return sql

def _generate_hist_update_sql(hist_table_object : DataObject, current_to_hist_key_map_df : pandas.DataFrame) -> str:
    """
    This function is an implementation detail and should not be used externally.
    
    :param hist_table_object: Description
    :type hist_table_object: DataObject
    :param current_to_hist_key_map_df: Description
    :type current_to_hist_key_map_df: pandas.DataFrame
    :return: Description
    :rtype: str
    """
    current_to_hist_key_map_records = _common.convert_df_to_records(current_to_hist_key_map_df)
    context = {"hist_table_catalog":hist_table_object.table_catalog, "hist_table_schema":hist_table_object.table_schema, "hist_table_name":hist_table_object.table_name,
               "current_to_hist_key_map_records":current_to_hist_key_map_records}
    template = "hist_update.jinja"
    sql = _common.create_sql(template, context)
    return sql

def _run_batch(session : Session, load_order : int, current_merge_sql : str, hist_insert_sql : str, hist_update_sql : str):
    """
    This function is an implementation detail and should not be used externally.
    
    :param session: Description
    :type session: Session
    :param load_order: Description
    :type load_order: int
    :param current_merge_sql: Description
    :type current_merge_sql: str
    :param hist_insert_sql: Description
    :type hist_insert_sql: str
    :param hist_update_sql: Description
    :type hist_update_sql: str
    """
    current_batch_merge_sql = current_merge_sql.replace("{load_order}", load_order)
    session.logger.info("Starting current merge.")
    _common.exec_dml(session, current_batch_merge_sql)
    session.logger.info("Current merge completed.")
    session.logger.info("Starting hist insert.")
    _common.exec_dml(session, hist_insert_sql)
    session.logger.info("Hist insert completed.")
    session.logger.info("Starting hist update.")
    _common.exec_dml(session, hist_update_sql)
    session.logger.info("Hist update completed.")

def _run_batches(session : Session, landing_table_object : DataObject, current_merge_sql : str, hist_insert_sql : str, hist_update_sql : str):
    """
    This function is an implementation detail and should not be used externally.
    
    :param session: Description
    :type session: Session
    :param landing_table_object: Description
    :type landing_table_object: DataObject
    :param current_merge_sql: Description
    :type current_merge_sql: str
    :param hist_insert_sql: Description
    :type hist_insert_sql: str
    :param hist_update_sql: Description
    :type hist_update_sql: str
    """
    batch_list = _get_batch_list(session, landing_table_object)
    session.logger.info("Starting batch load.  Note: batches are run in reverse load order.")
    for batch in batch_list:
        load_order, = batch
        load_order = str(load_order)
        session.logger.info(f"Starting batch {load_order}.")
        _run_batch(session, load_order, current_merge_sql, hist_insert_sql, hist_update_sql)
        session.logger.info(f"Finished batch {load_order}.")
    session.logger.info("Batch load complete.")

def _get_batch_list(session : Session, landing_table_object : DataObject):
    """This function is an implementation detail and should not be used externally."""
    sql = f"""
    SELECT DISTINCT __pstage_load_order
    FROM temp.main.temp__cte_{landing_table_object.table_name}
    ORDER BY __pstage_load_order DESC
    """
    batch_list = _common.exec_sql_return(session, sql)
    return batch_list

#TODO REMOVE.
# def main():
#     """
#     Can be used for direct local testing and debugging....
#     """
#     import logging
#     print("\ninline test start test run")
#     print("---------------")
#     logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d :: %(message)s', level = logging.DEBUG)
#     logger = logging.getLogger("duckdb_ps_load")
#     session = Session(r"C:\Users\james\Source\Repos\data-load-utilities\tests\data\test_persisted_stage.duckdb", logger)
#     #db_path_file =r"C:\Users\james\Source\Repos\data-load-utilities\duckdb\data\test_persisted_stage.duckdb"
#     #conn = duckdb.connect(db_path_file)
#     exec(session, "test_persisted_stage.main.loan__land", "test_persisted_stage.main.loan", "test_persisted_stage.main.loan__hist", ["update_timestamp", "create_timestamp"])
#     session.close()
 
# if __name__ == '__main__':
#     main()