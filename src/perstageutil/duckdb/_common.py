#This package is an implementation detail and should not be used externally.
from perstageutil.duckdb.session import Session
from collections import namedtuple
import pandas
import jinja2

JINJA_PACKAGE_NAME = "perstageutil.duckdb.load"

DataObject = namedtuple("DataObject", "table_catalog table_schema table_name")

def split_three_part_name(three_part_name : str) -> DataObject:
    """
    This function is an implementation detail and should not be used externally.
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
    This function is an implementation detail and should not be used externally.
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
    return True

def check_landing_table_structure(session : Session, landing_table_object : DataObject):
    """
    This function is an implementation detail and should not be used externally.
    Checks the landing table structure for the needed column.
    
    :param session: The current session as a Session object.
    :type session: Session
    :param landing_table_object: The landing table as a DataObject.
    :type landing_table_object: DataObject
    """
    df = return_columns_df(session, landing_table_object)
    if ((df["column_name"] == "__pstage_inserted_timestamp").any()) == False:
        raise Exception("Landing table is missing the needed __pstage_inserted_timestamp column.")

def check_current_table_structure(session : Session, current_table_object : DataObject):
    """This function is an implementation detail and should not be used externally."""
    df = return_columns_df(session, current_table_object)
    if ((df["column_name"] == "__pstage_inserted_timestamp").any()) == False:
        raise Exception("Current table is missing the needed __pstage_inserted_timestamp column.")
    if ((df["column_name"] == "__pstage_updated_timestamp").any()) == False:
        raise Exception("Current table is missing the needed __pstage_updated_timestamp column.")
    if ((df["column_name"] == "__pstage_deleted_indicator").any()) == False:
        raise Exception("Current table is missing the needed __pstage_deleted_indicator column.")
    if ((df["column_name"] == "__pstage_hash_diff").any()) == False:
        raise Exception("Current table is missing the needed __pstage_hash_diff column.")
    if ((df["column_name"] == "__pstage_dedupe_confidence_percent").any()) == False:
        raise Exception("Current table is missing the needed __pstage_dedupe_confidence_percent column.")
    if ((df["primary_key_indicator"] == True).any()) == False:
        raise Exception("Current table is missing the needed primary key.")
    
def check_hist_table_structure(session : Session, hist_table_object : DataObject):
    """This function is an implementation detail and should not be used externally."""
    df = return_columns_df(session, hist_table_object)
    if ((df["column_name"] == "__pstage_effective_timestamp").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_effective_timestamp column.")
    if ((df["column_name"] == "__pstage_expiration_timestamp").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_expiration_timestamp column.")
    if ((df["column_name"] == "__pstage_current_version_indicator").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_current_version_indicator column.")
    if ((df["column_name"] == "__pstage_inserted_timestamp").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_inserted_timestamp column.")
    if ((df["column_name"] == "__pstage_updated_timestamp").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_updated_timestamp column.")
    if ((df["column_name"] == "__pstage_deleted_indicator").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_deleted_indicator column.")
    if ((df["column_name"] == "__pstage_hash_diff").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_hash_diff column.")
    if ((df["column_name"] == "__pstage_dedupe_confidence_percent").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_dedupe_confidence_percent column.")
    if ((df["primary_key_indicator"] == True).any()) == False:
        raise Exception("Historical table is missing the needed primary key.")
    if (((df["primary_key_indicator"] == True) & (df["column_name"] == "__pstage_effective_timestamp")).any()) == False:
        raise Exception("Historical tables pirmary key does not include the __pstage_effective_timestamp column.")

def return_columns_df(session : Session, table_object : DataObject) -> pandas.DataFrame:
    """This function is an implementation detail and should not be used externally."""
    sql = f"""
    SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.ordinal_position, c.is_nullable, c.data_type,
    c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.datetime_precision,
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
    num_rows = df.shape[0]
    if num_rows == 0:
        table_name = table_object.table_catalog + "." + table_object.table_schema + "." + table_object.table_name
        raise Exception(f"Table {table_name} does not exist.")
    return df

def exec_sql_return_df(session : Session, sql : str) -> pandas.DataFrame:
    """
    This function is an implementation detail and should not be used externally.
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    :return: Description
    :rtype: DataFrame
    """
    try:
        session.logger.debug(f"SQL: {sql}")
        results = session.conn.execute(sql).df()
        #results = session.conn.sql(sql).df()
    except Exception as err:
        session.logger.error(err)
        raise err
    return results

def create_sql(template : str, context : dict):
    """
    This function is an implementation detail and should not be used externally.
    
    :param template: Description
    :type template: str
    :param context: Description
    :type context: dict
    """
    template : jinja2.Template
    env = jinja2.Environment(
        loader=jinja2.PackageLoader(JINJA_PACKAGE_NAME),
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
    This function is an implementation detail and should not be used externally.
    Executes data manipulation language statements in DuckDb and captures and logs the Updated rows output.
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    """
    try:
        session.logger.debug(f"SQL: {sql}")
        session.logger.info(f"Partial output of query being executed (first 256 characters):\n{sql[:256]}\n...")
        #session.conn.sql(sql)
        results = session.conn.execute(sql).fetchall()
        for result in results:
            updated_rows, = result
            updated_rows = str(updated_rows)
            session.logger.info(f"Updated rows: {updated_rows}.")
    except Exception as err:
        session.logger.error(err)
        raise err
    
def exec_sql_return(session : Session, sql : str):
    """
    This function is an implementation detail and should not be used externally.
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    """
    try:
        session.logger.debug(f"SQL: {sql}")
        results = session.conn.execute(sql).fetchall()
        #results = session.conn.sql(sql).fetchall()
    except Exception as err:
        session.logger.error(err)
        raise err
    return results

def exec_ddl(session : Session, sql : str):
    """
    This function is an implementation detail and should not be used externally.
    Executes data manipulation language statements in DuckDb and captures and logs the Updated rows output.
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    """
    try:
        session.logger.debug(f"SQL: {sql}")
        session.logger.info(f"Partial output of query being executed (first 256 characters):\n{sql[:256]}\n...")
        #session.conn.sql(sql)
        results = session.conn.execute(sql).fetchall()
        for result in results:
            # updated_rows, = result
            # updated_rows = str(updated_rows)
            session.logger.info(f"Result: {result}.")
    except Exception as err:
        session.logger.error(err)
        raise err

def convert_df_to_records(df : pandas.DataFrame):
    """
    This function is an implementation detail and should not be used externally.
    
    :param df: Description
    :type df: pandas.DataFrame
    """
    records = df.to_dict("records")
    return records