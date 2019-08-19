#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pyodbc
from time import sleep


def get_mssql_connection(logger, connection_string, autocommit=False, attempt=1, max_attempts=10):
    """
    Gets an ODBC connection.

    :param logger: logger
    :param connection_string: connection string
    :param autocommit: autocommit connection
    :param attempt: attempt count
    :param max_attempts: maximum number of attempts

    :type logger: logging.Logger
    :type connection_string: str
    :type autocommit: bool
    :type attempt: int
    :type max_attempts: int

    :returns: ODBC connection
    :rtype: pyodbc.Connection
    """
    try:
        return pyodbc.connect(connection_string, autocommit=autocommit)
    except pyodbc.Error as e:
        if attempt < max_attempts:
            sleep(5)
            logger.warning("Retrying to connect to server attempt {attempt}".format(attempt=attempt))
            attempt += 1
            return get_mssql_connection(logger, connection_string, autocommit, attempt, max_attempts)
        else:
            logger.error("{error}".format(error=e.__str__()))


def close_mssql_connection(logger, conn):
    """
    Closes an ODBC connection.

    :param logger: logger
    :param conn: ODBC connection

    :type logger: logging.Logger
    :type conn: pyodbc.Connection
    """
    try:
        if conn is not None:
            conn.close()
    except pyodbc.ProgrammingError as e:
        logger.error("{error}".format(error=e.__str__()))


def get_new_rows(logger, conn, db_name, schema_name, table_name, columns, id_field, last_known_value, limit=None):
    """
    Gets new rows from SQL server.
    
    :param logger: logger
    :param conn: ODBC connection
    :param db_name: database name
    :param schema_name: schema name
    :param table_name: table name
    :param columns: list of columns
    :param id_field: ID field
    :param last_known_value: last known value
    :param limit: mssql limit clause

    :type logger: logging.Logger
    :type conn: pyodbc.Connection
    :type db_name: str
    :type schema_name: str
    :type table_name: str
    :type columns: list
    :type id_field: str
    :type last_known_value: int
    :type limit: int

    :returns: new rows for a given table and condition
    :rtype: list
    """
    cursor = conn.cursor()
    columns = ["'{c}'".format(c=c) for c in columns]

    sql = """
    SELECT {columns}
    FROM [{db_name}].[{schema_name}].[{table_name}]
    WHERE {id_field} > {last_known_value}
    ORDER BY {id_field}
    """.format(db_name=db_name, schema_name=schema_name, table_name=table_name, columns=", ".join(columns),
               id_field=id_field, last_known_value=last_known_value)

    if limit:
        sql = sql.replace("SELECT", "SELECT TOP {limit}".format(limit=limit))

    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except pyodbc.Error as e:
        logger.error("Unable to get new rows from {db_name}.{schema_name} - {error}".format(db_name=db_name,
                                                                                            schema_name=schema_name,
                                                                                            table_name=table_name,
                                                                                            error=e.__str__()))
    finally:
        if cursor is not None:
            cursor.close()
            del cursor


def get_rows_with_ids(logger, conn, db_name, schema_name, table_name, columns, id_field, ids):
    cursor = conn.cursor()

    sql = """
    SELECT {columns} 
    FROM [{db_name}].[{schema_name}].[{table_name}]
    WHERE {id_field} IN ({ids})
    """.format(db_name=db_name, schema_name=schema_name, table_name=table_name, columns=", ".join(columns),
               id_field=id_field, ids=", ".join([str(i) for i in ids]))

    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except pyodbc.Error as e:
        logger.error("Unable to get rows with IDs from {db_name}.{schema_name} - {error}".format(db_name=db_name,
                                                                                                 schema_name=schema_name,
                                                                                                 table_name=table_name,
                                                                                                 error=e.__str__()))
    finally:
        if cursor is not None:
            cursor.close()
            del cursor


def get_last_row_id(logger, conn, db_name, schema_name, table_name, id_field):
    """
    Gets last row ID of a table.

    :param logger: logger
    :param conn: ODBC connection
    :param db_name: database name
    :param schema_name: schema name
    :param table_name: table name
    :param id_field: column name

    :type logger: logging.Logger
    :type conn: pyodbc.Connection
    :type db_name: str
    :type schema_name: str
    :type table_name: str
    :type id_field: str

    :returns: last row ID for a given table
    :rtype: int
    """
    cursor = conn.cursor()

    sql = """
    SELECT MAX([{id_field}]) FROM [{db_name}].[{schema_name}].[{table_name}];
    """.format(db_name=db_name, table_name=table_name, schema_name=schema_name, id_field=id_field)

    try:
        cursor.execute(sql)
        last_row_id = cursor.fetchone()
        return last_row_id[0]
    except pyodbc.Error as e:
        logger.error(
            "Unable to get last row ID for {db_name}.{schema_name}.{table_name} - {error}".format(db_name=db_name,
                                                                                                  schema_name=schema_name,
                                                                                                  table_name=table_name,
                                                                                                  error=e.__str__()))
    finally:
        if cursor is not None:
            cursor.close()
            del cursor


def get_mssql_driver(logger):
    """
    Gets the latest MSSQL ODBC driver.

    :param logger: logger

    :type logger: logging.Logger

    :returns: latest MSSQL ODBC driver
    :rtype: str
    """
    drivers = pyodbc.drivers()

    if "SQL Server" in drivers:
        return drivers[drivers.index("SQL Server")]

    versions = [int(d.split()[2]) for d in drivers if
                "ODBC Driver" in d and "SQL Server" in d and len(d.split()) == 6 and d.split()[2].isdigit()]

    if len(versions) == 0:
        logger.error("No ODBC drivers available, all tests terminated.")
        return

    return "ODBC Driver {version} for SQL Server".format(version=max(versions))
