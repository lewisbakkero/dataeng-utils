#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep

import mysql.connector


def get_mysql_connection(logger, config, autocommit=False, attempt=1, max_attempts=10):
    """
    Gets an mysql connection object.

    :param logger: logger
    :param config: config
    :param autocommit: autocommit connection
    :param attempt: attempt count
    :param max_attempts: maximum number of attempts

    :type logger: logging.Logger
    :type config: dict
    :type autocommit: bool
    :type attempt: int
    :type max_attempts: int

    :returns: mysql connection object
    :rtype: mysql.connector.Connection
    """
    try:
        return mysql.connector.connect(**config, autocommit=autocommit)
    except mysql.connector.Error as e:
        if attempt < max_attempts:
            sleep(5)
            logger.warning("Retrying to connect to server attempt {attempt}".format(attempt=attempt))
            attempt += 1
            return get_mysql_connection(logger, config, autocommit, attempt, max_attempts)
        else:
            logger.error("{error}".format(error=e.__str__()))


def close_mysql_connection(logger, conn):
    """
    Closes mysql connection.

    :param logger: logger
    :param conn: mysql Connection

    :type logger: logging.Logger
    :type conn: mysql connection object
    """
    try:
        if conn is not None:
            conn.close()
    except mysql.connector.Error as e:
        logger.error("{error}".format(error=e.__str__()))


def get_new_rows(logger, conn, db_name, table_name, columns, id_field, last_known_value, limit=None):
    """
    Gets new rows from mysql server.

    :param logger: logger
    :param conn: mysql Connection
    :param db_name: database name
    :param table_name: table name
    :param columns: list of columns
    :param id_field: ID field
    :param last_known_value: last known value
    :param limit: mysql limit clause

    :type logger: logging.Logger
    :type conn: mysql connection object
    :type db_name: str
    :type table_name: str
    :type columns: list
    :type id_field: str
    :type last_known_value: int
    :type limit: int

    :returns: new rows for a given table and condition
    :rtype: list
    """
    cursor = conn.cursor()
    columns = ["`{c}`".format(c=c) for c in columns]

    sql = """
    SELECT {columns}
    FROM {db_name}.{table_name}
    WHERE {id_field} > {last_known_value}
    ORDER BY {id_field}
    """.format(db_name=db_name, table_name=table_name, columns=", ".join(columns), id_field=id_field,
               last_known_value=last_known_value)

    if limit:
        sql += " LIMIT {limit}".format(limit=limit)

    sql += ";"

    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except mysql.connector.Error as e:
        logger.error("Unable to get new rows from {db_name}.{table_name} - {error}".format(db_name=db_name,
                                                                                           table_name=table_name,
                                                                                           error=e.__str__()))
    finally:
        if cursor is not None:
            cursor.close()
            del cursor


def get_rows_with_ids(logger, conn, db_name, table_name, columns, id_field, ids):
    """
    Gets rows from mysql server for given ids.

    :param logger: logger
    :param conn: mysql Connection
    :param db_name: database name
    :param table_name: table name
    :param columns: list of columns
    :param id_field: ID field
    :param ids: list of ids

    :type logger: logging.Logger
    :type conn: mysql connection object
    :type db_name: str
    :type table_name: str
    :type columns: list
    :type id_field: str
    :type ids: list

    :returns: matched rows for a given table and condition
    :rtype: list
    """
    cursor = conn.cursor()

    sql = """
    SELECT {columns}
    FROM {db_name}.{table_name}
    WHERE {id_field} IN ({ids});
    """.format(db_name=db_name, table_name=table_name, columns=", ".join(columns), id_field=id_field,
               ids=", ".join([str(i) for i in ids]))

    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except mysql.connector.Error as e:
        logger.error("Unable to get rows with IDs from {db_name}.{table_name} - {error}".format(db_name=db_name,
                                                                                                table_name=table_name,
                                                                                                error=e.__str__()))
    finally:
        if cursor is not None:
            cursor.close()
            del cursor


def get_last_row_id(logger, conn, db_name, table_name, id_field):
    """
    Gets last row ID of a table.

    :param logger: logger
    :param conn: mysql Connection
    :param db_name: database name
    :param table_name: table name
    :param id_field: column name

    :type logger: logging.Logger
    :type conn: mysql connection object
    :type db_name: str
    :type table_name: str
    :type id_field: str

    :returns: last row ID for a given table
    :rtype: int
    """
    cursor = conn.cursor()

    sql = """
    SELECT MAX({id_field})
    FROM {db_name}.{table_name};
    """.format(db_name=db_name, table_name=table_name, id_field=id_field)

    try:
        cursor.execute(sql)
        last_row_id = cursor.fetchone()
        return last_row_id[0]
    except mysql.connector.Error as e:
        logger.error(
            "Unable to get last row ID for {db_name}.{table_name} - {error}".format(db_name=db_name,
                                                                                    table_name=table_name,
                                                                                    error=e.__str__()))
    finally:
        if cursor is not None:
            cursor.close()
            del cursor
