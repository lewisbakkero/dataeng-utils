#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import unittest
from unittest.mock import patch, Mock
from dataeng.utils.mssql import get_mssql_connection, close_mssql_connection, get_new_rows, get_rows_with_ids, get_last_row_id, \
    get_mssql_driver
import pyodbc


class MssqlUtilTest(unittest.TestCase):

    def setUp(self):
        self.logger = Mock(spec=logging.Logger)

    def test_get_connection(self):
        connection_string = "DRIVER=testSQLServer;SERVER=test;DATABASE=testdb;UID=test;PWD=test"
        with patch(target='dataeng.utils.mssql.pyodbc.connect') as mock:
            get_mssql_connection(logger=self.logger, connection_string=connection_string)
            self.assertTrue(mock.called)

    @patch("dataeng.utils.mssql.pyodbc.connect")
    def test_get_connection_retry(self, mock_mysql):
        mock_mysql.side_effect = pyodbc.Error("Boom")
        connection_string = "DRIVER=testSQLServer;SERVER=test;DATABASE=testdb;UID=test;PWD=test"
        result = get_mssql_connection(logger=self.logger, connection_string=connection_string, max_attempts=2)
        self.assertIsNone(result)

    def test_close_connection(self):
        with patch("pyodbc.connect") as mock_connect:
            close_mssql_connection(logger=self.logger, conn=mock_connect)
            mock_connect.close.assert_called()

    def test_close_connection_exception(self):
        with patch("pyodbc.connect") as mock_connect:
            mock_connect.close.side_effect = pyodbc.ProgrammingError("Connection already closed")
            close_mssql_connection(logger=self.logger, conn=mock_connect)
            mock_connect.close.assert_called()

    def test_get_new_rows(self):
        with patch("pyodbc.connect") as mock_connect:
            get_new_rows(logger=self.logger, conn=mock_connect, db_name="test", schema_name="test", table_name="test",
                         columns=["test"], id_field="test", last_known_value=0)
            self.assertTrue(mock_connect.cursor.called)
            self.assertEqual(mock_connect.cursor.mock_calls[1][0], "().execute")
            self.assertEqual(mock_connect.cursor.mock_calls[2][0], "().fetchall")
            self.assertEqual(mock_connect.cursor.mock_calls[3][0], "().close")

    def test_get_new_rows_exception(self):
        with patch("pyodbc.connect") as mock_connect:
            mock_connect.cursor().execute.side_effect = pyodbc.Error("Connection error")
            get_new_rows(logger=self.logger, conn=mock_connect, db_name="test", schema_name="test", table_name="test",
                         columns=["test"], id_field="test", last_known_value=0)
            self.assertTrue(mock_connect.cursor.called)
            self.assertEqual(mock_connect.cursor.mock_calls[2][0], "().execute")
            self.assertEqual(mock_connect.cursor.mock_calls[3][0], "().close")

    def test_get_rows_with_ids(self):
        with patch("pyodbc.connect") as mock_connect:
            get_rows_with_ids(logger=self.logger, conn=mock_connect, db_name="test", schema_name="test",
                              table_name="test", columns=["test"], id_field="test", ids=[0, 1])
            self.assertTrue(mock_connect.cursor.called)
            self.assertEqual(mock_connect.cursor.mock_calls[1][0], "().execute")
            self.assertEqual(mock_connect.cursor.mock_calls[2][0], "().fetchall")
            self.assertEqual(mock_connect.cursor.mock_calls[3][0], "().close")

    def test_get_rows_with_ids_exception(self):
        with patch("pyodbc.connect") as mock_connect:
            mock_connect.cursor().execute.side_effect = pyodbc.Error("Connection error")
            get_rows_with_ids(logger=self.logger, conn=mock_connect, db_name="test", schema_name="test",
                              table_name="test", columns=["test"], id_field="test", ids=[0, 1])
            self.assertTrue(mock_connect.cursor.called)
            self.assertEqual(mock_connect.cursor.mock_calls[2][0], "().execute")
            self.assertEqual(mock_connect.cursor.mock_calls[3][0], "().close")

    def test_get_last_row_id(self):
        with patch("pyodbc.connect") as mock_connect:
            mock_connect.cursor().fetchone.return_value = [100]
            get_last_row_id(logger=self.logger, conn=mock_connect, db_name="test", schema_name="test",
                            table_name="test", id_field="test")
            self.assertTrue(mock_connect.cursor.called)
            self.assertEqual(mock_connect.cursor.mock_calls[2][0], "().execute")
            self.assertEqual(mock_connect.cursor.mock_calls[3][0], "().fetchone")
            self.assertEqual(mock_connect.cursor.mock_calls[4][0], "().close")

    def test_get_last_row_id_exception(self):
        with patch("pyodbc.connect") as mock_connect:
            mock_connect.cursor().execute.side_effect = pyodbc.Error("Connection error")
            mock_connect.cursor().fetchone.return_value = [100]
            get_last_row_id(logger=self.logger, conn=mock_connect, db_name="test", schema_name="test",
                            table_name="test", id_field="test")
            self.assertTrue(mock_connect.cursor.called)
            self.assertEqual(mock_connect.cursor.mock_calls[3][0], "().execute")
            self.assertEqual(mock_connect.cursor.mock_calls[4][0], "().close")

    def test_get_mssql_driver(self):
        with patch("pyodbc.drivers") as mock_drivers:
            mock_drivers.return_value = ['SQL Server', 'MySQL ODBC 8.0 ANSI Driver', 'MySQL ODBC 8.0 Unicode Driver']
            output = get_mssql_driver(logger=self.logger)
            self.assertEqual("SQL Server", output)

    def test_get_mssql_driver_not_found(self):
        with patch("pyodbc.drivers") as mock_drivers:
            mock_drivers.return_value = []
            output = get_mssql_driver(logger=self.logger)
            self.assertIsNone(output)


if __name__ == "__main__":
    unittest.main()
