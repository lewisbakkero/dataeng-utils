#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import unittest
from unittest.mock import patch, Mock
from dataeng.utils.mysql import get_mysql_connection, close_mysql_connection, get_new_rows, get_rows_with_ids, get_last_row_id
import mysql.connector


class MysqlUtilTest(unittest.TestCase):

    def setUp(self):
        self.logger = Mock(spec=logging.Logger)

    def test_get_connection(self):
        config = {'user': 'test', 'password': 'test'}
        with patch(target='dataeng.utils.mysql.mysql.connector.connect') as mock:
            get_mysql_connection(logger=self.logger, config=config)
            self.assertTrue(mock.called)

    @patch("dataeng.utils.mysql.mysql.connector.connect")
    def test_get_connection_retry(self, mock_mysql):
        mock_mysql.side_effect = mysql.connector.Error("Boom")
        config = {'user': 'test', 'password': 'test'}
        result = get_mysql_connection(logger=self.logger, config=config, max_attempts=2)
        self.assertIsNone(result)

    def test_close_connection(self):
        connection = Mock(spec=mysql.connector.MySQLConnection)
        close_mysql_connection(logger=self.logger, conn=connection)
        connection.close.assert_called()

    def test_close_connection_exception(self):
        connection = Mock(spec=mysql.connector.MySQLConnection)
        connection.close.side_effect = mysql.connector.Error("Connection already closed")
        close_mysql_connection(logger=self.logger, conn=connection)
        connection.close.assert_called()

    def test_get_new_rows(self):
        connection = Mock(spec=mysql.connector.MySQLConnection)
        get_new_rows(logger=self.logger, conn=connection, db_name="test", table_name="test", columns=["test"],
                     id_field="test", last_known_value=0)
        self.assertTrue(connection.cursor.called)
        self.assertEqual(connection.cursor.mock_calls[1][0], "().execute")
        self.assertEqual(connection.cursor.mock_calls[2][0], "().fetchall")
        self.assertEqual(connection.cursor.mock_calls[3][0], "().close")

    def test_get_new_rows_exception(self):
        connection = Mock(spec=mysql.connector.MySQLConnection)
        connection.cursor().execute.side_effect = mysql.connector.Error("Connection error")
        get_new_rows(logger=self.logger, conn=connection, db_name="test", table_name="test", columns=["test"],
                     id_field="test", last_known_value=0)
        self.assertTrue(connection.cursor.called)
        self.assertEqual(connection.cursor.mock_calls[2][0], "().execute")
        self.assertEqual(connection.cursor.mock_calls[3][0], "().close")

    def test_get_new_rows_with_limit_clause(self):
        connection = Mock(spec=mysql.connector.MySQLConnection)
        get_new_rows(logger=self.logger, conn=connection, db_name="test", table_name="test", columns=["test"],
                     id_field="test", last_known_value=0, limit="0,10")
        self.assertTrue(connection.cursor.called)
        self.assertEqual(connection.cursor.mock_calls[1][0], "().execute")
        self.assertEqual(connection.cursor.mock_calls[3][0], "().close")
        if "LIMIT" in connection.cursor.mock_calls[1][1][0]:
            self.assertTrue(True)
        else:
            self.assertTrue(False)

    def test_get_rows_with_ids(self):
        connection = Mock(spec=mysql.connector.MySQLConnection)
        get_rows_with_ids(logger=self.logger, conn=connection, db_name="test", table_name="test", columns=["test"],
                          id_field="test", ids=[0, 1])
        self.assertTrue(connection.cursor.called)
        self.assertEqual(connection.cursor.mock_calls[1][0], "().execute")
        self.assertEqual(connection.cursor.mock_calls[2][0], "().fetchall")
        self.assertEqual(connection.cursor.mock_calls[3][0], "().close")

    def test_get_rows_with_ids_exception(self):
        connection = Mock(spec=mysql.connector.MySQLConnection)
        connection.cursor().execute.side_effect = mysql.connector.Error("Connection error")
        get_rows_with_ids(logger=self.logger, conn=connection, db_name="test", table_name="test", columns=["test"],
                          id_field="test", ids=[0, 1])
        self.assertTrue(connection.cursor.called)
        self.assertEqual(connection.cursor.mock_calls[2][0], "().execute")
        self.assertEqual(connection.cursor.mock_calls[3][0], "().close")

    def test_get_last_row_id(self):
        connection = Mock(spec=mysql.connector.MySQLConnection)
        connection.cursor().fetchone.return_value = [100]
        get_last_row_id(logger=self.logger, conn=connection, db_name="test", table_name="test", id_field="test")
        self.assertTrue(connection.cursor.called)
        self.assertEqual(connection.cursor.mock_calls[2][0], "().execute")
        self.assertEqual(connection.cursor.mock_calls[3][0], "().fetchone")
        self.assertEqual(connection.cursor.mock_calls[4][0], "().close")

    def test_get_last_row_id_exception(self):
        connection = Mock(spec=mysql.connector.MySQLConnection)
        connection.cursor().execute.side_effect = mysql.connector.Error("Connection error")
        connection.cursor().fetchone.return_value = [100]
        get_last_row_id(logger=self.logger, conn=connection, db_name="test", table_name="test", id_field="test")
        self.assertTrue(connection.cursor.called)
        self.assertEqual(connection.cursor.mock_calls[3][0], "().execute")
        self.assertEqual(connection.cursor.mock_calls[4][0], "().close")


if __name__ == "__main__":
    unittest.main()
